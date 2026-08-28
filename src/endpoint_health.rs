use crate::HarborError;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointStatus {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointHealth {
    pub route_id: String,
    pub endpoint: String,
    pub interval_seconds: u64,
    pub timeout_millis: u64,
    pub failure_threshold: u16,
    pub status: EndpointStatus,
    pub consecutive_failures: u16,
    pub last_probe_at: Option<u64>,
    pub next_probe_at: u64,
    pub last_latency_millis: Option<u64>,
    pub last_error: Option<String>,
    pub version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeOutcome {
    pub health: EndpointHealth,
    pub successful: bool,
    pub routing_allowed: bool,
}

/// Append-only endpoint health state with deterministic probe scheduling.
pub struct EndpointHealthMonitor {
    path: PathBuf,
    endpoints: BTreeMap<String, EndpointHealth>,
}

impl EndpointHealthMonitor {
    /// Replays the health journal and validates contiguous per-route versions.
    ///
    /// # Errors
    /// Returns journal errors for unreadable, malformed, or non-contiguous records.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HarborError> {
        let path = path.as_ref().to_path_buf();
        let mut endpoints: BTreeMap<String, EndpointHealth> = BTreeMap::new();
        match fs::File::open(&path) {
            Ok(file) => {
                for (line_number, line) in BufReader::new(file).lines().enumerate() {
                    let line = line.map_err(|error| HarborError::Journal(error.to_string()))?;
                    let item = decode(&line).map_err(|message| {
                        HarborError::Journal(format!(
                            "endpoint health journal line {}: {message}",
                            line_number + 1
                        ))
                    })?;
                    if let Some(previous) = endpoints.get(&item.route_id) {
                        if item.version != previous.version + 1 {
                            return Err(HarborError::Journal(format!(
                                "route {} has non-contiguous health version {}",
                                item.route_id, item.version
                            )));
                        }
                    } else if item.version != 1 {
                        return Err(HarborError::Journal(format!(
                            "route {} starts at health version {}",
                            item.route_id, item.version
                        )));
                    }
                    endpoints.insert(item.route_id.clone(), item);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(HarborError::Journal(error.to_string())),
        }
        Ok(Self { path, endpoints })
    }

    /// Registers one route endpoint and its health policy.
    ///
    /// # Errors
    /// Returns validation, conflict, or journal errors without exposing unpersisted state.
    pub fn register(
        &mut self,
        route_id: &str,
        endpoint: &str,
        interval_seconds: u64,
        timeout_millis: u64,
        failure_threshold: u16,
        at: u64,
    ) -> Result<EndpointHealth, HarborError> {
        let route_id = required(route_id, "route id", 120)?;
        let endpoint = validated_endpoint(endpoint)?;
        if !(5..=3_600).contains(&interval_seconds) {
            return Err(HarborError::Validation(
                "probe interval must be from 5 through 3600 seconds".into(),
            ));
        }
        if !(100..=60_000).contains(&timeout_millis) {
            return Err(HarborError::Validation(
                "probe timeout must be from 100 through 60000 milliseconds".into(),
            ));
        }
        if !(1..=20).contains(&failure_threshold) {
            return Err(HarborError::Validation(
                "failure threshold must be from 1 through 20".into(),
            ));
        }
        if self.endpoints.contains_key(&route_id) {
            return Err(HarborError::Conflict(format!(
                "route {route_id} already has endpoint health policy"
            )));
        }
        let item = EndpointHealth {
            route_id: route_id.clone(),
            endpoint,
            interval_seconds,
            timeout_millis,
            failure_threshold,
            status: EndpointStatus::Unknown,
            consecutive_failures: 0,
            last_probe_at: None,
            next_probe_at: at,
            last_latency_millis: None,
            last_error: None,
            version: 1,
        };
        self.append(&item)?;
        self.endpoints.insert(route_id, item.clone());
        Ok(item)
    }

    /// Records a due probe after verifying it targeted the configured endpoint.
    ///
    /// HTTP 2xx and 3xx responses within the timeout are successful. A failure
    /// degrades the route, and reaching the configured threshold makes it unavailable.
    ///
    /// # Errors
    /// Returns not-found, version, scheduling, endpoint, response validation,
    /// or journal errors.
    #[allow(clippy::too_many_arguments)]
    pub fn record_probe(
        &mut self,
        route_id: &str,
        observed_endpoint: &str,
        status_code: Option<u16>,
        latency_millis: u64,
        error: Option<&str>,
        at: u64,
        expected_version: u64,
    ) -> Result<ProbeOutcome, HarborError> {
        let current = self.endpoints.get(route_id).ok_or_else(|| {
            HarborError::NotFound(format!("endpoint health for route {route_id}"))
        })?;
        if current.version != expected_version {
            return Err(HarborError::Conflict(format!(
                "endpoint health version is {}, expected {expected_version}",
                current.version
            )));
        }
        if at < current.next_probe_at {
            return Err(HarborError::Conflict(format!(
                "route {route_id} probe is not due until {}",
                current.next_probe_at
            )));
        }
        if validated_endpoint(observed_endpoint)? != current.endpoint {
            return Err(HarborError::Conflict(
                "probe endpoint differs from configured endpoint".into(),
            ));
        }
        if latency_millis == 0 {
            return Err(HarborError::Validation(
                "probe latency must be positive".into(),
            ));
        }
        if status_code.is_some_and(|code| !(100..=599).contains(&code)) {
            return Err(HarborError::Validation(
                "probe status must be from 100 through 599".into(),
            ));
        }
        let successful = status_code.is_some_and(|code| (200..400).contains(&code))
            && latency_millis <= current.timeout_millis
            && error.is_none();
        let mut next = current.clone();
        next.last_probe_at = Some(at);
        next.next_probe_at = at.checked_add(next.interval_seconds).ok_or_else(|| {
            HarborError::Validation("next probe time exceeds timestamp range".into())
        })?;
        next.last_latency_millis = Some(latency_millis);
        next.version += 1;
        if successful {
            next.status = EndpointStatus::Healthy;
            next.consecutive_failures = 0;
            next.last_error = None;
        } else {
            next.consecutive_failures = next.consecutive_failures.saturating_add(1);
            next.status = if next.consecutive_failures >= next.failure_threshold {
                EndpointStatus::Unavailable
            } else {
                EndpointStatus::Degraded
            };
            next.last_error = Some(failure_reason(
                status_code,
                latency_millis,
                next.timeout_millis,
                error,
            )?);
        }
        self.append(&next)?;
        self.endpoints.insert(route_id.to_owned(), next.clone());
        Ok(ProbeOutcome {
            routing_allowed: next.status != EndpointStatus::Unavailable,
            health: next,
            successful,
        })
    }

    /// Returns whether a recent successful/degraded endpoint state permits routing.
    /// Unknown, unavailable, and stale states fail closed.
    #[must_use]
    pub fn routing_allowed(&self, route_id: &str, at: u64) -> bool {
        self.endpoints.get(route_id).is_some_and(|item| {
            matches!(
                item.status,
                EndpointStatus::Healthy | EndpointStatus::Degraded
            ) && item
                .last_probe_at
                .is_some_and(|last| at <= last.saturating_add(item.interval_seconds * 2))
        })
    }

    #[must_use]
    pub fn health(&self, route_id: &str) -> Option<&EndpointHealth> {
        self.endpoints.get(route_id)
    }

    /// Returns routes whose next probe is due, oldest deadline first.
    #[must_use]
    pub fn due_routes(&self, at: u64) -> Vec<String> {
        let mut items: Vec<_> = self
            .endpoints
            .values()
            .filter(|item| item.next_probe_at <= at)
            .collect();
        items.sort_by_key(|item| (item.next_probe_at, item.route_id.as_str()));
        items
            .into_iter()
            .map(|item| item.route_id.clone())
            .collect()
    }

    fn append(&self, item: &EndpointHealth) -> Result<(), HarborError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| HarborError::Journal(error.to_string()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| HarborError::Journal(error.to_string()))?;
        writeln!(file, "{}", encode(item))
            .and_then(|()| file.sync_data())
            .map_err(|error| HarborError::Journal(error.to_string()))
    }
}

fn required(value: &str, field: &str, maximum: usize) -> Result<String, HarborError> {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.is_empty() || value.len() > maximum {
        return Err(HarborError::Validation(format!(
            "{field} must contain 1 to {maximum} characters"
        )));
    }
    Ok(value)
}

fn validated_endpoint(value: &str) -> Result<String, HarborError> {
    let value = required(value, "endpoint", 2_000)?;
    if !value.starts_with("https://") {
        return Err(HarborError::Validation(
            "health endpoint must use https://".into(),
        ));
    }
    Ok(value)
}

fn failure_reason(
    status: Option<u16>,
    latency: u64,
    timeout: u64,
    error: Option<&str>,
) -> Result<String, HarborError> {
    if let Some(error) = error {
        return required(error, "probe error", 500);
    }
    if latency > timeout {
        return Ok(format!("probe timed out after {latency}ms"));
    }
    Ok(status.map_or_else(
        || "probe returned no status".into(),
        |code| format!("probe returned HTTP {code}"),
    ))
}

fn encode(item: &EndpointHealth) -> String {
    let mut output = String::new();
    let status = match item.status {
        EndpointStatus::Unknown => "U",
        EndpointStatus::Healthy => "H",
        EndpointStatus::Degraded => "D",
        EndpointStatus::Unavailable => "X",
    };
    let last_probe = item
        .last_probe_at
        .map_or_else(|| "-".into(), |value| value.to_string());
    let latency = item
        .last_latency_millis
        .map_or_else(|| "-".into(), |value| value.to_string());
    let error = item.last_error.as_deref().map_or_else(|| "-".into(), hex);
    write!(
        output,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        hex(&item.route_id),
        hex(&item.endpoint),
        item.interval_seconds,
        item.timeout_millis,
        item.failure_threshold,
        status,
        item.consecutive_failures,
        last_probe,
        item.next_probe_at,
        latency,
        error,
        item.version
    )
    .expect("writing to a string cannot fail");
    output
}

fn decode(line: &str) -> Result<EndpointHealth, String> {
    let fields: Vec<_> = line.split('\t').collect();
    if fields.len() != 12 {
        return Err("expected 12 fields".into());
    }
    let status = match fields[5] {
        "U" => EndpointStatus::Unknown,
        "H" => EndpointStatus::Healthy,
        "D" => EndpointStatus::Degraded,
        "X" => EndpointStatus::Unavailable,
        _ => return Err("invalid status".into()),
    };
    Ok(EndpointHealth {
        route_id: unhex(fields[0])?,
        endpoint: unhex(fields[1])?,
        interval_seconds: number(fields[2])?,
        timeout_millis: number(fields[3])?,
        failure_threshold: number(fields[4])?,
        status,
        consecutive_failures: number(fields[6])?,
        last_probe_at: optional_number(fields[7])?,
        next_probe_at: number(fields[8])?,
        last_latency_millis: optional_number(fields[9])?,
        last_error: if fields[10] == "-" {
            None
        } else {
            Some(unhex(fields[10])?)
        },
        version: number(fields[11])?,
    })
}

fn number<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value.parse().map_err(|_| format!("invalid number {value}"))
}

fn optional_number(value: &str) -> Result<Option<u64>, String> {
    if value == "-" {
        Ok(None)
    } else {
        number(value).map(Some)
    }
}

fn hex(value: &str) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn unhex(value: &str) -> Result<String, String> {
    if !value.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| "invalid hex")?;
            u8::from_str_radix(text, 16).map_err(|_| "invalid hex")
        })
        .collect::<Result<Vec<_>, _>>()?;
    String::from_utf8(bytes).map_err(|_| "invalid UTF-8".into())
}
