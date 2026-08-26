use crate::HarborError;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const MAX_RETENTION_SECONDS: u64 = 366 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadState {
    Available,
    Purged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadRecord {
    pub event_id: String,
    pub tenant: String,
    pub state: PayloadState,
    pub payload: Option<Vec<u8>>,
    pub payload_len: usize,
    pub fingerprint: String,
    pub received_at: u64,
    pub purge_after: u64,
    pub terminal_at: Option<u64>,
    pub legal_hold: Option<String>,
    pub purged_at: Option<u64>,
    pub version: u64,
}

impl PayloadRecord {
    #[must_use]
    pub fn purge_eligible(&self, now: u64) -> bool {
        self.state == PayloadState::Available
            && self.terminal_at.is_some()
            && self.legal_hold.is_none()
            && now >= self.purge_after
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeResult {
    pub purged_event_ids: Vec<String>,
    pub bytes_released: usize,
}

pub struct PayloadRetention {
    path: PathBuf,
    records: BTreeMap<String, PayloadRecord>,
}

impl PayloadRetention {
    /// Replays the append-only retention journal and validates every payload or tombstone.
    ///
    /// # Errors
    /// Returns a journal error if a record is malformed, non-contiguous, or internally inconsistent.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HarborError> {
        let path = path.as_ref().to_path_buf();
        let mut records: BTreeMap<String, PayloadRecord> = BTreeMap::new();
        match fs::File::open(&path) {
            Ok(file) => {
                for (index, line) in BufReader::new(file).lines().enumerate() {
                    let line = line.map_err(|error| HarborError::Journal(error.to_string()))?;
                    let record = decode(&line).map_err(|message| {
                        HarborError::Journal(format!(
                            "retention journal line {}: {message}",
                            index + 1
                        ))
                    })?;
                    validate_record(&record)?;
                    if let Some(previous) = records.get(&record.event_id) {
                        if record.version != previous.version + 1 {
                            return Err(HarborError::Journal(format!(
                                "payload {} has non-contiguous version {}",
                                record.event_id, record.version
                            )));
                        }
                    } else if record.version != 1 {
                        return Err(HarborError::Journal(format!(
                            "payload {} starts at version {}",
                            record.event_id, record.version
                        )));
                    }
                    records.insert(record.event_id.clone(), record);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(HarborError::Journal(error.to_string())),
        }
        Ok(Self { path, records })
    }

    /// Captures a payload under a bounded tenant retention policy.
    ///
    /// # Errors
    /// Returns validation, conflict, overflow, or journal errors.
    pub fn capture(
        &mut self,
        event_id: &str,
        tenant: &str,
        payload: &[u8],
        received_at: u64,
        retention_seconds: u64,
    ) -> Result<PayloadRecord, HarborError> {
        let event_id = required(event_id, "event id")?;
        let tenant = required(tenant, "tenant")?;
        if payload.is_empty() {
            return Err(HarborError::Validation("payload cannot be empty".into()));
        }
        if retention_seconds == 0 || retention_seconds > MAX_RETENTION_SECONDS {
            return Err(HarborError::Validation(format!(
                "retention seconds must be between 1 and {MAX_RETENTION_SECONDS}"
            )));
        }
        if self.records.contains_key(&event_id) {
            return Err(HarborError::Conflict(format!(
                "payload {event_id} is already retained"
            )));
        }
        let purge_after = received_at.checked_add(retention_seconds).ok_or_else(|| {
            HarborError::Validation("retention deadline exceeds timestamp range".into())
        })?;
        let record = PayloadRecord {
            event_id: event_id.clone(),
            tenant,
            state: PayloadState::Available,
            payload: Some(payload.to_vec()),
            payload_len: payload.len(),
            fingerprint: fingerprint(payload),
            received_at,
            purge_after,
            terminal_at: None,
            legal_hold: None,
            purged_at: None,
            version: 1,
        };
        self.append(&record)?;
        self.records.insert(event_id, record.clone());
        Ok(record)
    }

    /// Marks processing terminal, making an expired payload eligible for purge.
    ///
    /// # Errors
    /// Returns not-found, version, lifecycle, time, or journal errors.
    pub fn mark_terminal(
        &mut self,
        event_id: &str,
        at: u64,
        expected_version: u64,
    ) -> Result<PayloadRecord, HarborError> {
        let mut record = self.current(event_id, expected_version)?;
        if record.state != PayloadState::Available {
            return Err(HarborError::Conflict(
                "purged payload cannot change lifecycle".into(),
            ));
        }
        if record.terminal_at.is_some() {
            return Err(HarborError::Conflict("payload is already terminal".into()));
        }
        if at < record.received_at {
            return Err(HarborError::Validation(
                "terminal time cannot precede receipt".into(),
            ));
        }
        record.terminal_at = Some(at);
        record.version += 1;
        self.replace(record)
    }

    /// Places a named legal or operational hold before payload purge.
    ///
    /// # Errors
    /// Returns validation, lifecycle, version, or journal errors.
    pub fn place_hold(
        &mut self,
        event_id: &str,
        reason: &str,
        expected_version: u64,
    ) -> Result<PayloadRecord, HarborError> {
        let mut record = self.current(event_id, expected_version)?;
        if record.state != PayloadState::Available {
            return Err(HarborError::Conflict("cannot hold a purged payload".into()));
        }
        if record.legal_hold.is_some() {
            return Err(HarborError::Conflict("payload already has a hold".into()));
        }
        record.legal_hold = Some(required(reason, "hold reason")?);
        record.version += 1;
        self.replace(record)
    }

    /// Releases a hold while preserving the reason in prior journal versions.
    ///
    /// # Errors
    /// Returns lifecycle, version, or journal errors.
    pub fn release_hold(
        &mut self,
        event_id: &str,
        expected_version: u64,
    ) -> Result<PayloadRecord, HarborError> {
        let mut record = self.current(event_id, expected_version)?;
        if record.legal_hold.is_none() {
            return Err(HarborError::Conflict("payload has no hold".into()));
        }
        record.legal_hold = None;
        record.version += 1;
        self.replace(record)
    }

    /// Purges every eligible payload and appends a metadata-only tombstone per event.
    ///
    /// # Errors
    /// Returns a journal error before exposing an unpersisted tombstone.
    pub fn sweep(&mut self, now: u64) -> Result<PurgeResult, HarborError> {
        let eligible: Vec<String> = self
            .records
            .values()
            .filter(|record| record.purge_eligible(now))
            .map(|record| record.event_id.clone())
            .collect();
        let mut released = 0_usize;
        let mut purged = Vec::with_capacity(eligible.len());
        for event_id in eligible {
            let Some(mut record) = self.records.get(&event_id).cloned() else {
                return Err(HarborError::NotFound(event_id));
            };
            released = released.saturating_add(record.payload_len);
            record.state = PayloadState::Purged;
            record.payload = None;
            record.purged_at = Some(now);
            record.version += 1;
            self.append(&record)?;
            self.records.insert(event_id.clone(), record);
            purged.push(event_id);
        }
        Ok(PurgeResult {
            purged_event_ids: purged,
            bytes_released: released,
        })
    }

    /// Returns one defensive record clone.
    ///
    /// # Errors
    /// Returns not-found when no payload metadata exists.
    pub fn record(&self, event_id: &str) -> Result<PayloadRecord, HarborError> {
        self.records
            .get(event_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(event_id.to_owned()))
    }

    #[must_use]
    pub fn due(&self, now: u64, tenant: Option<&str>) -> Vec<PayloadRecord> {
        self.records
            .values()
            .filter(|record| tenant.is_none_or(|candidate| record.tenant == candidate))
            .filter(|record| record.purge_eligible(now))
            .cloned()
            .collect()
    }

    fn current(&self, event_id: &str, expected_version: u64) -> Result<PayloadRecord, HarborError> {
        let record = self.record(event_id)?;
        if record.version != expected_version {
            return Err(HarborError::Conflict(format!(
                "payload {event_id} version conflict: expected {expected_version}, found {}",
                record.version
            )));
        }
        Ok(record)
    }

    fn replace(&mut self, record: PayloadRecord) -> Result<PayloadRecord, HarborError> {
        self.append(&record)?;
        self.records.insert(record.event_id.clone(), record.clone());
        Ok(record)
    }

    fn append(&self, record: &PayloadRecord) -> Result<(), HarborError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| HarborError::Journal(error.to_string()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| HarborError::Journal(error.to_string()))?;
        writeln!(file, "{}", encode(record))
            .and_then(|()| file.sync_data())
            .map_err(|error| HarborError::Journal(error.to_string()))
    }
}

fn validate_record(record: &PayloadRecord) -> Result<(), HarborError> {
    if record.event_id.is_empty() || record.tenant.is_empty() || record.payload_len == 0 {
        return Err(HarborError::Journal(
            "invalid payload identity metadata".into(),
        ));
    }
    if record.purge_after <= record.received_at {
        return Err(HarborError::Journal(
            "invalid payload retention window".into(),
        ));
    }
    match (&record.state, &record.payload, record.purged_at) {
        (PayloadState::Available, Some(payload), None)
            if payload.len() == record.payload_len
                && fingerprint(payload) == record.fingerprint => {}
        (PayloadState::Purged, None, Some(_)) => {}
        _ => {
            return Err(HarborError::Journal(
                "payload or tombstone integrity mismatch".into(),
            ));
        }
    }
    Ok(())
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(HarborError::Validation(format!("{field} is required")));
    }
    Ok(value.to_owned())
}

fn fingerprint(payload: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn encode(record: &PayloadRecord) -> String {
    let fields = [
        hex(record.event_id.as_bytes()),
        hex(record.tenant.as_bytes()),
        match record.state {
            PayloadState::Available => "available".into(),
            PayloadState::Purged => "purged".into(),
        },
        record.payload.as_deref().map_or_else(String::new, hex),
        record.payload_len.to_string(),
        record.fingerprint.clone(),
        record.received_at.to_string(),
        record.purge_after.to_string(),
        optional_number(record.terminal_at),
        record
            .legal_hold
            .as_deref()
            .map_or_else(String::new, |value| hex(value.as_bytes())),
        optional_number(record.purged_at),
        record.version.to_string(),
    ];
    fields.join("\t")
}

fn decode(line: &str) -> Result<PayloadRecord, String> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() != 12 {
        return Err("expected 12 fields".into());
    }
    let state = match fields[2] {
        "available" => PayloadState::Available,
        "purged" => PayloadState::Purged,
        value => return Err(format!("invalid payload state {value}")),
    };
    let payload = if fields[3].is_empty() {
        None
    } else {
        Some(unhex(fields[3])?)
    };
    Ok(PayloadRecord {
        event_id: text_from_hex(fields[0])?,
        tenant: text_from_hex(fields[1])?,
        state,
        payload,
        payload_len: number(fields[4], "payload length")?,
        fingerprint: fields[5].to_owned(),
        received_at: number(fields[6], "received time")?,
        purge_after: number(fields[7], "purge time")?,
        terminal_at: optional_number_decode(fields[8], "terminal time")?,
        legal_hold: if fields[9].is_empty() {
            None
        } else {
            Some(text_from_hex(fields[9])?)
        },
        purged_at: optional_number_decode(fields[10], "purged time")?,
        version: number(fields[11], "version")?,
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn unhex(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex field has odd length".into());
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| "invalid hex field".into())
        })
        .collect()
}

fn text_from_hex(value: &str) -> Result<String, String> {
    String::from_utf8(unhex(value)?).map_err(|_| "hex field is not UTF-8".into())
}

fn number<T: std::str::FromStr>(value: &str, field: &str) -> Result<T, String> {
    value.parse().map_err(|_| format!("invalid {field}"))
}

fn optional_number(value: Option<u64>) -> String {
    value.map_or_else(String::new, |item| item.to_string())
}

fn optional_number_decode(value: &str, field: &str) -> Result<Option<u64>, String> {
    if value.is_empty() {
        Ok(None)
    } else {
        number(value, field).map(Some)
    }
}
