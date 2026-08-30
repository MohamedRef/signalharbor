use crate::{HarborError, model::required};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Fixed-window admission limits applied before durable webhook ingestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressQuota {
    pub window_seconds: u64,
    pub max_events: u64,
    pub max_bytes: u64,
    pub max_event_bytes: u64,
}

impl IngressQuota {
    /// Build a validated quota.
    ///
    /// # Errors
    /// Returns [`HarborError::Validation`] when a limit is zero, a window is
    /// longer than one day, or a single event could exceed the window budget.
    pub fn new(
        window_seconds: u64,
        max_events: u64,
        max_bytes: u64,
        max_event_bytes: u64,
    ) -> Result<Self, HarborError> {
        if window_seconds == 0 || window_seconds > 86_400 {
            return Err(HarborError::Validation(
                "ingress quota window must be from 1 through 86400 seconds".into(),
            ));
        }
        if max_events == 0 || max_bytes == 0 || max_event_bytes == 0 {
            return Err(HarborError::Validation(
                "ingress quota limits must be positive".into(),
            ));
        }
        if max_event_bytes > max_bytes {
            return Err(HarborError::Validation(
                "maximum event bytes cannot exceed the window byte budget".into(),
            ));
        }
        Ok(Self {
            window_seconds,
            max_events,
            max_bytes,
            max_event_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressRejectReason {
    EventTooLarge,
    EventQuotaExceeded,
    ByteQuotaExceeded,
}

impl IngressRejectReason {
    const fn code(self) -> &'static str {
        match self {
            Self::EventTooLarge => "event_too_large",
            Self::EventQuotaExceeded => "event_quota_exceeded",
            Self::ByteQuotaExceeded => "byte_quota_exceeded",
        }
    }

    fn from_code(value: &str) -> Result<Self, HarborError> {
        match value {
            "event_too_large" => Ok(Self::EventTooLarge),
            "event_quota_exceeded" => Ok(Self::EventQuotaExceeded),
            "byte_quota_exceeded" => Ok(Self::ByteQuotaExceeded),
            _ => Err(HarborError::Journal(
                "unknown ingress rejection reason".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub accepted: bool,
    pub duplicate: bool,
    pub reason: Option<IngressRejectReason>,
    pub window_started_at: u64,
    pub window_ends_at: u64,
    pub remaining_events: u64,
    pub remaining_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressUsage {
    pub tenant: String,
    pub window_started_at: u64,
    pub window_ends_at: u64,
    pub accepted_events: u64,
    pub accepted_bytes: u64,
    pub rejected_events: u64,
    pub remaining_events: u64,
    pub remaining_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UsageCounter {
    window_started_at: u64,
    accepted_events: u64,
    accepted_bytes: u64,
    rejected_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Receipt {
    tenant: String,
    request_bytes: u64,
    decided_at: u64,
    decision: AdmissionDecision,
}

/// Deterministic, serializable ingress quota coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressGate {
    default_quota: IngressQuota,
    tenant_quotas: BTreeMap<String, IngressQuota>,
    usage: BTreeMap<String, UsageCounter>,
    receipts: BTreeMap<String, Receipt>,
    receipt_retention_seconds: u64,
}

impl IngressGate {
    /// Create an admission gate with a default quota and receipt retention.
    ///
    /// # Errors
    /// Returns [`HarborError::Validation`] for zero retention or retention
    /// longer than thirty days.
    pub fn new(
        default_quota: IngressQuota,
        receipt_retention_seconds: u64,
    ) -> Result<Self, HarborError> {
        if receipt_retention_seconds == 0 || receipt_retention_seconds > 2_592_000 {
            return Err(HarborError::Validation(
                "receipt retention must be from 1 through 2592000 seconds".into(),
            ));
        }
        Ok(Self {
            default_quota,
            tenant_quotas: BTreeMap::new(),
            usage: BTreeMap::new(),
            receipts: BTreeMap::new(),
            receipt_retention_seconds,
        })
    }

    /// Set a tenant-specific override without discarding its active usage.
    ///
    /// # Errors
    /// Returns [`HarborError::Validation`] when the tenant is blank or too long.
    pub fn set_tenant_quota(
        &mut self,
        tenant: &str,
        quota: IngressQuota,
    ) -> Result<(), HarborError> {
        let tenant = required(tenant, "tenant", 80)?.to_ascii_lowercase();
        self.tenant_quotas.insert(tenant, quota);
        Ok(())
    }

    /// Remove a tenant override and return to the default quota.
    ///
    /// # Errors
    /// Returns [`HarborError::NotFound`] when no override exists.
    pub fn remove_tenant_quota(&mut self, tenant: &str) -> Result<(), HarborError> {
        let tenant = required(tenant, "tenant", 80)?.to_ascii_lowercase();
        if self.tenant_quotas.remove(&tenant).is_none() {
            return Err(HarborError::NotFound(format!(
                "ingress quota for tenant {tenant}"
            )));
        }
        Ok(())
    }

    /// Decide whether a request may enter the durable ingestion pipeline.
    /// Repeating a retained request identifier returns the original decision
    /// without consuming quota again.
    ///
    /// # Errors
    /// Returns [`HarborError::Validation`] for invalid tenant, request ID, or a
    /// zero request size.
    pub fn admit(
        &mut self,
        tenant: &str,
        request_id: &str,
        request_bytes: u64,
        now: u64,
    ) -> Result<AdmissionDecision, HarborError> {
        let tenant = required(tenant, "tenant", 80)?.to_ascii_lowercase();
        let request_id = required(request_id, "request id", 200)?;
        if request_bytes == 0 {
            return Err(HarborError::Validation(
                "request bytes must be positive".into(),
            ));
        }
        self.prune_receipts(now);
        if let Some(receipt) = self.receipts.get(&request_id) {
            if receipt.tenant != tenant || receipt.request_bytes != request_bytes {
                return Err(HarborError::Conflict(
                    "request id was already used with different ingress data".into(),
                ));
            }
            let mut decision = receipt.decision.clone();
            decision.duplicate = true;
            return Ok(decision);
        }
        let quota = self.quota_for(&tenant).clone();
        let window_started_at = now - (now % quota.window_seconds);
        let window_ends_at = window_started_at
            .checked_add(quota.window_seconds)
            .ok_or_else(|| {
                HarborError::Validation("ingress window exceeds timestamp range".into())
            })?;
        let counter = self.usage.entry(tenant.clone()).or_insert(UsageCounter {
            window_started_at,
            accepted_events: 0,
            accepted_bytes: 0,
            rejected_events: 0,
        });
        if counter.window_started_at != window_started_at {
            *counter = UsageCounter {
                window_started_at,
                accepted_events: 0,
                accepted_bytes: 0,
                rejected_events: 0,
            };
        }
        let reason = if request_bytes > quota.max_event_bytes {
            Some(IngressRejectReason::EventTooLarge)
        } else if counter.accepted_events >= quota.max_events {
            Some(IngressRejectReason::EventQuotaExceeded)
        } else if counter.accepted_bytes.saturating_add(request_bytes) > quota.max_bytes {
            Some(IngressRejectReason::ByteQuotaExceeded)
        } else {
            None
        };
        if reason.is_some() {
            counter.rejected_events = counter.rejected_events.saturating_add(1);
        } else {
            counter.accepted_events = counter.accepted_events.saturating_add(1);
            counter.accepted_bytes = counter.accepted_bytes.saturating_add(request_bytes);
        }
        let decision = AdmissionDecision {
            accepted: reason.is_none(),
            duplicate: false,
            reason,
            window_started_at,
            window_ends_at,
            remaining_events: quota.max_events.saturating_sub(counter.accepted_events),
            remaining_bytes: quota.max_bytes.saturating_sub(counter.accepted_bytes),
        };
        self.receipts.insert(
            request_id,
            Receipt {
                tenant,
                request_bytes,
                decided_at: now,
                decision: decision.clone(),
            },
        );
        Ok(decision)
    }

    /// Return current usage, treating an elapsed window as empty without
    /// mutating the coordinator.
    ///
    /// # Errors
    /// Returns [`HarborError::Validation`] for an invalid tenant.
    pub fn usage(&self, tenant: &str, now: u64) -> Result<IngressUsage, HarborError> {
        let tenant = required(tenant, "tenant", 80)?.to_ascii_lowercase();
        let quota = self.quota_for(&tenant);
        let window_started_at = now - (now % quota.window_seconds);
        let window_ends_at = window_started_at
            .checked_add(quota.window_seconds)
            .ok_or_else(|| {
                HarborError::Validation("ingress window exceeds timestamp range".into())
            })?;
        let counter = self
            .usage
            .get(&tenant)
            .filter(|counter| counter.window_started_at == window_started_at);
        let accepted_events = counter.map_or(0, |value| value.accepted_events);
        let accepted_bytes = counter.map_or(0, |value| value.accepted_bytes);
        Ok(IngressUsage {
            tenant,
            window_started_at,
            window_ends_at,
            accepted_events,
            accepted_bytes,
            rejected_events: counter.map_or(0, |value| value.rejected_events),
            remaining_events: quota.max_events.saturating_sub(accepted_events),
            remaining_bytes: quota.max_bytes.saturating_sub(accepted_bytes),
        })
    }

    /// Serialize quotas, active counters, and idempotency receipts.
    #[must_use]
    pub fn snapshot(&self) -> String {
        let mut rows = vec![format!(
            "INGRESS|1|{}|{}|{}|{}|{}",
            self.default_quota.window_seconds,
            self.default_quota.max_events,
            self.default_quota.max_bytes,
            self.default_quota.max_event_bytes,
            self.receipt_retention_seconds
        )];
        for (tenant, quota) in &self.tenant_quotas {
            rows.push(format!(
                "Q|{}|{}|{}|{}|{}",
                encode(tenant),
                quota.window_seconds,
                quota.max_events,
                quota.max_bytes,
                quota.max_event_bytes
            ));
        }
        for (tenant, usage) in &self.usage {
            rows.push(format!(
                "U|{}|{}|{}|{}|{}",
                encode(tenant),
                usage.window_started_at,
                usage.accepted_events,
                usage.accepted_bytes,
                usage.rejected_events
            ));
        }
        for (request_id, receipt) in &self.receipts {
            rows.push(format!(
                "R|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                encode(request_id),
                encode(&receipt.tenant),
                receipt.request_bytes,
                receipt.decided_at,
                u8::from(receipt.decision.accepted),
                receipt
                    .decision
                    .reason
                    .map_or("-", IngressRejectReason::code),
                receipt.decision.window_started_at,
                receipt.decision.window_ends_at,
                receipt.decision.remaining_events,
                receipt.decision.remaining_bytes,
                u8::from(receipt.decision.duplicate)
            ));
        }
        format!("{}\n", rows.join("\n"))
    }

    /// Restore and validate a durable ingress snapshot.
    ///
    /// # Errors
    /// Returns [`HarborError::Journal`] for malformed, duplicate, or internally
    /// inconsistent snapshot records.
    pub fn restore(snapshot: &str) -> Result<Self, HarborError> {
        let mut lines = snapshot.lines();
        let header = lines
            .next()
            .ok_or_else(|| HarborError::Journal("missing ingress header".into()))?;
        let header = header.split('|').collect::<Vec<_>>();
        if header.len() != 7 || header[0] != "INGRESS" || header[1] != "1" {
            return Err(HarborError::Journal("unsupported ingress header".into()));
        }
        let quota = IngressQuota::new(
            number(header[2], "window")?,
            number(header[3], "events")?,
            number(header[4], "bytes")?,
            number(header[5], "event bytes")?,
        )
        .map_err(|error| HarborError::Journal(error.to_string()))?;
        let mut gate = Self::new(quota, number(header[6], "retention")?)
            .map_err(|error| HarborError::Journal(error.to_string()))?;
        for line in lines.filter(|line| !line.is_empty()) {
            let fields = line.split('|').collect::<Vec<_>>();
            match fields.first().copied() {
                Some("Q") if fields.len() == 6 => {
                    let tenant = decoded_identity(fields[1], "tenant", 80, true)?;
                    let quota = IngressQuota::new(
                        number(fields[2], "window")?,
                        number(fields[3], "events")?,
                        number(fields[4], "bytes")?,
                        number(fields[5], "event bytes")?,
                    )
                    .map_err(|error| HarborError::Journal(error.to_string()))?;
                    if gate.tenant_quotas.insert(tenant, quota).is_some() {
                        return Err(HarborError::Journal("duplicate tenant quota".into()));
                    }
                }
                Some("U") if fields.len() == 6 => {
                    let tenant = decoded_identity(fields[1], "tenant", 80, true)?;
                    let usage = UsageCounter {
                        window_started_at: number(fields[2], "window start")?,
                        accepted_events: number(fields[3], "accepted events")?,
                        accepted_bytes: number(fields[4], "accepted bytes")?,
                        rejected_events: number(fields[5], "rejected events")?,
                    };
                    let quota = gate.quota_for(&tenant);
                    if usage.accepted_events > quota.max_events
                        || usage.accepted_bytes > quota.max_bytes
                    {
                        return Err(HarborError::Journal("ingress usage exceeds quota".into()));
                    }
                    if gate.usage.insert(tenant, usage).is_some() {
                        return Err(HarborError::Journal("duplicate tenant usage".into()));
                    }
                }
                Some("R") if fields.len() == 12 => {
                    let request_id = decoded_identity(fields[1], "request id", 200, false)?;
                    let reason = if fields[6] == "-" {
                        None
                    } else {
                        Some(IngressRejectReason::from_code(fields[6])?)
                    };
                    let accepted = boolean(fields[5])?;
                    if accepted == reason.is_some() {
                        return Err(HarborError::Journal("inconsistent ingress decision".into()));
                    }
                    let receipt = Receipt {
                        tenant: decoded_identity(fields[2], "tenant", 80, true)?,
                        request_bytes: number(fields[3], "request bytes")?,
                        decided_at: number(fields[4], "decision time")?,
                        decision: AdmissionDecision {
                            accepted,
                            duplicate: boolean(fields[11])?,
                            reason,
                            window_started_at: number(fields[7], "receipt window")?,
                            window_ends_at: number(fields[8], "receipt window end")?,
                            remaining_events: number(fields[9], "remaining events")?,
                            remaining_bytes: number(fields[10], "remaining bytes")?,
                        },
                    };
                    if receipt.request_bytes == 0
                        || gate.receipts.insert(request_id, receipt).is_some()
                    {
                        return Err(HarborError::Journal(
                            "invalid or duplicate ingress receipt".into(),
                        ));
                    }
                }
                _ => return Err(HarborError::Journal("invalid ingress record".into())),
            }
        }
        Ok(gate)
    }

    fn quota_for(&self, tenant: &str) -> &IngressQuota {
        self.tenant_quotas
            .get(tenant)
            .unwrap_or(&self.default_quota)
    }

    fn prune_receipts(&mut self, now: u64) {
        self.receipts.retain(|_, receipt| {
            receipt
                .decided_at
                .saturating_add(self.receipt_retention_seconds)
                > now
        });
    }
}

fn encode(value: &str) -> String {
    value.as_bytes().iter().fold(
        String::with_capacity(value.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        },
    )
}

fn decode(value: &str) -> Result<String, HarborError> {
    if !value.len().is_multiple_of(2) {
        return Err(HarborError::Journal("invalid ingress text encoding".into()));
    }
    let bytes = (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HarborError::Journal("invalid ingress text encoding".into()))?;
    String::from_utf8(bytes).map_err(|_| HarborError::Journal("invalid ingress utf-8".into()))
}

fn decoded_identity(
    encoded: &str,
    field: &str,
    maximum: usize,
    lowercase: bool,
) -> Result<String, HarborError> {
    let value = decode(encoded)?;
    let normalized = required(&value, field, maximum)
        .map_err(|_| HarborError::Journal(format!("invalid ingress {field}")))?;
    if normalized != value || (lowercase && value != value.to_ascii_lowercase()) {
        return Err(HarborError::Journal(format!(
            "non-canonical ingress {field}"
        )));
    }
    Ok(value)
}

fn number<T: std::str::FromStr>(value: &str, field: &str) -> Result<T, HarborError> {
    value
        .parse()
        .map_err(|_| HarborError::Journal(format!("invalid ingress {field}")))
}

fn boolean(value: &str) -> Result<bool, HarborError> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(HarborError::Journal("invalid ingress boolean".into())),
    }
}
