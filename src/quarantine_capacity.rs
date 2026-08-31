use std::collections::{BTreeMap, HashSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedEvent {
    pub event_id: String,
    pub tenant: String,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuarantineLimit {
    pub tenant: String,
    pub maximum_events: usize,
    pub maximum_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineCapacityBreach {
    pub tenant: String,
    pub event_count: usize,
    pub payload_bytes: u64,
    pub excess_events: usize,
    pub excess_bytes: u64,
}

/// Finds tenant quarantine queues that exceed count or byte limits.
///
/// # Errors
///
/// Returns validation for duplicate events, invalid limits, or events without a limit.
pub fn assess_quarantine_capacity(
    events: &[QuarantinedEvent],
    limits: &[TenantQuarantineLimit],
) -> Result<Vec<QuarantineCapacityBreach>, HarborError> {
    let mut configured = BTreeMap::new();
    for limit in limits {
        let tenant = limit.tenant.trim();
        if tenant.is_empty()
            || limit.maximum_events == 0
            || limit.maximum_bytes == 0
            || configured.insert(tenant.to_owned(), limit).is_some()
        {
            return Err(HarborError::Validation(
                "quarantine limits must be unique, named, and positive".into(),
            ));
        }
    }
    let mut seen = HashSet::new();
    let mut usage: BTreeMap<String, (usize, u64)> = BTreeMap::new();
    for event in events {
        let event_id = event.event_id.trim();
        let tenant = event.tenant.trim();
        if event_id.is_empty() || tenant.is_empty() || !seen.insert(event_id.to_owned()) {
            return Err(HarborError::Validation(
                "quarantine events must have unique identities and tenants".into(),
            ));
        }
        if !configured.contains_key(tenant) {
            return Err(HarborError::Validation(format!(
                "quarantine limit missing for tenant {tenant}"
            )));
        }
        let entry = usage.entry(tenant.to_owned()).or_default();
        entry.0 += 1;
        entry.1 = entry.1.saturating_add(event.payload_bytes);
    }
    let mut breaches = Vec::new();
    for (tenant, (event_count, payload_bytes)) in usage {
        let limit = configured[&tenant];
        let excess_events = event_count.saturating_sub(limit.maximum_events);
        let excess_bytes = payload_bytes.saturating_sub(limit.maximum_bytes);
        if excess_events > 0 || excess_bytes > 0 {
            breaches.push(QuarantineCapacityBreach {
                tenant,
                event_count,
                payload_bytes,
                excess_events,
                excess_bytes,
            });
        }
    }
    breaches.sort_by(|a, b| {
        b.excess_bytes
            .cmp(&a.excess_bytes)
            .then_with(|| b.excess_events.cmp(&a.excess_events))
            .then_with(|| a.tenant.cmp(&b.tenant))
    });
    Ok(breaches)
}
