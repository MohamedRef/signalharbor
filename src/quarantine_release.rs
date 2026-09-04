use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseCapacity {
    pub route_id: String,
    pub events_per_minute: u64,
    pub maximum_batch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineReleaseRequest {
    pub request_id: String,
    pub route_id: String,
    pub event_count: u64,
    pub priority: u8,
    pub evidence_approved: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineReleaseAllocation {
    pub request_id: String,
    pub route_id: String,
    pub released_events: u64,
    pub deferred_events: u64,
    pub minutes_required: u64,
    pub blocked_reason: Option<String>,
}

/// Allocates bounded quarantine releases while preserving approval and route throughput controls.
///
/// # Errors
///
/// Rejects invalid capacities, requests, route references, windows, and overflow.
pub fn allocate_quarantine_releases(
    capacities: &[ReleaseCapacity],
    requests: &[QuarantineReleaseRequest],
    available_minutes: u64,
) -> Result<Vec<QuarantineReleaseAllocation>, HarborError> {
    if available_minutes == 0 {
        return Err(HarborError::Validation(
            "quarantine release window must be positive".into(),
        ));
    }
    let mut remaining = BTreeMap::new();
    for capacity in capacities {
        let route = required(&capacity.route_id, "quarantine release route")?;
        let key = route.to_ascii_lowercase();
        if capacity.events_per_minute == 0
            || capacity.maximum_batch == 0
            || remaining.contains_key(&key)
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate release capacity {route}"
            )));
        }
        let window = capacity
            .events_per_minute
            .checked_mul(available_minutes)
            .ok_or_else(|| {
                HarborError::Validation("quarantine release capacity overflows".into())
            })?;
        remaining.insert(
            key,
            (
                route,
                window,
                capacity.maximum_batch,
                capacity.events_per_minute,
            ),
        );
    }
    let mut ids = BTreeSet::new();
    let mut ordered = requests.to_vec();
    ordered.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.request_id.cmp(&b.request_id))
    });
    let mut rows = Vec::with_capacity(ordered.len());
    for request in ordered {
        let request_id = required(&request.request_id, "quarantine release request")?;
        let route_key = request.route_id.trim().to_ascii_lowercase();
        if !ids.insert(request_id.to_ascii_lowercase())
            || request.event_count == 0
            || request.priority == 0
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate quarantine release {request_id}"
            )));
        }
        let Some((route, available, maximum_batch, rate)) = remaining.get_mut(&route_key) else {
            return Err(HarborError::Validation(format!(
                "release request {request_id} has no route capacity"
            )));
        };
        let released = if request.evidence_approved {
            request.event_count.min(*maximum_batch).min(*available)
        } else {
            0
        };
        *available -= released;
        rows.push(QuarantineReleaseAllocation {
            request_id,
            route_id: route.clone(),
            released_events: released,
            deferred_events: request.event_count - released,
            minutes_required: if released == 0 {
                0
            } else {
                released.div_ceil(*rate)
            },
            blocked_reason: if request.evidence_approved {
                None
            } else {
                Some("release_evidence_not_approved".into())
            },
        });
    }
    Ok(rows)
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(format!("{field} is required")))
    } else {
        Ok(value.to_owned())
    }
}
