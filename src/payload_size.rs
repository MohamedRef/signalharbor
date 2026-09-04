use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSizePolicy {
    pub route_id: String,
    pub maximum_bytes: u64,
    pub warning_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookPayloadSample {
    pub route_id: String,
    pub event_id: String,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PayloadSizeReadiness {
    Rejected,
    Warning,
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSizeAssessment {
    pub route_id: String,
    pub event_id: String,
    pub readiness: PayloadSizeReadiness,
    pub payload_bytes: u64,
    pub remaining_bytes: u64,
}

/// Assesses webhook payloads against route-specific admission envelopes.
///
/// # Errors
///
/// Rejects blank or duplicate identities, missing policies, zero limits, or
/// warning percentages outside one through one hundred.
pub fn assess_payload_sizes(
    policies: &[PayloadSizePolicy],
    samples: &[WebhookPayloadSample],
) -> Result<Vec<PayloadSizeAssessment>, HarborError> {
    let mut limits = BTreeMap::new();
    for policy in policies {
        let route_id = policy.route_id.trim();
        if route_id.is_empty() {
            return Err(HarborError::Validation("payload route is required".into()));
        }
        let key = route_id.to_ascii_lowercase();
        if limits.contains_key(&key) {
            return Err(HarborError::Validation(format!(
                "duplicate payload route {route_id}"
            )));
        }
        if policy.maximum_bytes == 0 || !(1..=100).contains(&policy.warning_percent) {
            return Err(HarborError::Validation(format!(
                "payload route {route_id} has invalid limits"
            )));
        }
        limits.insert(
            key,
            (
                route_id.to_owned(),
                policy.maximum_bytes,
                policy.warning_percent,
            ),
        );
    }
    let mut events = BTreeSet::new();
    let mut rows = Vec::with_capacity(samples.len());
    for sample in samples {
        let route = sample.route_id.trim();
        let event_id = sample.event_id.trim();
        if route.is_empty() || event_id.is_empty() {
            return Err(HarborError::Validation(
                "payload route and event are required".into(),
            ));
        }
        let route_key = route.to_ascii_lowercase();
        let Some((route_id, maximum, warning_percent)) = limits.get(&route_key) else {
            return Err(HarborError::Validation(format!(
                "payload route {route} has no policy"
            )));
        };
        if !events.insert((route_key, event_id.to_owned())) {
            return Err(HarborError::Validation(format!(
                "duplicate payload sample {route}/{event_id}"
            )));
        }
        let warning_bytes = maximum
            .checked_mul(u64::from(*warning_percent))
            .ok_or_else(|| HarborError::Validation("payload warning threshold overflows".into()))?
            .div_ceil(100);
        let readiness = if sample.payload_bytes > *maximum {
            PayloadSizeReadiness::Rejected
        } else if sample.payload_bytes >= warning_bytes {
            PayloadSizeReadiness::Warning
        } else {
            PayloadSizeReadiness::Accepted
        };
        rows.push(PayloadSizeAssessment {
            route_id: route_id.clone(),
            event_id: event_id.to_owned(),
            readiness,
            payload_bytes: sample.payload_bytes,
            remaining_bytes: maximum.saturating_sub(sample.payload_bytes),
        });
    }
    rows.sort_by(|left, right| {
        left.readiness
            .cmp(&right.readiness)
            .then_with(|| left.route_id.cmp(&right.route_id))
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    Ok(rows)
}
