use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningKeyWindow {
    pub route_id: String,
    pub key_id: String,
    pub active_from: i64,
    pub active_until: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningOverlapPolicy {
    pub route_id: String,
    pub minimum_overlap_seconds: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SigningOverlapReadiness {
    Gap,
    Insufficient,
    Ready,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningOverlapAssessment {
    pub route_id: String,
    pub retiring_key_id: String,
    pub replacement_key_id: String,
    pub overlap_seconds: u64,
    pub readiness: SigningOverlapReadiness,
}

/// Verifies that each route rotates signing keys with a safe verification overlap.
pub fn assess_signing_overlap(
    policies: &[SigningOverlapPolicy],
    windows: &[SigningKeyWindow],
) -> Result<Vec<SigningOverlapAssessment>, HarborError> {
    let mut configured = BTreeMap::new();
    for policy in policies {
        let route = required(&policy.route_id, "signing overlap route")?;
        let key = route.to_ascii_lowercase();
        if configured
            .insert(key, (route.clone(), policy.minimum_overlap_seconds))
            .is_some()
            || policy.minimum_overlap_seconds == 0
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate signing overlap policy {route}"
            )));
        }
    }
    let mut identities = BTreeSet::new();
    let mut by_route: BTreeMap<String, Vec<(String, i64, i64)>> = BTreeMap::new();
    for window in windows {
        let route = required(&window.route_id, "signing key route")?;
        let key_id = required(&window.key_id, "signing key identity")?;
        let route_key = route.to_ascii_lowercase();
        if !configured.contains_key(&route_key)
            || window.active_until <= window.active_from
            || !identities.insert((route_key.clone(), key_id.to_ascii_lowercase()))
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate signing key window {route}/{key_id}"
            )));
        }
        by_route.entry(route_key).or_default().push((
            key_id,
            window.active_from,
            window.active_until,
        ));
    }
    let mut rows = Vec::new();
    for (route_key, mut keys) in by_route {
        let (route, minimum) = configured.get(&route_key).expect("route validated");
        keys.sort_by_key(|row| row.1);
        for pair in keys.windows(2) {
            let retiring = &pair[0];
            let replacement = &pair[1];
            let overlap = retiring.2.saturating_sub(replacement.1).unsigned_abs();
            let readiness = if replacement.1 > retiring.2 {
                SigningOverlapReadiness::Gap
            } else if overlap < *minimum {
                SigningOverlapReadiness::Insufficient
            } else {
                SigningOverlapReadiness::Ready
            };
            rows.push(SigningOverlapAssessment {
                route_id: route.clone(),
                retiring_key_id: retiring.0.clone(),
                replacement_key_id: replacement.0.clone(),
                overlap_seconds: if replacement.1 > retiring.2 {
                    0
                } else {
                    overlap
                },
                readiness,
            });
        }
    }
    rows.sort_by(|a, b| {
        a.readiness
            .cmp(&b.readiness)
            .then_with(|| a.route_id.cmp(&b.route_id))
            .then_with(|| a.retiring_key_id.cmp(&b.retiring_key_id))
    });
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
