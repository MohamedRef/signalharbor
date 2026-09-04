use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutPolicy {
    pub tenant_id: String,
    pub maximum_routes_per_event: u64,
    pub maximum_concurrent_deliveries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteFanoutSample {
    pub event_id: String,
    pub tenant_id: String,
    pub matched_routes: u64,
    pub active_deliveries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutAssessment {
    pub event_id: String,
    pub tenant_id: String,
    pub excess_routes: u64,
    pub excess_concurrency: u64,
    pub admitted: bool,
}

/// Enforces tenant fanout and concurrency ceilings before delivery rows are created.
///
/// # Errors
///
/// Returns validation errors for invalid policies, duplicate events, and unknown tenants.
pub fn assess_route_fanout(
    policies: &[FanoutPolicy],
    samples: &[RouteFanoutSample],
) -> Result<Vec<FanoutAssessment>, HarborError> {
    let mut configured = BTreeMap::new();
    for policy in policies {
        let tenant = required(&policy.tenant_id, "fanout tenant")?;
        let key = tenant.to_ascii_lowercase();
        if configured.contains_key(&key)
            || policy.maximum_routes_per_event == 0
            || policy.maximum_concurrent_deliveries == 0
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate fanout policy {tenant}"
            )));
        }
        configured.insert(
            key,
            (
                tenant,
                policy.maximum_routes_per_event,
                policy.maximum_concurrent_deliveries,
            ),
        );
    }
    let mut seen = BTreeSet::new();
    let mut rows = Vec::with_capacity(samples.len());
    for sample in samples {
        let event = required(&sample.event_id, "fanout event")?;
        if !seen.insert(event.to_ascii_lowercase()) {
            return Err(HarborError::Validation(format!(
                "duplicate fanout event {event}"
            )));
        }
        let tenant = required(&sample.tenant_id, "fanout sample tenant")?;
        let Some((canonical, route_limit, concurrency_limit)) =
            configured.get(&tenant.to_ascii_lowercase())
        else {
            return Err(HarborError::Validation(format!(
                "tenant {tenant} has no fanout policy"
            )));
        };
        let excess_routes = sample.matched_routes.saturating_sub(*route_limit);
        let excess_concurrency = sample
            .active_deliveries
            .saturating_add(sample.matched_routes)
            .saturating_sub(*concurrency_limit);
        rows.push(FanoutAssessment {
            event_id: event,
            tenant_id: canonical.clone(),
            excess_routes,
            excess_concurrency,
            admitted: excess_routes == 0 && excess_concurrency == 0,
        });
    }
    rows.sort_by(|a, b| {
        a.admitted
            .cmp(&b.admitted)
            .then_with(|| b.excess_routes.cmp(&a.excess_routes))
            .then_with(|| a.event_id.cmp(&b.event_id))
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
