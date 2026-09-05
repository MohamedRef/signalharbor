//! Tenant- and route-bounded replay scope validation.

use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayScope {
    pub tenant_id: String,
    pub route_ids: Vec<String>,
    pub event_ids: Vec<String>,
}

fn normalized(value: &str, field: &str) -> Result<String, String> {
    let result = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if result.is_empty() {
        return Err(format!("replay {field} is required"));
    }
    Ok(result)
}

/// Builds a deterministic replay scope and rejects duplicate identities.
///
/// # Errors
/// Returns an error for blank values, duplicates, or an empty event set.
pub fn build_replay_scope(
    tenant_id: &str,
    route_ids: &[String],
    event_ids: &[String],
) -> Result<ReplayScope, String> {
    let tenant_id = normalized(tenant_id, "tenant")?;
    let mut routes = BTreeSet::new();
    for route_id in route_ids {
        let route_id = normalized(route_id, "route")?;
        if !routes.insert(route_id.to_lowercase()) {
            return Err(format!("duplicate replay route: {route_id}"));
        }
    }
    if routes.is_empty() {
        return Err("replay scope requires at least one route".to_owned());
    }
    let mut events = BTreeSet::new();
    for event_id in event_ids {
        let event_id = normalized(event_id, "event")?;
        if !events.insert(event_id.to_lowercase()) {
            return Err(format!("duplicate replay event: {event_id}"));
        }
    }
    if events.is_empty() {
        return Err("replay scope requires at least one event".to_owned());
    }
    Ok(ReplayScope {
        tenant_id,
        route_ids: routes.into_iter().collect(),
        event_ids: events.into_iter().collect(),
    })
}
