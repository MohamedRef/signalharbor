use crate::HarborError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainDemand {
    pub route_id: String,
    pub queued_events: u64,
    pub seconds_remaining: u64,
    pub critical: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainCapacity {
    pub worker_id: String,
    pub events_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainPlan {
    pub route_id: String,
    pub worker_id: Option<String>,
    pub projected_seconds: Option<u64>,
    pub state: String,
}

/// Assigns one drain worker to each route with earliest critical deadlines first.
///
/// # Errors
/// Rejects empty identities, duplicates, zero capacity, and zero drain horizons.
pub fn plan_route_drain_capacity(
    demands: &[DrainDemand],
    capacities: &[DrainCapacity],
) -> Result<Vec<DrainPlan>, HarborError> {
    let mut seen_routes = BTreeSet::new();
    let mut queue = Vec::new();
    for demand in demands {
        let route = demand.route_id.trim();
        if route.is_empty()
            || demand.seconds_remaining == 0
            || !seen_routes.insert(route.to_ascii_lowercase())
        {
            return Err(HarborError::Validation("invalid route drain demand".into()));
        }
        queue.push((route, demand));
    }
    queue.sort_by(|(left_id, left), (right_id, right)| {
        right
            .critical
            .cmp(&left.critical)
            .then(left.seconds_remaining.cmp(&right.seconds_remaining))
            .then(right.queued_events.cmp(&left.queued_events))
            .then(left_id.cmp(right_id))
    });
    let mut seen_workers = BTreeSet::new();
    let mut workers = Vec::new();
    for capacity in capacities {
        let worker = capacity.worker_id.trim();
        if worker.is_empty()
            || capacity.events_per_second == 0
            || !seen_workers.insert(worker.to_ascii_lowercase())
        {
            return Err(HarborError::Validation(
                "invalid drain worker capacity".into(),
            ));
        }
        workers.push((worker, capacity.events_per_second));
    }
    workers.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let mut plans = Vec::with_capacity(queue.len());
    for (route, demand) in queue {
        if workers.is_empty() {
            plans.push(DrainPlan {
                route_id: route.into(),
                worker_id: None,
                projected_seconds: None,
                state: "unassigned".into(),
            });
            continue;
        }
        let (worker, rate) = workers.remove(0);
        let projected = demand.queued_events.div_ceil(rate);
        let state = if projected <= demand.seconds_remaining {
            "ready"
        } else {
            "deadline_risk"
        };
        plans.push(DrainPlan {
            route_id: route.into(),
            worker_id: Some(worker.into()),
            projected_seconds: Some(projected),
            state: state.into(),
        });
    }
    Ok(plans)
}
