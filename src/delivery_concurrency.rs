use crate::HarborError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDeliveryDemand {
    pub route_id: String,
    pub queued_deliveries: u32,
    pub maximum_parallel: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolCapacity {
    pub pool_id: String,
    pub route_ids: Vec<String>,
    pub available_workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryConcurrencyPlan {
    pub route_id: String,
    pub assigned_pool: Option<String>,
    pub parallel_deliveries: u32,
    pub deferred_deliveries: u32,
}

/// Allocates delivery workers while preserving each route's concurrency ceiling.
///
/// # Errors
/// Returns validation errors for duplicate routes, invalid ceilings, or duplicate pools.
pub fn plan_delivery_concurrency(
    demands: &[RouteDeliveryDemand],
    pools: &[WorkerPoolCapacity],
) -> Result<Vec<DeliveryConcurrencyPlan>, HarborError> {
    let mut capacity = Vec::new();
    let mut coverage = Vec::new();
    let mut pool_ids = BTreeSet::new();
    for pool in pools {
        let id = required(&pool.pool_id)?;
        if !pool_ids.insert(id.to_lowercase()) || pool.available_workers == 0 {
            return Err(HarborError::Validation(
                "invalid or duplicate worker pool".into(),
            ));
        }
        capacity.push(pool.available_workers);
        coverage.push(
            pool.route_ids
                .iter()
                .map(|route| required(route).map(|v| v.to_lowercase()))
                .collect::<Result<BTreeSet<_>, _>>()?,
        );
    }
    let mut ordered = demands.to_vec();
    ordered.sort_by(|a, b| {
        b.queued_deliveries
            .cmp(&a.queued_deliveries)
            .then(a.route_id.cmp(&b.route_id))
    });
    let mut seen = BTreeSet::new();
    let mut plans = Vec::new();
    for demand in ordered {
        let route = required(&demand.route_id)?;
        if !seen.insert(route.to_lowercase()) || demand.maximum_parallel == 0 {
            return Err(HarborError::Validation(
                "invalid or duplicate delivery demand".into(),
            ));
        }
        let selected = coverage
            .iter()
            .enumerate()
            .filter(|(i, routes)| capacity[*i] > 0 && routes.contains(&route.to_lowercase()))
            .max_by_key(|(i, _)| capacity[*i])
            .map(|(i, _)| i);
        let assigned = selected.map_or(0, |index| {
            capacity[index]
                .min(demand.maximum_parallel)
                .min(demand.queued_deliveries)
        });
        if let Some(index) = selected {
            capacity[index] -= assigned;
        }
        plans.push(DeliveryConcurrencyPlan {
            route_id: route,
            assigned_pool: selected.map(|index| pools[index].pool_id.trim().into()),
            parallel_deliveries: assigned,
            deferred_deliveries: demand.queued_deliveries - assigned,
        });
    }
    Ok(plans)
}

fn required(value: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(
            "delivery concurrency identifier is required".into(),
        ))
    } else {
        Ok(value.into())
    }
}
