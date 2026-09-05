//! Deterministic retry drain capacity allocation.

use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryDrainCapacityResource {
    pub id: String,
    pub group: String,
    pub available_units: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryDrainCapacityDemand {
    pub id: String,
    pub group: String,
    pub required_units: usize,
    pub priority: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryDrainCapacityAllocation {
    pub resource_id: String,
    pub units: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryDrainCapacityDecision {
    pub demand_id: String,
    pub allocations: Vec<RetryDrainCapacityAllocation>,
    pub shortfall_units: usize,
}

fn required(value: &str, field: &str) -> Result<String, String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Err(format!("retry drain {field} is required"));
    }
    Ok(normalized)
}

/// Allocates retry drain demand by priority without crossing resource groups.
///
/// # Errors
///
/// Returns an error for duplicate identities, blank groups, or zero capacity,
/// demand, and priority declarations.
pub fn allocate_retry_drain_capacity(
    resources: &[RetryDrainCapacityResource],
    demands: &[RetryDrainCapacityDemand],
) -> Result<Vec<RetryDrainCapacityDecision>, String> {
    let mut resource_ids = BTreeSet::new();
    let mut normalized_resources = Vec::with_capacity(resources.len());
    for resource in resources {
        let id = required(&resource.id, "resource")?;
        if !resource_ids.insert(id.to_lowercase()) {
            return Err(format!("duplicate retry drain resource: {id}"));
        }
        if resource.available_units == 0 {
            return Err("retry drain available units must be positive".to_owned());
        }
        normalized_resources.push(RetryDrainCapacityResource {
            id,
            group: required(&resource.group, "resource group")?.to_lowercase(),
            available_units: resource.available_units,
        });
    }
    normalized_resources.sort_by(|left, right| left.id.cmp(&right.id));
    let mut remaining = normalized_resources
        .iter()
        .map(|resource| resource.available_units)
        .collect::<Vec<_>>();

    let mut demand_ids = BTreeSet::new();
    let mut normalized_demands = Vec::with_capacity(demands.len());
    for demand in demands {
        let id = required(&demand.id, "demand")?;
        if !demand_ids.insert(id.to_lowercase()) {
            return Err(format!("duplicate retry drain demand: {id}"));
        }
        if demand.required_units == 0 || demand.priority == 0 {
            return Err("retry drain demand and priority must be positive".to_owned());
        }
        normalized_demands.push(RetryDrainCapacityDemand {
            id,
            group: required(&demand.group, "demand group")?.to_lowercase(),
            required_units: demand.required_units,
            priority: demand.priority,
        });
    }
    normalized_demands.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| right.required_units.cmp(&left.required_units))
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut decisions = Vec::with_capacity(normalized_demands.len());
    for demand in normalized_demands {
        let mut needed = demand.required_units;
        let mut allocations = Vec::new();
        for (index, resource) in normalized_resources.iter().enumerate() {
            if resource.group != demand.group || needed == 0 {
                continue;
            }
            let units = remaining[index].min(needed);
            if units == 0 {
                continue;
            }
            allocations.push(RetryDrainCapacityAllocation {
                resource_id: resource.id.clone(),
                units,
            });
            remaining[index] -= units;
            needed -= units;
        }
        decisions.push(RetryDrainCapacityDecision {
            demand_id: demand.id,
            allocations,
            shortfall_units: needed,
        });
    }
    Ok(decisions)
}
