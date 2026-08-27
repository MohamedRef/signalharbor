use std::collections::BTreeMap;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryDemand {
    pub tenant: String,
    pub pending: u32,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryBudgetAllocation {
    pub tenant: String,
    pub granted: u32,
    pub deferred: u32,
}

/// Shares a bounded retry budget fairly between tenants with outstanding work.
///
/// Each active tenant receives one slot per round, repeated according to its
/// weight, so a noisy tenant cannot consume the entire retry window.
///
/// # Errors
///
/// Returns validation for blank or duplicate tenants, zero weights, or an
/// empty demand set.
pub fn allocate_retry_budget(
    demands: &[RetryDemand],
    total_budget: u32,
) -> Result<Vec<RetryBudgetAllocation>, HarborError> {
    if demands.is_empty() {
        return Err(HarborError::Validation(
            "retry demand cannot be empty".into(),
        ));
    }

    let mut pending = BTreeMap::new();
    let mut weights = BTreeMap::new();
    for demand in demands {
        let tenant = demand.tenant.trim().to_ascii_lowercase();
        if tenant.is_empty() || tenant.len() > 80 {
            return Err(HarborError::Validation(
                "tenant must contain 1 through 80 characters".into(),
            ));
        }
        if demand.weight == 0 {
            return Err(HarborError::Validation(
                "retry weight must be positive".into(),
            ));
        }
        if pending.insert(tenant.clone(), demand.pending).is_some() {
            return Err(HarborError::Conflict(format!(
                "duplicate retry demand for tenant {tenant}"
            )));
        }
        weights.insert(tenant, demand.weight);
    }

    let original = pending.clone();
    let mut granted = BTreeMap::<String, u32>::new();
    let mut remaining_budget = total_budget;
    while remaining_budget > 0 && pending.values().any(|value| *value > 0) {
        for (tenant, weight) in &weights {
            let Some(available) = pending.get_mut(tenant) else {
                continue;
            };
            let give = (*available).min(*weight).min(remaining_budget);
            *available -= give;
            *granted.entry(tenant.clone()).or_default() += give;
            remaining_budget -= give;
            if remaining_budget == 0 {
                break;
            }
        }
    }

    Ok(original
        .into_iter()
        .map(|(tenant, demand)| {
            let grant = granted.get(&tenant).copied().unwrap_or_default();
            RetryBudgetAllocation {
                tenant,
                granted: grant,
                deferred: demand - grant,
            }
        })
        .collect())
}
