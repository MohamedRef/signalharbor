use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverEndpoint {
    pub endpoint: String,
    pub priority: u16,
    pub healthy: bool,
    pub available_slots: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverAllocation {
    pub endpoint: String,
    pub allocated: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverPlan {
    pub allocations: Vec<FailoverAllocation>,
    pub unallocated: u64,
}

/// Allocates failed-route traffic to healthy endpoints in priority order.
///
/// # Errors
///
/// Returns validation for zero demand, blank endpoint identities, zero
/// priorities, or duplicate endpoint identities.
pub fn plan_route_failover(
    endpoints: &[FailoverEndpoint],
    deliveries: u64,
) -> Result<FailoverPlan, HarborError> {
    if deliveries == 0 {
        return Err(HarborError::Validation(
            "failover demand must be positive".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut candidates = endpoints.to_vec();
    for endpoint in &mut candidates {
        endpoint.endpoint = endpoint.endpoint.trim().to_owned();
        if endpoint.endpoint.is_empty() || endpoint.priority == 0 {
            return Err(HarborError::Validation(
                "failover endpoint and positive priority are required".into(),
            ));
        }
        if !seen.insert(endpoint.endpoint.to_ascii_lowercase()) {
            return Err(HarborError::Validation(format!(
                "duplicate failover endpoint {}",
                endpoint.endpoint
            )));
        }
    }
    candidates.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(a.endpoint.cmp(&b.endpoint))
    });
    let mut remaining = deliveries;
    let mut allocations = Vec::new();
    for endpoint in candidates
        .into_iter()
        .filter(|row| row.healthy && row.available_slots > 0)
    {
        let allocated = remaining.min(endpoint.available_slots);
        if allocated > 0 {
            allocations.push(FailoverAllocation {
                endpoint: endpoint.endpoint,
                allocated,
            });
            remaining -= allocated;
        }
        if remaining == 0 {
            break;
        }
    }
    Ok(FailoverPlan {
        allocations,
        unallocated: remaining,
    })
}
