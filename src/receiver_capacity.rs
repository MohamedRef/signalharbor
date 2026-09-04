use std::collections::BTreeSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverCapacityInput {
    pub route_id: String,
    pub endpoint: String,
    pub max_concurrency: u32,
    pub active_leases: u32,
    pub pending_due: u64,
    pub average_attempt_millis: u64,
    pub drain_objective_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReceiverCapacityState {
    Saturated,
    Constrained,
    Healthy,
    Idle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverCapacityAssessment {
    pub route_id: String,
    pub endpoint: String,
    pub state: ReceiverCapacityState,
    pub available_slots: u32,
    pub estimated_drain_seconds: u64,
    pub objective_overrun_seconds: u64,
}

/// Assesses whether receiver concurrency can drain currently due deliveries.
///
/// Estimates use whole scheduling waves and round milliseconds up to seconds.
/// Results put saturated and constrained receivers first, with stable route ids.
///
/// # Errors
///
/// Returns validation for duplicate or blank routes, invalid endpoint identity,
/// zero capacity/timing controls, impossible active lease counts, or overflow.
pub fn assess_receiver_capacity(
    inputs: &[ReceiverCapacityInput],
) -> Result<Vec<ReceiverCapacityAssessment>, HarborError> {
    let mut routes = BTreeSet::new();
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let route_id = input.route_id.trim();
        let endpoint = input.endpoint.trim();
        if route_id.is_empty() || endpoint.is_empty() {
            return Err(HarborError::Validation(
                "receiver route and endpoint are required".into(),
            ));
        }
        if !routes.insert(route_id.to_ascii_lowercase()) {
            return Err(HarborError::Validation(format!(
                "duplicate receiver route {route_id}"
            )));
        }
        if input.max_concurrency == 0
            || input.average_attempt_millis == 0
            || input.drain_objective_seconds == 0
        {
            return Err(HarborError::Validation(format!(
                "receiver {route_id} needs positive capacity and timing controls"
            )));
        }
        if input.active_leases > input.max_concurrency {
            return Err(HarborError::Validation(format!(
                "receiver {route_id} active leases exceed capacity"
            )));
        }
        let waves = input.pending_due.div_ceil(u64::from(input.max_concurrency));
        let drain_millis = waves
            .checked_mul(input.average_attempt_millis)
            .ok_or_else(|| HarborError::Validation("receiver drain estimate overflows".into()))?;
        let estimated_drain_seconds = drain_millis.div_ceil(1_000);
        let available_slots = input.max_concurrency - input.active_leases;
        let state = if input.pending_due == 0 {
            ReceiverCapacityState::Idle
        } else if available_slots == 0 {
            ReceiverCapacityState::Saturated
        } else if estimated_drain_seconds > input.drain_objective_seconds {
            ReceiverCapacityState::Constrained
        } else {
            ReceiverCapacityState::Healthy
        };
        rows.push(ReceiverCapacityAssessment {
            route_id: route_id.to_owned(),
            endpoint: endpoint.to_owned(),
            state,
            available_slots,
            estimated_drain_seconds,
            objective_overrun_seconds: estimated_drain_seconds
                .saturating_sub(input.drain_objective_seconds),
        });
    }
    rows.sort_by(|left, right| {
        left.state
            .cmp(&right.state)
            .then_with(|| {
                right
                    .objective_overrun_seconds
                    .cmp(&left.objective_overrun_seconds)
            })
            .then_with(|| left.route_id.cmp(&right.route_id))
    });
    Ok(rows)
}
