use std::collections::BTreeSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryObservation {
    pub delivery_id: String,
    pub route_id: String,
    pub completed_at: u64,
    pub latency_ms: u64,
    pub successful: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliverySloObjective {
    pub minimum_samples: usize,
    pub minimum_success_basis_points: u16,
    pub maximum_p95_latency_ms: u64,
    pub maximum_consecutive_failures: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySloReport {
    pub route_id: String,
    pub samples: usize,
    pub successes: usize,
    pub failures: usize,
    pub success_basis_points: u16,
    pub p95_latency_ms: u64,
    pub longest_failure_streak: usize,
    pub enough_samples: bool,
    pub breached_reasons: Vec<String>,
    pub breached: bool,
}

/// Evaluates a route's completed-delivery sample against a deterministic SLO.
///
/// # Errors
///
/// Returns validation for an invalid objective, malformed observations, duplicate
/// delivery ids, or samples from more than one route.
pub fn evaluate_delivery_slo(
    observations: &[DeliveryObservation],
    objective: DeliverySloObjective,
) -> Result<DeliverySloReport, HarborError> {
    validate_objective(objective)?;
    if observations.is_empty() {
        return Err(HarborError::Validation(
            "at least one delivery observation is required".into(),
        ));
    }
    let mut ordered = observations.to_vec();
    ordered.sort_by(|left, right| {
        left.completed_at
            .cmp(&right.completed_at)
            .then_with(|| left.delivery_id.cmp(&right.delivery_id))
    });
    let route_id = required(&ordered[0].route_id, "route id")?;
    let mut identities = BTreeSet::new();
    let mut successes = 0_usize;
    let mut current_streak = 0_usize;
    let mut longest_streak = 0_usize;
    let mut latencies = Vec::with_capacity(ordered.len());
    for observation in &ordered {
        let delivery_id = required(&observation.delivery_id, "delivery id")?;
        if required(&observation.route_id, "route id")? != route_id {
            return Err(HarborError::Validation(
                "SLO observations must belong to one route".into(),
            ));
        }
        if !identities.insert(delivery_id) {
            return Err(HarborError::Conflict(
                "duplicate delivery observation".into(),
            ));
        }
        if observation.latency_ms == 0 {
            return Err(HarborError::Validation(
                "delivery latency must be positive".into(),
            ));
        }
        latencies.push(observation.latency_ms);
        if observation.successful {
            successes += 1;
            current_streak = 0;
        } else {
            current_streak += 1;
            longest_streak = longest_streak.max(current_streak);
        }
    }
    latencies.sort_unstable();
    let samples = ordered.len();
    let success_basis_points = u16::try_from(successes * 10_000 / samples)
        .map_err(|_| HarborError::Validation("success rate overflow".into()))?;
    let percentile_index = (samples * 95).div_ceil(100).saturating_sub(1);
    let p95_latency_ms = latencies[percentile_index];
    let enough_samples = samples >= objective.minimum_samples;
    let mut breached_reasons = Vec::new();
    if enough_samples && success_basis_points < objective.minimum_success_basis_points {
        breached_reasons.push("success_rate".into());
    }
    if enough_samples && p95_latency_ms > objective.maximum_p95_latency_ms {
        breached_reasons.push("p95_latency".into());
    }
    if longest_streak > objective.maximum_consecutive_failures {
        breached_reasons.push("failure_streak".into());
    }
    Ok(DeliverySloReport {
        route_id,
        samples,
        successes,
        failures: samples - successes,
        success_basis_points,
        p95_latency_ms,
        longest_failure_streak: longest_streak,
        enough_samples,
        breached: !breached_reasons.is_empty(),
        breached_reasons,
    })
}

fn validate_objective(objective: DeliverySloObjective) -> Result<(), HarborError> {
    if objective.minimum_samples == 0
        || objective.minimum_success_basis_points > 10_000
        || objective.maximum_p95_latency_ms == 0
        || objective.maximum_consecutive_failures == 0
    {
        return Err(HarborError::Validation(
            "invalid delivery SLO objective".into(),
        ));
    }
    Ok(())
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Err(HarborError::Validation(format!("{field} is required")));
    }
    Ok(normalized)
}
