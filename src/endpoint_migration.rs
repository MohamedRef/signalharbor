use std::collections::BTreeSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMigrationInput {
    pub route_id: String,
    pub current_endpoint: String,
    pub candidate_endpoint: String,
    pub candidate_traffic_percent: u8,
    pub candidate_success_basis_points: u16,
    pub minimum_success_basis_points: u16,
    pub candidate_samples: u64,
    pub minimum_samples: u64,
    pub rollback_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EndpointMigrationReadiness {
    RollbackRequired,
    Hold,
    ReadyToAdvance,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMigrationAssessment {
    pub route_id: String,
    pub readiness: EndpointMigrationReadiness,
    pub current_traffic_percent: u8,
    pub candidate_traffic_percent: u8,
    pub next_candidate_traffic_percent: u8,
    pub reason: String,
}

/// Assesses staged receiver migrations using sample and success gates.
///
/// # Errors
///
/// Rejects blank or duplicate routes, identical endpoints, invalid percentage
/// controls, impossible basis points, or zero sample requirements.
pub fn assess_endpoint_migrations(
    inputs: &[EndpointMigrationInput],
    advance_step_percent: u8,
) -> Result<Vec<EndpointMigrationAssessment>, HarborError> {
    if advance_step_percent == 0 || advance_step_percent > 100 {
        return Err(HarborError::Validation(
            "endpoint migration step must be from 1 through 100".into(),
        ));
    }
    let mut routes = BTreeSet::new();
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let route_id = input.route_id.trim();
        let current = input.current_endpoint.trim();
        let candidate = input.candidate_endpoint.trim();
        if route_id.is_empty() || current.is_empty() || candidate.is_empty() {
            return Err(HarborError::Validation(
                "migration route and endpoints are required".into(),
            ));
        }
        if !routes.insert(route_id.to_ascii_lowercase()) {
            return Err(HarborError::Validation(format!(
                "duplicate migration route {route_id}"
            )));
        }
        if current.eq_ignore_ascii_case(candidate) {
            return Err(HarborError::Validation(format!(
                "migration route {route_id} needs distinct endpoints"
            )));
        }
        if input.candidate_traffic_percent > 100
            || input.candidate_success_basis_points > 10_000
            || input.minimum_success_basis_points > 10_000
            || input.minimum_samples == 0
        {
            return Err(HarborError::Validation(format!(
                "migration route {route_id} has invalid controls"
            )));
        }
        let meets_samples = input.candidate_samples >= input.minimum_samples;
        let meets_success =
            input.candidate_success_basis_points >= input.minimum_success_basis_points;
        let (readiness, next, reason) =
            if input.candidate_traffic_percent == 100 && meets_samples && meets_success {
                (
                    EndpointMigrationReadiness::Complete,
                    100,
                    "candidate endpoint satisfies the completed migration gate",
                )
            } else if input.candidate_samples > 0 && !meets_success {
                if input.rollback_ready {
                    (
                        EndpointMigrationReadiness::RollbackRequired,
                        0,
                        "candidate success is below threshold and rollback is ready",
                    )
                } else {
                    (
                        EndpointMigrationReadiness::Hold,
                        input.candidate_traffic_percent,
                        "candidate success is below threshold but rollback is unavailable",
                    )
                }
            } else if !meets_samples {
                (
                    EndpointMigrationReadiness::Hold,
                    input.candidate_traffic_percent,
                    "candidate needs more delivery samples",
                )
            } else {
                (
                    EndpointMigrationReadiness::ReadyToAdvance,
                    input
                        .candidate_traffic_percent
                        .saturating_add(advance_step_percent)
                        .min(100),
                    "candidate satisfies the next migration gate",
                )
            };
        rows.push(EndpointMigrationAssessment {
            route_id: route_id.to_owned(),
            readiness,
            current_traffic_percent: 100 - input.candidate_traffic_percent,
            candidate_traffic_percent: input.candidate_traffic_percent,
            next_candidate_traffic_percent: next,
            reason: reason.to_owned(),
        });
    }
    rows.sort_by(|left, right| {
        left.readiness
            .cmp(&right.readiness)
            .then_with(|| left.route_id.cmp(&right.route_id))
    });
    Ok(rows)
}
