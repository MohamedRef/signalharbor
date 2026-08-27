use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainInput {
    pub endpoint: String,
    pub queued: u64,
    pub in_flight: u64,
    pub completions_per_minute: u64,
    pub deadline_minutes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainReadiness {
    Ready,
    OnTrack,
    AtRisk,
    Stalled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainAssessment {
    pub endpoint: String,
    pub outstanding: u64,
    pub estimated_minutes: Option<u64>,
    pub readiness: DrainReadiness,
}

/// Assesses whether an endpoint can drain safely before routing is removed.
///
/// # Errors
///
/// Returns validation for a blank endpoint, an excessive identifier, or a
/// zero-minute deadline while work remains.
pub fn assess_endpoint_drain(input: &DrainInput) -> Result<DrainAssessment, HarborError> {
    let endpoint = input.endpoint.trim();
    if endpoint.is_empty() || endpoint.len() > 200 {
        return Err(HarborError::Validation(
            "endpoint must contain 1 through 200 characters".into(),
        ));
    }
    let outstanding = input
        .queued
        .checked_add(input.in_flight)
        .ok_or_else(|| HarborError::Validation("endpoint outstanding count overflowed".into()))?;
    if outstanding == 0 {
        return Ok(DrainAssessment {
            endpoint: endpoint.into(),
            outstanding,
            estimated_minutes: Some(0),
            readiness: DrainReadiness::Ready,
        });
    }
    if input.deadline_minutes == 0 {
        return Err(HarborError::Validation(
            "drain deadline must be positive while work remains".into(),
        ));
    }
    if input.completions_per_minute == 0 {
        return Ok(DrainAssessment {
            endpoint: endpoint.into(),
            outstanding,
            estimated_minutes: None,
            readiness: DrainReadiness::Stalled,
        });
    }

    let estimated_minutes = outstanding.div_ceil(input.completions_per_minute);
    let readiness = if estimated_minutes <= input.deadline_minutes {
        DrainReadiness::OnTrack
    } else {
        DrainReadiness::AtRisk
    };
    Ok(DrainAssessment {
        endpoint: endpoint.into(),
        outstanding,
        estimated_minutes: Some(estimated_minutes),
        readiness,
    })
}
