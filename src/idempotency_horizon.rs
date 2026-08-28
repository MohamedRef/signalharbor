use crate::HarborError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HorizonReadiness {
    Sufficient,
    Insufficient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyHorizon {
    pub retention_seconds: u64,
    pub maximum_delivery_lifetime_seconds: u64,
    pub maximum_redrive_delay_seconds: u64,
    pub clock_skew_allowance_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HorizonAssessment {
    pub required_seconds: u64,
    pub retention_seconds: u64,
    pub shortfall_seconds: u64,
    pub readiness: HorizonReadiness,
}

/// Confirms that deduplication records outlive every supported delivery path.
///
/// # Errors
///
/// Returns validation when retention or delivery lifetime is zero, or when the
/// combined required horizon overflows.
pub fn assess_idempotency_horizon(
    horizon: &IdempotencyHorizon,
) -> Result<HorizonAssessment, HarborError> {
    if horizon.retention_seconds == 0 || horizon.maximum_delivery_lifetime_seconds == 0 {
        return Err(HarborError::Validation(
            "retention and delivery lifetime must be positive".into(),
        ));
    }
    let required_seconds = horizon
        .maximum_delivery_lifetime_seconds
        .checked_add(horizon.maximum_redrive_delay_seconds)
        .and_then(|value| value.checked_add(horizon.clock_skew_allowance_seconds))
        .ok_or_else(|| HarborError::Validation("required idempotency horizon overflowed".into()))?;
    let shortfall_seconds = required_seconds.saturating_sub(horizon.retention_seconds);
    Ok(HorizonAssessment {
        required_seconds,
        retention_seconds: horizon.retention_seconds,
        shortfall_seconds,
        readiness: if shortfall_seconds == 0 {
            HorizonReadiness::Sufficient
        } else {
            HorizonReadiness::Insufficient
        },
    })
}
