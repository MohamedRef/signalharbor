use crate::HarborError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedriveReadiness {
    Available,
    Constrained,
    Saturated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedriveCapacity {
    pub endpoint: String,
    pub capacity_per_minute: u64,
    pub live_traffic_per_minute: u64,
    pub requested_redrive_per_minute: u64,
    pub reserve_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedriveThrottle {
    pub endpoint: String,
    pub reserved_per_minute: u64,
    pub available_per_minute: u64,
    pub approved_redrive_per_minute: u64,
    pub readiness: RedriveReadiness,
}

/// Derives a redrive rate that cannot consume endpoint safety reserve.
///
/// # Errors
///
/// Returns validation for blank endpoints, zero capacity, impossible live
/// traffic, an invalid reserve, or arithmetic overflow.
pub fn calculate_redrive_throttle(input: &RedriveCapacity) -> Result<RedriveThrottle, HarborError> {
    let endpoint = input.endpoint.trim();
    if endpoint.is_empty() || endpoint.len() > 200 {
        return Err(HarborError::Validation(
            "endpoint must contain 1 through 200 characters".into(),
        ));
    }
    if input.capacity_per_minute == 0 || input.reserve_percent > 100 {
        return Err(HarborError::Validation(
            "capacity must be positive and reserve cannot exceed 100 percent".into(),
        ));
    }
    if input.live_traffic_per_minute > input.capacity_per_minute {
        return Err(HarborError::Validation(
            "live traffic cannot exceed endpoint capacity".into(),
        ));
    }
    let reserve_numerator = input
        .capacity_per_minute
        .checked_mul(u64::from(input.reserve_percent))
        .ok_or_else(|| HarborError::Validation("redrive reserve overflowed".into()))?;
    let reserved_per_minute = reserve_numerator.div_ceil(100);
    let available_per_minute = input
        .capacity_per_minute
        .saturating_sub(input.live_traffic_per_minute)
        .saturating_sub(reserved_per_minute);
    let approved_redrive_per_minute = available_per_minute.min(input.requested_redrive_per_minute);
    let readiness = if available_per_minute == 0 {
        RedriveReadiness::Saturated
    } else if approved_redrive_per_minute < input.requested_redrive_per_minute {
        RedriveReadiness::Constrained
    } else {
        RedriveReadiness::Available
    };
    Ok(RedriveThrottle {
        endpoint: endpoint.into(),
        reserved_per_minute,
        available_per_minute,
        approved_redrive_per_minute,
        readiness,
    })
}
