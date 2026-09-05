use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryCeilingPolicy {
    pub route_id: String,
    pub base_delay_seconds: u64,
    pub multiplier: u32,
    pub maximum_delay_seconds: u64,
    pub maximum_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryCeilingStep {
    pub route_id: String,
    pub attempt: u32,
    pub delay_seconds: u64,
    pub capped: bool,
}

/// Builds overflow-safe exponential retry delays capped by route policy.
///
/// # Errors
/// Returns validation errors for empty routes or unusable retry limits.
pub fn build_retry_ceiling(
    policy: &RetryCeilingPolicy,
) -> Result<Vec<RetryCeilingStep>, HarborError> {
    let route = policy.route_id.trim();
    if route.is_empty()
        || policy.base_delay_seconds == 0
        || policy.multiplier < 2
        || policy.maximum_delay_seconds < policy.base_delay_seconds
        || policy.maximum_attempts == 0
        || policy.maximum_attempts > 10_000
    {
        return Err(HarborError::Validation(
            "invalid retry ceiling policy; attempts must be from 1 through 10000".into(),
        ));
    }
    let mut delay = policy.base_delay_seconds;
    let mut steps = Vec::with_capacity(policy.maximum_attempts as usize);
    for attempt in 1..=policy.maximum_attempts {
        let capped = delay >= policy.maximum_delay_seconds;
        steps.push(RetryCeilingStep {
            route_id: route.into(),
            attempt,
            delay_seconds: delay.min(policy.maximum_delay_seconds),
            capped,
        });
        delay = delay
            .saturating_mul(u64::from(policy.multiplier))
            .min(policy.maximum_delay_seconds);
    }
    Ok(steps)
}
