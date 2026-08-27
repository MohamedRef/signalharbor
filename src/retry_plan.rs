use crate::HarborError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u16,
    pub base_delay_seconds: u64,
    pub max_delay_seconds: u64,
    pub max_elapsed_seconds: u64,
    pub jitter_basis_points: u16,
    pub jitter_seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySlot {
    pub attempt: u16,
    pub delay_seconds: u64,
    pub due_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPlan {
    pub started_at: u64,
    pub slots: Vec<RetrySlot>,
    pub exhausted_by_elapsed_limit: bool,
}

/// Builds a deterministic capped exponential retry schedule with bounded jitter.
///
/// # Errors
///
/// Returns validation for unsupported policy bounds and overflow-prone timestamps.
pub fn plan_retries(started_at: u64, policy: RetryPolicy) -> Result<RetryPlan, HarborError> {
    validate(policy)?;
    let mut slots = Vec::with_capacity(usize::from(policy.max_attempts));
    let mut due_at = started_at;
    let deadline = started_at
        .checked_add(policy.max_elapsed_seconds)
        .ok_or_else(|| HarborError::Validation("retry deadline overflows timestamp".into()))?;
    let mut exhausted = false;
    for attempt in 1..=policy.max_attempts {
        let exponent = u32::from(attempt.saturating_sub(1)).min(63);
        let uncapped = policy.base_delay_seconds.saturating_mul(1_u64 << exponent);
        let delay = uncapped.min(policy.max_delay_seconds);
        let adjusted = jittered(
            delay,
            policy.jitter_basis_points,
            policy.jitter_seed,
            attempt,
        );
        let candidate = due_at
            .checked_add(adjusted)
            .ok_or_else(|| HarborError::Validation("retry due time overflows timestamp".into()))?;
        if candidate > deadline {
            exhausted = true;
            break;
        }
        due_at = candidate;
        slots.push(RetrySlot {
            attempt,
            delay_seconds: adjusted,
            due_at,
        });
    }
    Ok(RetryPlan {
        started_at,
        slots,
        exhausted_by_elapsed_limit: exhausted,
    })
}

fn jittered(delay: u64, basis_points: u16, seed: u64, attempt: u16) -> u64 {
    if basis_points == 0 {
        return delay;
    }
    let spread = delay.saturating_mul(u64::from(basis_points)) / 10_000;
    if spread == 0 {
        return delay;
    }
    let random = mix(seed ^ u64::from(attempt));
    let width = spread.saturating_mul(2).saturating_add(1);
    let offset = random % width;
    delay.saturating_sub(spread).saturating_add(offset)
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate(policy: RetryPolicy) -> Result<(), HarborError> {
    if !(1..=20).contains(&policy.max_attempts) {
        return Err(HarborError::Validation(
            "max_attempts must be from 1 through 20".into(),
        ));
    }
    if policy.base_delay_seconds == 0 || policy.max_delay_seconds < policy.base_delay_seconds {
        return Err(HarborError::Validation(
            "retry delay cap must be at least the positive base delay".into(),
        ));
    }
    if policy.max_elapsed_seconds < policy.base_delay_seconds {
        return Err(HarborError::Validation(
            "max elapsed time must allow the base delay".into(),
        ));
    }
    if policy.jitter_basis_points > 5_000 {
        return Err(HarborError::Validation(
            "jitter must not exceed 5000 basis points".into(),
        ));
    }
    Ok(())
}
