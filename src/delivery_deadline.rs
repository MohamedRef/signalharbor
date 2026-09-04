use std::collections::BTreeSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryDeadlineInput {
    pub tenant: String,
    pub delivery_id: String,
    pub next_attempt_at: u64,
    pub expires_at: u64,
    pub attempts_remaining: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryUrgency {
    Expired,
    Due,
    AtRisk,
    OnTrack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryDeadlineAssessment {
    pub tenant: String,
    pub delivery_id: String,
    pub urgency: DeliveryUrgency,
    pub seconds_until_attempt: u64,
    pub retry_window_seconds: u64,
    pub attempts_remaining: u16,
}

/// Prioritizes pending deliveries by expiry exposure without changing queue state.
///
/// A delivery is at risk when the time between its next attempt and expiry is no
/// larger than `risk_window_seconds`, or when it has no attempts remaining.
/// Results are stable by urgency, retry window, tenant, then delivery identity.
///
/// # Errors
///
/// Returns validation for blank identities, duplicate tenant/delivery pairs,
/// zero risk windows, or an expiry earlier than the planned next attempt.
pub fn assess_delivery_deadlines(
    inputs: &[DeliveryDeadlineInput],
    observed_at: u64,
    risk_window_seconds: u64,
) -> Result<Vec<DeliveryDeadlineAssessment>, HarborError> {
    if risk_window_seconds == 0 {
        return Err(HarborError::Validation(
            "deadline risk window must be positive".into(),
        ));
    }
    let mut identities = BTreeSet::new();
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let tenant = input.tenant.trim();
        let delivery_id = input.delivery_id.trim();
        if tenant.is_empty() || delivery_id.is_empty() {
            return Err(HarborError::Validation(
                "deadline tenant and delivery are required".into(),
            ));
        }
        let identity = (tenant.to_ascii_lowercase(), delivery_id.to_owned());
        if !identities.insert(identity) {
            return Err(HarborError::Validation(format!(
                "duplicate deadline delivery {tenant}/{delivery_id}"
            )));
        }
        if input.expires_at < input.next_attempt_at {
            return Err(HarborError::Validation(format!(
                "delivery {delivery_id} expires before its next attempt"
            )));
        }
        let retry_window_seconds = input.expires_at - input.next_attempt_at;
        let urgency = if input.expires_at <= observed_at {
            DeliveryUrgency::Expired
        } else if input.next_attempt_at <= observed_at {
            DeliveryUrgency::Due
        } else if retry_window_seconds <= risk_window_seconds || input.attempts_remaining == 0 {
            DeliveryUrgency::AtRisk
        } else {
            DeliveryUrgency::OnTrack
        };
        rows.push(DeliveryDeadlineAssessment {
            tenant: tenant.to_owned(),
            delivery_id: delivery_id.to_owned(),
            urgency,
            seconds_until_attempt: input.next_attempt_at.saturating_sub(observed_at),
            retry_window_seconds,
            attempts_remaining: input.attempts_remaining,
        });
    }
    rows.sort_by(|left, right| {
        left.urgency
            .cmp(&right.urgency)
            .then_with(|| left.retry_window_seconds.cmp(&right.retry_window_seconds))
            .then_with(|| left.tenant.cmp(&right.tenant))
            .then_with(|| left.delivery_id.cmp(&right.delivery_id))
    });
    Ok(rows)
}
