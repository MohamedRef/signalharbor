use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockSkewInput {
    pub event_id: String,
    pub signed_at_epoch: u64,
    pub received_at_epoch: u64,
    pub maximum_past_skew_seconds: u64,
    pub maximum_future_skew_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSkewDecision {
    Accept,
    Stale,
    FutureDated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockSkewAssessment {
    pub event_id: String,
    pub absolute_skew_seconds: u64,
    pub decision: ClockSkewDecision,
}

/// Assesses a producer timestamp against route-specific skew tolerances.
///
/// # Errors
///
/// Returns validation when the event identity is blank or either tolerance is zero.
pub fn assess_clock_skew(input: &ClockSkewInput) -> Result<ClockSkewAssessment, HarborError> {
    let event_id = input.event_id.trim();
    if event_id.is_empty()
        || input.maximum_past_skew_seconds == 0
        || input.maximum_future_skew_seconds == 0
    {
        return Err(HarborError::Validation(
            "event identity and positive clock-skew tolerances are required".into(),
        ));
    }
    let (absolute_skew_seconds, decision) = if input.signed_at_epoch > input.received_at_epoch {
        let skew = input.signed_at_epoch - input.received_at_epoch;
        let decision = if skew > input.maximum_future_skew_seconds {
            ClockSkewDecision::FutureDated
        } else {
            ClockSkewDecision::Accept
        };
        (skew, decision)
    } else {
        let skew = input.received_at_epoch - input.signed_at_epoch;
        let decision = if skew > input.maximum_past_skew_seconds {
            ClockSkewDecision::Stale
        } else {
            ClockSkewDecision::Accept
        };
        (skew, decision)
    };
    Ok(ClockSkewAssessment {
        event_id: event_id.to_owned(),
        absolute_skew_seconds,
        decision,
    })
}
