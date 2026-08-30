use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayWindowRequest {
    pub route: String,
    pub requested_from_epoch: u64,
    pub requested_to_epoch: u64,
    pub retained_from_epoch: u64,
    pub idempotency_from_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayWindowReadiness {
    Ready,
    PayloadUnavailable,
    DeduplicationExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayWindowAssessment {
    pub route: String,
    pub effective_from_epoch: u64,
    pub effective_to_epoch: u64,
    pub readiness: ReplayWindowReadiness,
}

pub fn assess_replay_window(
    request: &ReplayWindowRequest,
) -> Result<ReplayWindowAssessment, HarborError> {
    let route = request.route.trim();
    if route.is_empty() || request.requested_from_epoch >= request.requested_to_epoch {
        return Err(HarborError::Validation(
            "replay route and positive time window are required".into(),
        ));
    }
    let readiness = if request.requested_from_epoch < request.retained_from_epoch {
        ReplayWindowReadiness::PayloadUnavailable
    } else if request.requested_from_epoch < request.idempotency_from_epoch {
        ReplayWindowReadiness::DeduplicationExpired
    } else {
        ReplayWindowReadiness::Ready
    };
    let effective_from_epoch = request
        .requested_from_epoch
        .max(request.retained_from_epoch)
        .max(request.idempotency_from_epoch);
    Ok(ReplayWindowAssessment {
        route: route.to_owned(),
        effective_from_epoch,
        effective_to_epoch: request.requested_to_epoch,
        readiness,
    })
}
