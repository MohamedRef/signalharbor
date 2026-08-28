use crate::HarborError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureFreshness {
    Fresh,
    Expired,
    Future,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureWindow {
    pub signed_at_epoch_seconds: i64,
    pub received_at_epoch_seconds: i64,
    pub maximum_age_seconds: u64,
    pub maximum_future_skew_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureAssessment {
    pub freshness: SignatureFreshness,
    pub age_seconds: i64,
    pub acceptable: bool,
}

/// Classifies webhook signature timestamps before payload processing.
///
/// # Errors
///
/// Returns validation when either timestamp predates the Unix epoch or the
/// configured age window is zero.
pub fn assess_signature_window(
    window: &SignatureWindow,
) -> Result<SignatureAssessment, HarborError> {
    if window.signed_at_epoch_seconds < 0 || window.received_at_epoch_seconds < 0 {
        return Err(HarborError::Validation(
            "signature timestamps cannot predate the Unix epoch".into(),
        ));
    }
    if window.maximum_age_seconds == 0 {
        return Err(HarborError::Validation(
            "signature maximum age must be positive".into(),
        ));
    }
    let age_seconds = window
        .received_at_epoch_seconds
        .checked_sub(window.signed_at_epoch_seconds)
        .ok_or_else(|| HarborError::Validation("signature age overflowed".into()))?;
    let freshness = if age_seconds < 0 {
        SignatureFreshness::Future
    } else if u64::try_from(age_seconds).unwrap_or(u64::MAX) > window.maximum_age_seconds {
        SignatureFreshness::Expired
    } else {
        SignatureFreshness::Fresh
    };
    let acceptable = match freshness {
        SignatureFreshness::Fresh => true,
        SignatureFreshness::Expired => false,
        SignatureFreshness::Future => {
            age_seconds.unsigned_abs() <= window.maximum_future_skew_seconds
        }
    };
    Ok(SignatureAssessment {
        freshness,
        age_seconds,
        acceptable,
    })
}
