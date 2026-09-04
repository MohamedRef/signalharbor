use crate::HarborError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySignature {
    pub delivery_id: String,
    pub signed_at_epoch: i64,
    pub delivered_at_epoch: Option<i64>,
    pub maximum_age_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureFreshnessAssessment {
    pub delivery_id: String,
    pub age_seconds: u64,
    pub state: String,
}

/// Detects webhook signatures that became stale before delivery.
///
/// # Errors
/// Rejects duplicate deliveries, invalid horizons, and impossible timestamps.
pub fn assess_delivery_signature_freshness(
    signatures: &[DeliverySignature],
    now_epoch: i64,
) -> Result<Vec<SignatureFreshnessAssessment>, HarborError> {
    let mut seen = BTreeSet::new();
    let mut assessments = Vec::with_capacity(signatures.len());
    for signature in signatures {
        let delivery = signature.delivery_id.trim();
        if delivery.is_empty()
            || signature.maximum_age_seconds == 0
            || !seen.insert(delivery.to_ascii_lowercase())
        {
            return Err(HarborError::Validation("invalid delivery signature".into()));
        }
        let evaluated_at = signature.delivered_at_epoch.unwrap_or(now_epoch);
        if signature.signed_at_epoch > evaluated_at || evaluated_at > now_epoch {
            return Err(HarborError::Validation(
                "invalid signature chronology".into(),
            ));
        }
        let age = evaluated_at.abs_diff(signature.signed_at_epoch);
        let state = if signature.delivered_at_epoch.is_none() {
            if age > signature.maximum_age_seconds {
                "pending_stale"
            } else {
                "pending_fresh"
            }
        } else if age > signature.maximum_age_seconds {
            "delivered_stale"
        } else {
            "delivered_fresh"
        };
        assessments.push(SignatureFreshnessAssessment {
            delivery_id: delivery.into(),
            age_seconds: age,
            state: state.into(),
        });
    }
    assessments.sort_by(|a, b| {
        rank(&a.state)
            .cmp(&rank(&b.state))
            .then(b.age_seconds.cmp(&a.age_seconds))
            .then(a.delivery_id.cmp(&b.delivery_id))
    });
    Ok(assessments)
}

fn rank(state: &str) -> u8 {
    match state {
        "pending_stale" => 0,
        "delivered_stale" => 1,
        "pending_fresh" => 2,
        _ => 3,
    }
}
