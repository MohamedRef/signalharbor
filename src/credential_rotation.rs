use std::collections::HashSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointCredential {
    pub endpoint: String,
    pub expires_at_epoch: u64,
    pub replacement_staged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReadiness {
    Ready,
    ReplacementMissing,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRotation {
    pub endpoint: String,
    pub expires_at_epoch: u64,
    pub readiness: RotationReadiness,
}

/// Plans endpoint credential rotations inside a look-ahead window.
///
/// # Errors
///
/// Returns validation for blank or duplicate endpoints or a zero horizon.
pub fn plan_credential_rotations(
    rows: &[EndpointCredential],
    as_of_epoch: u64,
    horizon_seconds: u64,
) -> Result<Vec<CredentialRotation>, HarborError> {
    if horizon_seconds == 0 {
        return Err(HarborError::Validation(
            "credential rotation horizon must be positive".into(),
        ));
    }
    let horizon = as_of_epoch.saturating_add(horizon_seconds);
    let mut seen = HashSet::new();
    let mut due = Vec::new();
    for row in rows {
        let endpoint = row.endpoint.trim();
        if endpoint.is_empty() || !seen.insert(endpoint.to_owned()) {
            return Err(HarborError::Validation(
                "credential endpoints must be unique and non-empty".into(),
            ));
        }
        if row.expires_at_epoch <= horizon {
            let readiness = if row.expires_at_epoch < as_of_epoch {
                RotationReadiness::Expired
            } else if row.replacement_staged {
                RotationReadiness::Ready
            } else {
                RotationReadiness::ReplacementMissing
            };
            due.push(CredentialRotation {
                endpoint: endpoint.to_owned(),
                expires_at_epoch: row.expires_at_epoch,
                readiness,
            });
        }
    }
    due.sort_by(|a, b| {
        a.expires_at_epoch
            .cmp(&b.expires_at_epoch)
            .then_with(|| a.endpoint.cmp(&b.endpoint))
    });
    Ok(due)
}
