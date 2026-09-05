//! Replay investigation retention and legal-hold decisions.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayArtifact {
    pub artifact_id: String,
    pub created_at_epoch: u64,
    pub retention_days: u32,
    pub legal_hold: bool,
    pub active_replay: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayRetentionDecision {
    pub artifact_id: String,
    pub purge_eligible: bool,
    pub expires_at_epoch: u64,
    pub reason: String,
}

/// Calculates deterministic replay artifact retention decisions.
///
/// # Errors
/// Returns an error for blank identities, zero retention, duplicates, or overflow.
pub fn assess_replay_retention(
    artifacts: &[ReplayArtifact],
    as_of_epoch: u64,
) -> Result<Vec<ReplayRetentionDecision>, String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut decisions = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let artifact_id = artifact.artifact_id.trim();
        if artifact_id.is_empty() || artifact.retention_days == 0 {
            return Err("replay artifact id and retention days are required".to_owned());
        }
        if !seen.insert(artifact_id.to_lowercase()) {
            return Err(format!("duplicate replay artifact: {artifact_id}"));
        }
        let retention_seconds = u64::from(artifact.retention_days)
            .checked_mul(86_400)
            .ok_or_else(|| "replay retention duration overflow".to_owned())?;
        let expires_at_epoch = artifact
            .created_at_epoch
            .checked_add(retention_seconds)
            .ok_or_else(|| "replay retention timestamp overflow".to_owned())?;
        let (purge_eligible, reason) = if artifact.legal_hold {
            (false, "legal_hold")
        } else if artifact.active_replay {
            (false, "active_replay")
        } else if expires_at_epoch > as_of_epoch {
            (false, "retention_active")
        } else {
            (true, "retention_elapsed")
        };
        decisions.push(ReplayRetentionDecision {
            artifact_id: artifact_id.to_owned(),
            purge_eligible,
            expires_at_epoch,
            reason: reason.to_owned(),
        });
    }
    decisions.sort_by(|left, right| {
        left.expires_at_epoch
            .cmp(&right.expires_at_epoch)
            .then_with(|| left.artifact_id.cmp(&right.artifact_id))
    });
    Ok(decisions)
}
