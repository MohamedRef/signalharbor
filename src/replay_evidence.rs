//! Replay evidence coverage and integrity assessment.

use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEvidence {
    pub evidence_id: String,
    pub event_id: String,
    pub payload_sha256: String,
    pub captured_at_epoch: u64,
    pub quarantined: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEvidenceReport {
    pub complete: bool,
    pub covered_events: usize,
    pub missing_event_ids: Vec<String>,
    pub quarantined_event_ids: Vec<String>,
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Confirms that every requested event has one intact replay evidence record.
///
/// # Errors
/// Returns an error for blank, duplicate, malformed, or future evidence.
pub fn assess_replay_evidence(
    event_ids: &[String],
    evidence: &[ReplayEvidence],
    as_of_epoch: u64,
) -> Result<ReplayEvidenceReport, String> {
    let requested = event_ids
        .iter()
        .map(|event_id| event_id.trim().to_lowercase())
        .collect::<BTreeSet<_>>();
    if requested.is_empty() || requested.contains("") {
        return Err("replay evidence requires requested events".to_owned());
    }
    let mut evidence_ids = BTreeSet::new();
    let mut covered = BTreeSet::new();
    let mut quarantined = Vec::new();
    for item in evidence {
        let evidence_id = item.evidence_id.trim();
        let event_id = item.event_id.trim().to_lowercase();
        if evidence_id.is_empty() || event_id.is_empty() {
            return Err("replay evidence identities are required".to_owned());
        }
        if !evidence_ids.insert(evidence_id.to_lowercase()) {
            return Err(format!("duplicate replay evidence: {evidence_id}"));
        }
        if !valid_sha256(item.payload_sha256.trim()) {
            return Err(format!("replay evidence {evidence_id} has invalid sha256"));
        }
        if item.captured_at_epoch > as_of_epoch {
            return Err(format!("replay evidence {evidence_id} is from the future"));
        }
        if !requested.contains(&event_id) {
            continue;
        }
        if !covered.insert(event_id.clone()) {
            return Err(format!(
                "event has multiple replay evidence records: {event_id}"
            ));
        }
        if item.quarantined {
            quarantined.push(event_id);
        }
    }
    let missing_event_ids = requested.difference(&covered).cloned().collect::<Vec<_>>();
    quarantined.sort();
    Ok(ReplayEvidenceReport {
        complete: missing_event_ids.is_empty() && quarantined.is_empty(),
        covered_events: covered.len(),
        missing_event_ids,
        quarantined_event_ids: quarantined,
    })
}
