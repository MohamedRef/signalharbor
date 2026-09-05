//! Deterministic receiver clock verification readiness assessment.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReceiverClockVerificationDisposition {
    Blocked,
    Attention,
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiverClockVerificationObservation {
    pub id: String,
    pub owner: String,
    pub due_minute: u64,
    pub completed_minute: Option<u64>,
    pub required_signals: Vec<String>,
    pub observed_signals: Vec<String>,
    pub exposure_units: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiverClockVerificationFinding {
    pub id: String,
    pub owner: String,
    pub disposition: ReceiverClockVerificationDisposition,
    pub missing_signals: Vec<String>,
    pub overdue_minutes: u64,
    pub exposure_units: u64,
}

fn required_text(value: &str, field: &str) -> Result<String, String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Err(format!("receiver clock verification {field} is required"));
    }
    Ok(normalized)
}

fn normalized_signals(values: &[String], field: &str) -> Result<BTreeMap<String, String>, String> {
    let mut result = BTreeMap::new();
    for value in values {
        let normalized = required_text(value, field)?;
        let key = normalized.to_lowercase();
        if result.insert(key, normalized.clone()).is_some() {
            return Err(format!(
                "duplicate receiver clock verification {field}: {normalized}"
            ));
        }
    }
    Ok(result)
}

/// Returns receiver clock verification blockers before attention and ready observations.
///
/// # Errors
///
/// Returns an error for invalid review time, duplicate identity, malformed
/// evidence, or completion recorded after the review boundary.
pub fn assess_receiver_clock_verification(
    observations: &[ReceiverClockVerificationObservation],
    review_minute: u64,
) -> Result<Vec<ReceiverClockVerificationFinding>, String> {
    if review_minute == 0 {
        return Err("receiver clock verification review minute must be positive".to_owned());
    }
    let mut seen = BTreeSet::new();
    let mut findings = Vec::with_capacity(observations.len());
    for observation in observations {
        let id = required_text(&observation.id, "id")?;
        if !seen.insert(id.to_lowercase()) {
            return Err(format!(
                "duplicate receiver clock verification observation: {id}"
            ));
        }
        let owner = required_text(&observation.owner, "owner")?;
        if observation.due_minute == 0 {
            return Err("receiver clock verification due minute must be positive".to_owned());
        }
        if observation
            .completed_minute
            .is_some_and(|completed| completed == 0 || completed > review_minute)
        {
            return Err("receiver clock verification completion minute is invalid".to_owned());
        }

        let required = normalized_signals(&observation.required_signals, "required signal")?;
        let observed = normalized_signals(&observation.observed_signals, "observed signal")?;
        let missing_signals = required
            .into_iter()
            .filter_map(|(key, display)| (!observed.contains_key(&key)).then_some(display))
            .collect::<Vec<_>>();
        let effective = observation.completed_minute.unwrap_or(review_minute);
        let overdue_minutes = effective.saturating_sub(observation.due_minute);
        let disposition = if !missing_signals.is_empty() && observation.due_minute <= review_minute
        {
            ReceiverClockVerificationDisposition::Blocked
        } else if !missing_signals.is_empty() || overdue_minutes > 0 {
            ReceiverClockVerificationDisposition::Attention
        } else {
            ReceiverClockVerificationDisposition::Ready
        };
        findings.push(ReceiverClockVerificationFinding {
            id,
            owner,
            disposition,
            missing_signals,
            overdue_minutes,
            exposure_units: observation.exposure_units,
        });
    }

    findings.sort_by(|left, right| {
        left.disposition
            .cmp(&right.disposition)
            .then_with(|| right.exposure_units.cmp(&left.exposure_units))
            .then_with(|| right.overdue_minutes.cmp(&left.overdue_minutes))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(findings)
}
