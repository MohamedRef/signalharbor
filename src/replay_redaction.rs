//! Header redaction for replay investigation exports.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayHeaderSet {
    pub event_id: String,
    pub headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedactedReplayHeaders {
    pub event_id: String,
    pub headers: BTreeMap<String, String>,
    pub redacted_names: Vec<String>,
}

/// Redacts configured headers while preserving names for investigation context.
///
/// # Errors
/// Returns an error for blank identities, names, or an empty policy.
pub fn redact_replay_headers(
    input: &ReplayHeaderSet,
    sensitive_names: &[String],
) -> Result<RedactedReplayHeaders, String> {
    let event_id = input.event_id.trim();
    if event_id.is_empty() {
        return Err("replay header event id is required".to_owned());
    }
    let sensitive = sensitive_names
        .iter()
        .map(|name| name.trim().to_lowercase())
        .collect::<BTreeSet<_>>();
    if sensitive.is_empty() || sensitive.contains("") {
        return Err("replay header redaction policy is required".to_owned());
    }
    let mut headers = BTreeMap::new();
    let mut redacted_names = Vec::new();
    for (name, value) in &input.headers {
        let normalized_name = name.trim().to_lowercase();
        if normalized_name.is_empty() {
            return Err("replay header name is required".to_owned());
        }
        if headers.contains_key(&normalized_name) {
            return Err(format!("duplicate replay header: {normalized_name}"));
        }
        if sensitive.contains(&normalized_name) {
            headers.insert(normalized_name.clone(), "[redacted]".to_owned());
            redacted_names.push(normalized_name);
        } else {
            headers.insert(normalized_name, value.trim().to_owned());
        }
    }
    Ok(RedactedReplayHeaders {
        event_id: event_id.to_owned(),
        headers,
        redacted_names,
    })
}
