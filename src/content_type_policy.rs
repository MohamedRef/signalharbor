use crate::HarborError;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentTypePolicy {
    pub route_id: String,
    pub allowed_media_types: Vec<String>,
    pub require_utf8: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookContentObservation {
    pub event_id: String,
    pub route_id: String,
    pub media_type: String,
    pub charset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentTypeAssessment {
    pub event_id: String,
    pub route_id: String,
    pub state: String,
}

/// Checks webhook media types before receiver routing.
///
/// # Errors
/// Rejects duplicate policies or observations and observations for unknown routes.
pub fn assess_content_types(
    policies: &[ContentTypePolicy],
    observations: &[WebhookContentObservation],
) -> Result<Vec<ContentTypeAssessment>, HarborError> {
    let mut configured = BTreeMap::new();
    for policy in policies {
        let route = policy.route_id.trim();
        let allowed: BTreeSet<_> = policy
            .allowed_media_types
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect();
        if route.is_empty()
            || allowed.is_empty()
            || allowed.iter().any(String::is_empty)
            || allowed.len() != policy.allowed_media_types.len()
            || configured
                .insert(
                    route.to_ascii_lowercase(),
                    (route, allowed, policy.require_utf8),
                )
                .is_some()
        {
            return Err(HarborError::Validation(
                "invalid content type policy".into(),
            ));
        }
    }
    let mut seen = BTreeSet::new();
    let mut assessments = Vec::new();
    for observation in observations {
        let event = observation.event_id.trim();
        let route_key = observation.route_id.trim().to_ascii_lowercase();
        let media_type = observation.media_type.trim().to_ascii_lowercase();
        if event.is_empty() || media_type.is_empty() || !seen.insert(event.to_ascii_lowercase()) {
            return Err(HarborError::Validation(
                "invalid content observation".into(),
            ));
        }
        let Some((route, allowed, require_utf8)) = configured.get(&route_key) else {
            return Err(HarborError::Validation(
                "content observation route is unknown".into(),
            ));
        };
        let charset = observation.charset.as_deref().map(str::trim);
        let state = if !allowed.contains(&media_type) {
            "unsupported_media_type"
        } else if *require_utf8 && !charset.is_some_and(|value| value.eq_ignore_ascii_case("utf-8"))
        {
            "charset_rejected"
        } else {
            "accepted"
        };
        assessments.push(ContentTypeAssessment {
            event_id: event.into(),
            route_id: (*route).into(),
            state: state.into(),
        });
    }
    assessments.sort_by(|a, b| {
        rank(&a.state)
            .cmp(&rank(&b.state))
            .then(a.route_id.cmp(&b.route_id))
            .then(a.event_id.cmp(&b.event_id))
    });
    Ok(assessments)
}

fn rank(state: &str) -> u8 {
    match state {
        "unsupported_media_type" => 0,
        "charset_rejected" => 1,
        _ => 2,
    }
}
