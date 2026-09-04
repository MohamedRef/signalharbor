use crate::HarborError;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaObservation {
    pub route_id: String,
    pub event_type: String,
    pub observed_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSchemaPolicy {
    pub route_id: String,
    pub event_type: String,
    pub required_fields: Vec<String>,
    pub allowed_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDriftFinding {
    pub route_id: String,
    pub event_type: String,
    pub missing_fields: Vec<String>,
    pub unexpected_fields: Vec<String>,
}

/// Detects payload-field drift without inspecting payload values.
///
/// # Errors
/// Returns validation errors for duplicate policies, observations, or malformed field names.
pub fn detect_schema_drift(
    policies: &[RouteSchemaPolicy],
    observations: &[SchemaObservation],
) -> Result<Vec<SchemaDriftFinding>, HarborError> {
    let mut configured = BTreeMap::new();
    for policy in policies {
        let route = required(&policy.route_id, "schema route")?;
        let event = required(&policy.event_type, "schema event type")?;
        let required_fields = fields(&policy.required_fields)?;
        let mut allowed = fields(&policy.allowed_fields)?;
        allowed.extend(required_fields.iter().cloned());
        if configured
            .insert(
                (route.to_lowercase(), event.to_lowercase()),
                (route, event, required_fields, allowed),
            )
            .is_some()
        {
            return Err(HarborError::Validation(
                "duplicate route schema policy".into(),
            ));
        }
    }
    let mut seen = BTreeSet::new();
    let mut findings = Vec::new();
    for observation in observations {
        let route = required(&observation.route_id, "schema observation route")?;
        let event = required(&observation.event_type, "schema observation event")?;
        let key = (route.to_lowercase(), event.to_lowercase());
        if !seen.insert(key.clone()) {
            return Err(HarborError::Validation(
                "duplicate route schema observation".into(),
            ));
        }
        let observed = fields(&observation.observed_fields)?;
        let Some((canonical_route, canonical_event, required_set, allowed)) = configured.get(&key)
        else {
            return Err(HarborError::Validation(format!(
                "missing schema policy for {route}/{event}"
            )));
        };
        let missing_fields = required_set.difference(&observed).cloned().collect();
        let unexpected_fields = observed.difference(allowed).cloned().collect();
        findings.push(SchemaDriftFinding {
            route_id: canonical_route.clone(),
            event_type: canonical_event.clone(),
            missing_fields,
            unexpected_fields,
        });
    }
    findings.sort_by(|a, b| {
        a.route_id
            .cmp(&b.route_id)
            .then(a.event_type.cmp(&b.event_type))
    });
    Ok(findings)
}

fn fields(values: &[String]) -> Result<BTreeSet<String>, HarborError> {
    values
        .iter()
        .map(|value| required(value, "schema field").map(|field| field.to_lowercase()))
        .collect()
}
fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(format!("{field} is required")))
    } else {
        Ok(value.into())
    }
}
