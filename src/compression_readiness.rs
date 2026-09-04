use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionPolicy {
    pub route_id: String,
    pub supported_encodings: Vec<String>,
    pub minimum_payload_bytes: u64,
    pub minimum_savings_basis_points: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionSample {
    pub route_id: String,
    pub event_id: String,
    pub encoding: String,
    pub original_bytes: u64,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompressionReadiness {
    InvalidExpansion,
    InsufficientSavings,
    Ready,
    BelowThreshold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionAssessment {
    pub route_id: String,
    pub event_id: String,
    pub encoding: String,
    pub readiness: CompressionReadiness,
    pub savings_basis_points: u16,
}

type CompressionPolicyIndex = BTreeMap<String, (String, BTreeSet<String>, u64, u16)>;

fn index_compression_policies(
    policies: &[CompressionPolicy],
) -> Result<CompressionPolicyIndex, HarborError> {
    let mut routes = BTreeMap::new();
    for policy in policies {
        let route_id = policy.route_id.trim();
        if route_id.is_empty() {
            return Err(HarborError::Validation(
                "compression route is required".into(),
            ));
        }
        let key = route_id.to_ascii_lowercase();
        if routes.contains_key(&key) {
            return Err(HarborError::Validation(format!(
                "duplicate compression route {route_id}"
            )));
        }
        if policy.minimum_payload_bytes == 0 || policy.minimum_savings_basis_points > 10_000 {
            return Err(HarborError::Validation(format!(
                "compression route {route_id} has invalid controls"
            )));
        }
        let encodings = policy
            .supported_encodings
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if encodings.is_empty()
            || encodings.contains("")
            || encodings.len() != policy.supported_encodings.len()
        {
            return Err(HarborError::Validation(format!(
                "compression route {route_id} needs unique encodings"
            )));
        }
        routes.insert(
            key,
            (
                route_id.to_owned(),
                encodings,
                policy.minimum_payload_bytes,
                policy.minimum_savings_basis_points,
            ),
        );
    }
    Ok(routes)
}

/// Checks whether negotiated webhook compression is safe and worthwhile.
///
/// # Errors
///
/// Rejects duplicate identities, unsupported encodings, zero sizes, or policy
/// controls outside their valid ranges.
pub fn assess_compression_readiness(
    policies: &[CompressionPolicy],
    samples: &[CompressionSample],
) -> Result<Vec<CompressionAssessment>, HarborError> {
    let routes = index_compression_policies(policies)?;
    let mut identities = BTreeSet::new();
    let mut rows = Vec::with_capacity(samples.len());
    for sample in samples {
        let route = sample.route_id.trim();
        let event_id = sample.event_id.trim();
        let encoding = sample.encoding.trim().to_ascii_lowercase();
        if route.is_empty()
            || event_id.is_empty()
            || encoding.is_empty()
            || sample.original_bytes == 0
            || sample.encoded_bytes == 0
        {
            return Err(HarborError::Validation(
                "compression sample identity and sizes are required".into(),
            ));
        }
        let route_key = route.to_ascii_lowercase();
        let Some((route_id, encodings, minimum_bytes, minimum_savings)) = routes.get(&route_key)
        else {
            return Err(HarborError::Validation(format!(
                "compression route {route} has no policy"
            )));
        };
        if !encodings.contains(&encoding) {
            return Err(HarborError::Validation(format!(
                "encoding {encoding} is unsupported for {route}"
            )));
        }
        if !identities.insert((route_key, event_id.to_owned(), encoding.clone())) {
            return Err(HarborError::Validation(format!(
                "duplicate compression sample {route}/{event_id}/{encoding}"
            )));
        }
        let saved = sample.original_bytes.saturating_sub(sample.encoded_bytes);
        let savings = saved
            .checked_mul(10_000)
            .ok_or_else(|| HarborError::Validation("compression savings overflow".into()))?
            / sample.original_bytes;
        let savings_basis_points = u16::try_from(savings).map_err(|_| {
            HarborError::Validation("compression savings cannot be represented".into())
        })?;
        let readiness = if sample.encoded_bytes > sample.original_bytes {
            CompressionReadiness::InvalidExpansion
        } else if sample.original_bytes < *minimum_bytes {
            CompressionReadiness::BelowThreshold
        } else if savings_basis_points < *minimum_savings {
            CompressionReadiness::InsufficientSavings
        } else {
            CompressionReadiness::Ready
        };
        rows.push(CompressionAssessment {
            route_id: route_id.clone(),
            event_id: event_id.to_owned(),
            encoding,
            readiness,
            savings_basis_points,
        });
    }
    rows.sort_by(|left, right| {
        left.readiness
            .cmp(&right.readiness)
            .then_with(|| left.route_id.cmp(&right.route_id))
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    Ok(rows)
}
