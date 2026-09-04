use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverTlsPolicy {
    pub receiver_id: String,
    pub minimum_protocol: u16,
    pub required_issuer: String,
    pub minimum_days_remaining: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverTlsProbe {
    pub receiver_id: String,
    pub protocol: u16,
    pub issuer: String,
    pub days_remaining: u64,
    pub hostname_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TlsReadiness {
    Blocked,
    RotationDue,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverTlsAssessment {
    pub receiver_id: String,
    pub readiness: TlsReadiness,
    pub blockers: Vec<String>,
}

/// Checks transport policy separately from signing credentials.
///
/// # Errors
///
/// Returns validation errors for invalid policy, duplicate probes, or unknown receivers.
pub fn assess_receiver_tls(
    policies: &[ReceiverTlsPolicy],
    probes: &[ReceiverTlsProbe],
) -> Result<Vec<ReceiverTlsAssessment>, HarborError> {
    let mut configured = BTreeMap::new();
    for policy in policies {
        let id = required(&policy.receiver_id, "TLS receiver")?;
        let issuer = required(&policy.required_issuer, "TLS issuer")?;
        if configured.contains_key(&id.to_ascii_lowercase())
            || !matches!(policy.minimum_protocol, 12 | 13)
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate TLS policy {id}"
            )));
        }
        configured.insert(
            id.to_ascii_lowercase(),
            (
                id,
                policy.minimum_protocol,
                issuer,
                policy.minimum_days_remaining,
            ),
        );
    }
    let mut seen = BTreeSet::new();
    let mut rows = Vec::with_capacity(probes.len());
    for probe in probes {
        let id = required(&probe.receiver_id, "TLS probe receiver")?;
        let key = id.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            return Err(HarborError::Validation(format!("duplicate TLS probe {id}")));
        }
        let Some((canonical, protocol, issuer, days)) = configured.get(&key) else {
            return Err(HarborError::Validation(format!(
                "receiver {id} has no TLS policy"
            )));
        };
        let mut blockers = Vec::new();
        if probe.protocol < *protocol {
            blockers.push("protocol_below_minimum".into());
        }
        if !probe.issuer.trim().eq_ignore_ascii_case(issuer) {
            blockers.push("issuer_not_permitted".into());
        }
        if !probe.hostname_verified {
            blockers.push("hostname_not_verified".into());
        }
        let readiness = if !blockers.is_empty() {
            TlsReadiness::Blocked
        } else if probe.days_remaining < *days {
            TlsReadiness::RotationDue
        } else {
            TlsReadiness::Ready
        };
        rows.push(ReceiverTlsAssessment {
            receiver_id: canonical.clone(),
            readiness,
            blockers,
        });
    }
    rows.sort_by(|a, b| {
        a.readiness
            .cmp(&b.readiness)
            .then_with(|| a.receiver_id.cmp(&b.receiver_id))
    });
    Ok(rows)
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(format!("{field} is required")))
    } else {
        Ok(value.into())
    }
}
