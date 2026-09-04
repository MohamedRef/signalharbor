use std::collections::BTreeMap;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupeObservation {
    pub tenant: String,
    pub key: String,
    pub payload_digest: String,
    pub received_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupeKeyAudit {
    pub tenant: String,
    pub key: String,
    pub canonical_payload_digest: String,
    pub observations: u64,
    pub exact_replays: u64,
    pub conflicting_payloads: u64,
    pub first_received_at: u64,
    pub last_received_at: u64,
    pub healthy: bool,
}

#[derive(Debug)]
struct MutableAudit {
    row: DedupeKeyAudit,
}

/// Audits retained idempotency observations for payload-key conflicts.
///
/// The earliest observation defines the canonical digest. Later observations
/// with that digest are exact replays; every other digest is a conflict. Input
/// ordering does not affect results, and tenant identity is case-insensitive.
///
/// # Errors
///
/// Returns validation for more than 100,000 observations, blank identities,
/// zero timestamps, or payload digests that are not 64 hexadecimal characters.
pub fn audit_deduplication(
    observations: &[DedupeObservation],
) -> Result<Vec<DedupeKeyAudit>, HarborError> {
    if observations.len() > 100_000 {
        return Err(HarborError::Validation(
            "deduplication audit cannot exceed 100000 observations".into(),
        ));
    }
    let mut ordered = observations.to_vec();
    ordered.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then_with(|| left.tenant.cmp(&right.tenant))
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.payload_digest.cmp(&right.payload_digest))
    });
    let mut audits: BTreeMap<(String, String), MutableAudit> = BTreeMap::new();
    for observation in ordered {
        let tenant = observation.tenant.trim();
        let key = observation.key.trim();
        let digest = observation.payload_digest.trim().to_ascii_lowercase();
        if tenant.is_empty() || key.is_empty() {
            return Err(HarborError::Validation(
                "deduplication tenant and key are required".into(),
            ));
        }
        if observation.received_at == 0 {
            return Err(HarborError::Validation(
                "deduplication receipt time must be positive".into(),
            ));
        }
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(HarborError::Validation(format!(
                "deduplication payload digest is invalid for {tenant}/{key}"
            )));
        }
        let identity = (tenant.to_ascii_lowercase(), key.to_owned());
        let audit = audits.entry(identity).or_insert_with(|| MutableAudit {
            row: DedupeKeyAudit {
                tenant: tenant.to_owned(),
                key: key.to_owned(),
                canonical_payload_digest: digest.clone(),
                observations: 0,
                exact_replays: 0,
                conflicting_payloads: 0,
                first_received_at: observation.received_at,
                last_received_at: observation.received_at,
                healthy: true,
            },
        });
        if audit.row.observations > 0 {
            if audit.row.canonical_payload_digest == digest {
                audit.row.exact_replays += 1;
            } else {
                audit.row.conflicting_payloads += 1;
                audit.row.healthy = false;
            }
        }
        audit.row.observations += 1;
        audit.row.last_received_at = observation.received_at;
    }
    let mut rows = audits
        .into_values()
        .map(|audit| audit.row)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .conflicting_payloads
            .cmp(&left.conflicting_payloads)
            .then_with(|| left.tenant.cmp(&right.tenant))
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(rows)
}
