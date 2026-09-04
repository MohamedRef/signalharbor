use std::collections::{BTreeMap, BTreeSet};

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBurstLimit {
    pub tenant_id: String,
    pub sustained_per_second: u64,
    pub burst_capacity: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBurstSample {
    pub tenant_id: String,
    pub window_seconds: u64,
    pub accepted_events: u64,
    pub queued_events: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BurstReadiness {
    Rejected,
    Throttled,
    Healthy,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBurstAssessment {
    pub tenant_id: String,
    pub readiness: BurstReadiness,
    pub permitted_events: u64,
    pub excess_events: u64,
}

/// Assesses tenant ingress bursts without allowing arithmetic overflow.
///
/// # Errors
///
/// Rejects invalid limits, duplicate tenants, unknown samples, and overflow.
pub fn assess_tenant_bursts(
    limits: &[TenantBurstLimit],
    samples: &[TenantBurstSample],
) -> Result<Vec<TenantBurstAssessment>, HarborError> {
    let mut configured = BTreeMap::new();
    for limit in limits {
        let tenant = required(&limit.tenant_id, "tenant burst identity")?;
        let key = tenant.to_ascii_lowercase();
        if configured.contains_key(&key)
            || limit.sustained_per_second == 0
            || limit.burst_capacity == 0
        {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate tenant burst limit {tenant}"
            )));
        }
        configured.insert(
            key,
            (tenant, limit.sustained_per_second, limit.burst_capacity),
        );
    }
    let mut seen = BTreeSet::new();
    let mut rows = Vec::with_capacity(samples.len());
    for sample in samples {
        let tenant = required(&sample.tenant_id, "tenant burst sample identity")?;
        let key = tenant.to_ascii_lowercase();
        let Some((canonical, sustained, burst)) = configured.get(&key) else {
            return Err(HarborError::Validation(format!(
                "tenant {tenant} has no burst limit"
            )));
        };
        if !seen.insert(key) || sample.window_seconds == 0 {
            return Err(HarborError::Validation(format!(
                "invalid or duplicate tenant burst sample {tenant}"
            )));
        }
        let sustained_capacity = sustained
            .checked_mul(sample.window_seconds)
            .ok_or_else(|| HarborError::Validation("tenant burst capacity overflows".into()))?;
        let permitted = sustained_capacity
            .checked_add(*burst)
            .ok_or_else(|| HarborError::Validation("tenant burst allowance overflows".into()))?;
        let demand = sample
            .accepted_events
            .checked_add(sample.queued_events)
            .ok_or_else(|| HarborError::Validation("tenant burst demand overflows".into()))?;
        let readiness = if sample.accepted_events > permitted {
            BurstReadiness::Rejected
        } else if demand > permitted {
            BurstReadiness::Throttled
        } else {
            BurstReadiness::Healthy
        };
        rows.push(TenantBurstAssessment {
            tenant_id: canonical.clone(),
            readiness,
            permitted_events: permitted,
            excess_events: demand.saturating_sub(permitted),
        });
    }
    rows.sort_by(|a, b| {
        a.readiness
            .cmp(&b.readiness)
            .then_with(|| b.excess_events.cmp(&a.excess_events))
            .then_with(|| a.tenant_id.cmp(&b.tenant_id))
    });
    Ok(rows)
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(format!("{field} is required")))
    } else {
        Ok(value.to_owned())
    }
}
