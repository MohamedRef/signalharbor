use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQueueDepth {
    pub tenant_id: String,
    pub pending: u64,
    pub maximum_share_per_mille: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantFairnessBreach {
    pub tenant_id: String,
    pub pending: u64,
    pub actual_share_per_mille: u16,
    pub excess_share_per_mille: u16,
}

/// Finds tenants occupying more than their configured share of a shared queue.
///
/// # Errors
///
/// Returns validation for blank or duplicate tenants and invalid share limits.
pub fn assess_tenant_fairness(
    rows: &[TenantQueueDepth],
) -> Result<Vec<TenantFairnessBreach>, HarborError> {
    let mut tenants = std::collections::BTreeSet::new();
    let mut total = 0_u64;
    for row in rows {
        let tenant_id = row.tenant_id.trim();
        if tenant_id.is_empty()
            || row.maximum_share_per_mille == 0
            || row.maximum_share_per_mille > 1_000
        {
            return Err(HarborError::Validation(
                "tenant and a share limit from 1 to 1000 per mille are required".into(),
            ));
        }
        if !tenants.insert(tenant_id.to_owned()) {
            return Err(HarborError::Validation(format!(
                "duplicate tenant {tenant_id}"
            )));
        }
        total = total.checked_add(row.pending).ok_or_else(|| {
            HarborError::Validation("tenant queue total exceeds supported range".into())
        })?;
    }
    if total == 0 {
        return Ok(Vec::new());
    }
    let mut breaches = Vec::new();
    for row in rows {
        let actual = row.pending.saturating_mul(1_000).div_ceil(total);
        let actual_share_per_mille = u16::try_from(actual).unwrap_or(1_000);
        if actual_share_per_mille > row.maximum_share_per_mille {
            breaches.push(TenantFairnessBreach {
                tenant_id: row.tenant_id.trim().to_owned(),
                pending: row.pending,
                actual_share_per_mille,
                excess_share_per_mille: actual_share_per_mille - row.maximum_share_per_mille,
            });
        }
    }
    breaches.sort_by(|left, right| {
        right
            .excess_share_per_mille
            .cmp(&left.excess_share_per_mille)
            .then_with(|| left.tenant_id.cmp(&right.tenant_id))
    });
    Ok(breaches)
}
