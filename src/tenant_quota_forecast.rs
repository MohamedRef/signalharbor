use crate::HarborError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuotaSample {
    pub tenant_id: String,
    pub quota_events: u64,
    pub used_events: u64,
    pub recent_events_per_minute: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaForecast {
    pub tenant_id: String,
    pub remaining_events: u64,
    pub minutes_until_exhausted: Option<u64>,
    pub state: String,
}

/// Forecasts tenant ingress quota exhaustion from a deterministic recent rate.
///
/// # Errors
/// Rejects empty or duplicate tenants and usage greater than the quota.
pub fn forecast_tenant_quotas(
    samples: &[TenantQuotaSample],
    warning_horizon_minutes: u64,
) -> Result<Vec<QuotaForecast>, HarborError> {
    if warning_horizon_minutes == 0 {
        return Err(HarborError::Validation(
            "quota warning horizon is invalid".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut forecasts = Vec::with_capacity(samples.len());
    for sample in samples {
        let tenant = sample.tenant_id.trim();
        if tenant.is_empty()
            || sample.quota_events == 0
            || sample.used_events > sample.quota_events
            || !seen.insert(tenant.to_ascii_lowercase())
        {
            return Err(HarborError::Validation(
                "invalid tenant quota sample".into(),
            ));
        }
        let remaining = sample.quota_events - sample.used_events;
        let minutes = if sample.recent_events_per_minute == 0 {
            None
        } else {
            Some(remaining.div_ceil(sample.recent_events_per_minute))
        };
        let state = if remaining == 0 {
            "exhausted"
        } else if minutes.is_some_and(|value| value <= warning_horizon_minutes) {
            "at_risk"
        } else if minutes.is_none() {
            "idle"
        } else {
            "healthy"
        };
        forecasts.push(QuotaForecast {
            tenant_id: tenant.into(),
            remaining_events: remaining,
            minutes_until_exhausted: minutes,
            state: state.into(),
        });
    }
    forecasts.sort_by(|a, b| {
        rank(&a.state)
            .cmp(&rank(&b.state))
            .then(a.minutes_until_exhausted.cmp(&b.minutes_until_exhausted))
            .then(a.tenant_id.cmp(&b.tenant_id))
    });
    Ok(forecasts)
}

fn rank(state: &str) -> u8 {
    match state {
        "exhausted" => 0,
        "at_risk" => 1,
        "healthy" => 2,
        _ => 3,
    }
}
