use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMaintenanceWindow {
    pub endpoint_id: String,
    pub starts_at_epoch: i64,
    pub ends_at_epoch: i64,
    pub drain_lead_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceWindowState {
    pub endpoint_id: String,
    pub state: String,
    pub seconds_until_transition: u64,
}

/// Classifies endpoint maintenance windows for deterministic router drain decisions.
///
/// # Errors
/// Returns validation errors for invalid chronology or duplicate endpoint windows.
pub fn assess_maintenance_windows(
    windows: &[EndpointMaintenanceWindow],
    now_epoch: i64,
) -> Result<Vec<MaintenanceWindowState>, HarborError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut states = Vec::new();
    for window in windows {
        let endpoint = window.endpoint_id.trim();
        if endpoint.is_empty()
            || !seen.insert(endpoint.to_lowercase())
            || window.ends_at_epoch <= window.starts_at_epoch
        {
            return Err(HarborError::Validation(
                "invalid or duplicate maintenance window".into(),
            ));
        }
        let drain_at = window
            .starts_at_epoch
            .saturating_sub(i64::try_from(window.drain_lead_seconds).unwrap_or(i64::MAX));
        let (state, transition) = if now_epoch < drain_at {
            ("scheduled", drain_at - now_epoch)
        } else if now_epoch < window.starts_at_epoch {
            ("draining", window.starts_at_epoch - now_epoch)
        } else if now_epoch < window.ends_at_epoch {
            ("maintenance", window.ends_at_epoch - now_epoch)
        } else {
            ("complete", 0)
        };
        states.push(MaintenanceWindowState {
            endpoint_id: endpoint.into(),
            state: state.into(),
            seconds_until_transition: transition.cast_unsigned(),
        });
    }
    states.sort_by(|a, b| {
        a.state
            .cmp(&b.state)
            .then(a.endpoint_id.cmp(&b.endpoint_id))
    });
    Ok(states)
}
