use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryQueueSample {
    pub route_id: String,
    pub queued: u64,
    pub workers: u32,
    pub attempts_per_worker_per_minute: u32,
    pub maximum_drain_minutes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrySaturation {
    pub route_id: String,
    pub queued: u64,
    pub drain_minutes: u64,
    pub excess_minutes: u64,
}

/// Detects retry routes whose current worker pool cannot drain within policy.
///
/// # Errors
///
/// Returns validation when route identity or any positive capacity field is invalid.
pub fn detect_retry_saturation(
    rows: &[RetryQueueSample],
) -> Result<Vec<RetrySaturation>, HarborError> {
    let mut routes = std::collections::BTreeSet::new();
    let mut saturated = Vec::new();
    for row in rows {
        let route_id = row.route_id.trim();
        if route_id.is_empty()
            || row.workers == 0
            || row.attempts_per_worker_per_minute == 0
            || row.maximum_drain_minutes == 0
        {
            return Err(HarborError::Validation(
                "route and positive retry capacity fields are required".into(),
            ));
        }
        if !routes.insert(route_id.to_owned()) {
            return Err(HarborError::Validation(format!(
                "duplicate retry route {route_id}"
            )));
        }
        let capacity = u64::from(row.workers) * u64::from(row.attempts_per_worker_per_minute);
        let drain_minutes = row.queued.div_ceil(capacity);
        let maximum = u64::from(row.maximum_drain_minutes);
        if drain_minutes > maximum {
            saturated.push(RetrySaturation {
                route_id: route_id.to_owned(),
                queued: row.queued,
                drain_minutes,
                excess_minutes: drain_minutes - maximum,
            });
        }
    }
    saturated.sort_by(|left, right| {
        right
            .excess_minutes
            .cmp(&left.excess_minutes)
            .then_with(|| left.route_id.cmp(&right.route_id))
    });
    Ok(saturated)
}
