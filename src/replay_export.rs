//! Deterministic replay investigation export rows.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayExportEvent {
    pub event_id: String,
    pub route_id: String,
    pub received_at_epoch: u64,
    pub attempt_count: u32,
    pub disposition: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayExportRow {
    pub ordinal: usize,
    pub event_id: String,
    pub route_id: String,
    pub received_at_epoch: u64,
    pub attempt_count: u32,
    pub disposition: String,
}

/// Produces stable, bounded replay export rows.
///
/// # Errors
/// Returns an error for invalid limits, blank values, duplicates, or oversized exports.
pub fn build_replay_export(
    events: &[ReplayExportEvent],
    maximum_rows: usize,
) -> Result<Vec<ReplayExportRow>, String> {
    if maximum_rows == 0 || maximum_rows > 100_000 {
        return Err("replay export maximum rows must be between 1 and 100000".to_owned());
    }
    if events.len() > maximum_rows {
        return Err(format!(
            "replay export has {} rows but limit is {maximum_rows}",
            events.len()
        ));
    }
    let mut ordered = events.to_vec();
    ordered.sort_by(|left, right| {
        left.received_at_epoch
            .cmp(&right.received_at_epoch)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    let mut seen = std::collections::BTreeSet::new();
    let mut rows = Vec::with_capacity(ordered.len());
    for (index, event) in ordered.into_iter().enumerate() {
        let event_id = event.event_id.trim();
        let route_id = event.route_id.trim();
        let disposition = event.disposition.trim().to_lowercase();
        if event_id.is_empty() || route_id.is_empty() || disposition.is_empty() {
            return Err("replay export event, route, and disposition are required".to_owned());
        }
        if !seen.insert(event_id.to_lowercase()) {
            return Err(format!("duplicate replay export event: {event_id}"));
        }
        rows.push(ReplayExportRow {
            ordinal: index + 1,
            event_id: event_id.to_owned(),
            route_id: route_id.to_owned(),
            received_at_epoch: event.received_at_epoch,
            attempt_count: event.attempt_count,
            disposition,
        });
    }
    Ok(rows)
}
