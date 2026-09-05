//! Stable cursor pagination for replay event investigations.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEventRow {
    pub event_id: String,
    pub received_at_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEventPage {
    pub rows: Vec<ReplayEventRow>,
    pub next_cursor: Option<String>,
}

/// Pages event rows in stable timestamp and identity order.
///
/// # Errors
/// Returns an error for invalid limits, duplicate rows, or an unknown cursor.
pub fn page_replay_events(
    rows: &[ReplayEventRow],
    after: Option<&str>,
    limit: usize,
) -> Result<ReplayEventPage, String> {
    if limit == 0 || limit > 500 {
        return Err("replay page limit must be between 1 and 500".to_owned());
    }
    let mut ordered = rows.to_vec();
    ordered.sort_by(|left, right| {
        left.received_at_epoch
            .cmp(&right.received_at_epoch)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    for pair in ordered.windows(2) {
        if pair[0].event_id.eq_ignore_ascii_case(&pair[1].event_id) {
            return Err(format!("duplicate replay event row: {}", pair[0].event_id));
        }
    }
    let start = match after {
        None => 0,
        Some(cursor) => ordered
            .iter()
            .position(|row| row.event_id == cursor)
            .map_or_else(
                || Err(format!("unknown replay cursor: {cursor}")),
                |index| Ok(index + 1),
            )?,
    };
    let end = start.saturating_add(limit).min(ordered.len());
    let page_rows = ordered[start..end].to_vec();
    let next_cursor = if end < ordered.len() {
        page_rows.last().map(|row| row.event_id.clone())
    } else {
        None
    };
    Ok(ReplayEventPage {
        rows: page_rows,
        next_cursor,
    })
}
