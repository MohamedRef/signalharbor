use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEvent {
    pub id: String,
    pub endpoint: String,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryBatch {
    pub endpoint: String,
    pub event_ids: Vec<String>,
    pub payload_bytes: u64,
}

/// Builds deterministic endpoint batches within count and byte ceilings.
///
/// # Errors
///
/// Returns validation for zero limits, duplicate event identities, missing
/// endpoint data, zero payloads, or payloads larger than the byte ceiling.
pub fn build_delivery_batches(
    events: &[BatchEvent],
    max_events: usize,
    max_payload_bytes: u64,
) -> Result<Vec<DeliveryBatch>, HarborError> {
    if max_events == 0 || max_payload_bytes == 0 {
        return Err(HarborError::Validation(
            "batch limits must be positive".into(),
        ));
    }
    let mut rows = events.to_vec();
    rows.sort_by(|left, right| {
        left.endpoint
            .cmp(&right.endpoint)
            .then(left.id.cmp(&right.id))
    });
    let mut seen = std::collections::HashSet::new();
    let mut batches: Vec<DeliveryBatch> = Vec::new();
    for event in rows {
        let id = event.id.trim();
        let endpoint = event.endpoint.trim();
        if id.is_empty()
            || endpoint.is_empty()
            || event.payload_bytes == 0
            || event.payload_bytes > max_payload_bytes
        {
            return Err(HarborError::Validation(
                "batch events require identity, endpoint, and feasible payload size".into(),
            ));
        }
        if !seen.insert(id.to_owned()) {
            return Err(HarborError::Validation(format!(
                "duplicate batch event {id}"
            )));
        }
        let append = batches.last().is_some_and(|batch| {
            batch.endpoint == endpoint
                && batch.event_ids.len() < max_events
                && batch
                    .payload_bytes
                    .checked_add(event.payload_bytes)
                    .is_some_and(|total| total <= max_payload_bytes)
        });
        if append && let Some(batch) = batches.last_mut() {
            batch.event_ids.push(id.to_owned());
            batch.payload_bytes += event.payload_bytes;
        } else {
            batches.push(DeliveryBatch {
                endpoint: endpoint.to_owned(),
                event_ids: vec![id.to_owned()],
                payload_bytes: event.payload_bytes,
            });
        }
    }
    Ok(batches)
}
