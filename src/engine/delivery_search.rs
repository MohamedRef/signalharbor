use std::collections::BTreeMap;

use super::SignalHarbor;
use crate::model::required;
use crate::{Delivery, DeliveryState, HarborError, WebhookEvent};

/// Tenant-scoped delivery inspection, distinct from the worker's due queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySearchQuery {
    pub tenant: String,
    pub route_id: Option<String>,
    pub topic: Option<String>,
    pub state: Option<DeliveryState>,
    pub received_from: Option<u64>,
    pub received_through: Option<u64>,
    pub page_size: usize,
}

impl DeliverySearchQuery {
    /// Creates an unfiltered tenant search with a bounded page size.
    ///
    /// # Errors
    /// Returns validation for an empty tenant or page size outside 1..=1,000.
    pub fn new(tenant: &str, page_size: usize) -> Result<Self, HarborError> {
        Self {
            tenant: tenant.to_owned(),
            route_id: None,
            topic: None,
            state: None,
            received_from: None,
            received_through: None,
            page_size,
        }
        .normalized()
    }

    fn normalized(&self) -> Result<Self, HarborError> {
        if !(1..=1_000).contains(&self.page_size) {
            return Err(HarborError::Validation(
                "search page size must be from 1 through 1000".into(),
            ));
        }
        if let (Some(from), Some(through)) = (self.received_from, self.received_through)
            && from > through
        {
            return Err(HarborError::Validation(
                "search receive window is inverted".into(),
            ));
        }
        Ok(Self {
            tenant: required(&self.tenant, "tenant", 80)?.to_ascii_lowercase(),
            route_id: self
                .route_id
                .as_deref()
                .map(|value| required(value, "route id", 80))
                .transpose()?,
            topic: self
                .topic
                .as_deref()
                .map(|value| required(value, "topic", 120).map(|text| text.to_ascii_lowercase()))
                .transpose()?,
            ..self.clone()
        })
    }

    fn matches(&self, delivery: &Delivery, event: &WebhookEvent) -> bool {
        event.tenant == self.tenant
            && self
                .route_id
                .as_ref()
                .is_none_or(|route| *route == delivery.route_id)
            && self
                .topic
                .as_ref()
                .is_none_or(|topic| *topic == event.topic)
            && self.state.is_none_or(|state| state == delivery.state)
            && self
                .received_from
                .is_none_or(|from| event.received_at >= from)
            && self
                .received_through
                .is_none_or(|through| event.received_at <= through)
    }
}

/// Opaque continuation bound to the normalized query and creation high-water.
///
/// Reuse only with the same journal. New deliveries are excluded from later
/// pages, including backfills for older events. Existing status and lease data
/// remain live: this is not a point-in-time snapshot of mutable delivery state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySearchCursor {
    query: DeliverySearchQuery,
    after: (u64, String),
    last_delivery_sequence: u64,
}

/// Inspection metadata intentionally omits the retained webhook body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySearchRow {
    pub delivery: Delivery,
    pub tenant: String,
    pub topic: String,
    pub received_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySearchPage {
    pub rows: Vec<DeliverySearchRow>,
    pub next_cursor: Option<DeliverySearchCursor>,
}

impl SignalHarbor {
    /// Searches delivery metadata by tenant, route, topic, state and receive time.
    ///
    /// Receive bounds are inclusive. Results are ordered by event receive time
    /// then delivery id, regardless of retry deadlines. At most `page_size + 1`
    /// matching rows are retained during selection. Continuations exclude work
    /// created after the first page and reject changed filters or page sizes.
    /// The caller remains responsible for authenticating the requested tenant.
    ///
    /// # Errors
    ///
    /// Returns validation for malformed filters, conflict for a cursor belonging
    /// to another query or a later journal state, and journal errors for invalid
    /// delivery identifiers or missing event references. The operation never writes.
    pub fn search_deliveries(
        &self,
        query: &DeliverySearchQuery,
        cursor: Option<&DeliverySearchCursor>,
    ) -> Result<DeliverySearchPage, HarborError> {
        let query = query.normalized()?;
        let high_water =
            cursor.map_or(self.delivery_sequence, |value| value.last_delivery_sequence);
        if cursor.is_some_and(|value| value.query != query) || high_water > self.delivery_sequence {
            return Err(HarborError::Conflict(
                "delivery search cursor does not match query or journal state".into(),
            ));
        }
        let mut selected = BTreeMap::new();
        for delivery in self.deliveries.values() {
            if delivery_sequence(&delivery.id)? > high_water {
                continue;
            }
            let event = self.events.get(&delivery.event_id).ok_or_else(|| {
                HarborError::Journal(format!(
                    "delivery {} references a missing event",
                    delivery.id
                ))
            })?;
            if !query.matches(delivery, event) {
                continue;
            }
            let key = (event.received_at, delivery.id.clone());
            if cursor.is_some_and(|value| key <= value.after) {
                continue;
            }
            if selected.len() == query.page_size + 1
                && selected
                    .last_key_value()
                    .is_some_and(|(last, _)| key >= *last)
            {
                continue;
            }
            selected.insert(
                key,
                DeliverySearchRow {
                    delivery: delivery.clone(),
                    tenant: event.tenant.clone(),
                    topic: event.topic.clone(),
                    received_at: event.received_at,
                },
            );
            if selected.len() > query.page_size + 1 {
                selected.pop_last();
            }
        }
        let has_more = selected.len() > query.page_size;
        let mut rows = Vec::with_capacity(selected.len().min(query.page_size));
        let mut after = None;
        for (key, row) in selected.into_iter().take(query.page_size) {
            after = Some(key);
            rows.push(row);
        }
        let next_cursor = if has_more {
            after.map(|after| DeliverySearchCursor {
                query,
                after,
                last_delivery_sequence: high_water,
            })
        } else {
            None
        };
        Ok(DeliverySearchPage { rows, next_cursor })
    }
}

fn delivery_sequence(id: &str) -> Result<u64, HarborError> {
    id.strip_prefix("del-")
        .and_then(|sequence| sequence.parse().ok())
        .filter(|sequence| *sequence > 0)
        .ok_or_else(|| HarborError::Journal(format!("invalid delivery identifier: {id}")))
}
