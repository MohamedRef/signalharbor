use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::journal::{FileJournal, Record};
use crate::model::{
    AttemptOutcome, AttemptResult, Delivery, DeliveryState, HarborError, HarborSummary,
    IngestResult, Route, WebhookEvent, WebhookInput,
};

pub struct SignalHarbor {
    journal: FileJournal,
    routes: BTreeMap<String, Route>,
    events: BTreeMap<String, WebhookEvent>,
    deliveries: BTreeMap<String, Delivery>,
    dedupe: HashMap<(String, String), String>,
    event_sequence: u64,
    delivery_sequence: u64,
}

impl SignalHarbor {
    /// Opens a journal and reconstructs the latest route and delivery state.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Journal`] when the file cannot be read, contains
    /// an invalid record, or references missing related records.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HarborError> {
        let journal = FileJournal::new(path);
        let mut harbor = Self {
            journal,
            routes: BTreeMap::new(),
            events: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            dedupe: HashMap::new(),
            event_sequence: 0,
            delivery_sequence: 0,
        };
        for record in harbor.journal.replay()? {
            match record {
                Record::Route(route) => {
                    harbor.routes.insert(route.id.clone(), route);
                }
                Record::Event(event) => {
                    harbor.events.insert(event.id.clone(), event);
                }
                Record::Delivery(delivery) => {
                    harbor.deliveries.insert(delivery.id.clone(), delivery);
                }
            }
        }
        harbor.rebuild_indexes()?;
        Ok(harbor)
    }

    /// Persists a new route before exposing it to ingestion.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Conflict`] for an existing route id or a journal
    /// error when the route cannot be durably appended.
    pub fn register_route(&mut self, route: Route) -> Result<Route, HarborError> {
        if self.routes.contains_key(&route.id) {
            return Err(HarborError::Conflict(format!(
                "route id already registered: {}",
                route.id
            )));
        }
        self.journal.append(&Record::Route(route.clone()))?;
        self.routes.insert(route.id.clone(), route.clone());
        Ok(route)
    }

    /// Activates or deactivates a route and journals the new state.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] for an unknown route or a journal
    /// error when the change cannot be durably appended.
    pub fn set_route_active(&mut self, route_id: &str, active: bool) -> Result<Route, HarborError> {
        let mut route = self
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        if route.active == active {
            return Ok(route);
        }
        route.active = active;
        self.journal.append(&Record::Route(route.clone()))?;
        self.routes.insert(route.id.clone(), route.clone());
        Ok(route)
    }

    #[must_use]
    pub fn routes(&self) -> Vec<Route> {
        self.routes.values().cloned().collect()
    }

    /// Accepts an event, deduplicates it, and creates initial deliveries in one transaction.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Conflict`] when a dedupe key is reused with
    /// different content, or a journal error when persistence fails.
    pub fn ingest(&mut self, input: WebhookInput) -> Result<IngestResult, HarborError> {
        let dedupe_key = (input.tenant.clone(), input.dedupe_key.clone());
        if let Some(existing_id) = self.dedupe.get(&dedupe_key) {
            let existing = self.events.get(existing_id).ok_or_else(|| {
                HarborError::Journal("dedupe index references a missing event".into())
            })?;
            if existing.topic != input.topic || existing.body != input.body {
                return Err(HarborError::Conflict(format!(
                    "dedupe key {} was already used with different content",
                    input.dedupe_key
                )));
            }
            return Ok(IngestResult {
                event: existing.clone(),
                duplicate: true,
                delivery_ids: self.delivery_ids_for_event(&existing.id),
            });
        }

        let event_sequence = self.event_sequence + 1;
        let event = WebhookEvent {
            id: format!("evt-{event_sequence:012}"),
            tenant: input.tenant,
            topic: input.topic,
            dedupe_key: input.dedupe_key,
            body: input.body,
            received_at: input.received_at,
        };
        let matching_routes = self
            .routes
            .values()
            .filter(|route| route.matches(&event.tenant, &event.topic))
            .cloned()
            .collect::<Vec<_>>();
        let mut next_delivery_sequence = self.delivery_sequence;
        let deliveries = matching_routes
            .into_iter()
            .map(|route| {
                next_delivery_sequence += 1;
                Delivery {
                    id: format!("del-{next_delivery_sequence:012}"),
                    event_id: event.id.clone(),
                    route_id: route.id,
                    endpoint: route.endpoint,
                    state: DeliveryState::Pending,
                    attempts: 0,
                    next_attempt_at: event.received_at,
                    last_error: None,
                }
            })
            .collect::<Vec<_>>();

        let mut records = vec![Record::Event(event.clone())];
        records.extend(deliveries.iter().cloned().map(Record::Delivery));
        self.journal.append_batch(&records)?;

        self.event_sequence = event_sequence;
        self.delivery_sequence = next_delivery_sequence;
        self.dedupe.insert(dedupe_key, event.id.clone());
        self.events.insert(event.id.clone(), event.clone());
        let delivery_ids = deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        for delivery in deliveries {
            self.deliveries.insert(delivery.id.clone(), delivery);
        }
        Ok(IngestResult {
            event,
            duplicate: false,
            delivery_ids,
        })
    }

    /// Returns a previously accepted event.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] when the id is unknown.
    pub fn event(&self, event_id: &str) -> Result<WebhookEvent, HarborError> {
        self.events
            .get(event_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("event {event_id}")))
    }

    /// Returns the latest snapshot for a delivery.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] when the id is unknown.
    pub fn delivery(&self, delivery_id: &str) -> Result<Delivery, HarborError> {
        self.deliveries
            .get(delivery_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("delivery {delivery_id}")))
    }

    /// Selects pending deliveries due at or before `now` in stable order.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Validation`] when `limit` is zero or greater
    /// than 10,000.
    pub fn due_deliveries(&self, now: u64, limit: usize) -> Result<Vec<Delivery>, HarborError> {
        if limit == 0 || limit > 10_000 {
            return Err(HarborError::Validation(
                "delivery limit must be from 1 through 10000".into(),
            ));
        }
        let mut rows = self
            .deliveries
            .values()
            .filter(|delivery| delivery.due(now))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (left.next_attempt_at, &left.id).cmp(&(right.next_attempt_at, &right.id))
        });
        rows.truncate(limit);
        Ok(rows)
    }

    /// Applies an HTTP or transport outcome and persists the resulting state.
    ///
    /// # Errors
    ///
    /// Returns not-found, terminal-state, not-due, relationship, or journal
    /// errors without mutating the in-memory delivery.
    pub fn record_attempt(
        &mut self,
        delivery_id: &str,
        attempted_at: u64,
        outcome: AttemptOutcome,
    ) -> Result<AttemptResult, HarborError> {
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::Pending {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} is already terminal"
            )));
        }
        if current.next_attempt_at > attempted_at {
            return Err(HarborError::NotDue {
                delivery_id: delivery_id.to_owned(),
                due_at: current.next_attempt_at,
            });
        }
        let route = self
            .routes
            .get(&current.route_id)
            .ok_or_else(|| HarborError::Journal(format!("missing route {}", current.route_id)))?;
        let mut updated = current;
        updated.attempts += 1;
        let (successful, retryable, error) = classify(outcome);
        let mut retried = false;
        let mut retry_delay_seconds = None;
        if successful {
            updated.state = DeliveryState::Succeeded;
            updated.last_error = None;
        } else if retryable && updated.attempts < route.max_attempts {
            let exponent = u32::from(updated.attempts.saturating_sub(1)).min(20);
            let delay = route
                .base_delay_seconds
                .saturating_mul(1_u64 << exponent)
                .min(86_400);
            updated.next_attempt_at = attempted_at.saturating_add(delay);
            updated.last_error = Some(error);
            retried = true;
            retry_delay_seconds = Some(delay);
        } else {
            updated.state = DeliveryState::DeadLetter;
            updated.last_error = Some(error);
        }
        self.journal.append(&Record::Delivery(updated.clone()))?;
        self.deliveries.insert(updated.id.clone(), updated.clone());
        Ok(AttemptResult {
            delivery: updated,
            retried,
            retry_delay_seconds,
        })
    }

    /// Resets a dead-letter delivery for another attempt at `due_at`.
    ///
    /// # Errors
    ///
    /// Returns an error unless the delivery is dead-lettered, its route is
    /// active, and the new snapshot can be written to the journal.
    pub fn redrive(&mut self, delivery_id: &str, due_at: u64) -> Result<Delivery, HarborError> {
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::DeadLetter {
            return Err(HarborError::Conflict(
                "only dead-letter deliveries can be redriven".into(),
            ));
        }
        let route = self
            .routes
            .get(&current.route_id)
            .ok_or_else(|| HarborError::Journal(format!("missing route {}", current.route_id)))?;
        if !route.active {
            return Err(HarborError::Conflict(
                "cannot redrive while the route is inactive".into(),
            ));
        }
        let updated = Delivery {
            state: DeliveryState::Pending,
            attempts: 0,
            next_attempt_at: due_at,
            last_error: None,
            ..current
        };
        self.journal.append(&Record::Delivery(updated.clone()))?;
        self.deliveries.insert(updated.id.clone(), updated.clone());
        Ok(updated)
    }

    #[must_use]
    pub fn dead_letters(&self) -> Vec<Delivery> {
        self.deliveries
            .values()
            .filter(|delivery| delivery.state == DeliveryState::DeadLetter)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn summary(&self) -> HarborSummary {
        HarborSummary {
            events: self.events.len(),
            deliveries: self.deliveries.len(),
            pending: self
                .deliveries
                .values()
                .filter(|row| row.state == DeliveryState::Pending)
                .count(),
            succeeded: self
                .deliveries
                .values()
                .filter(|row| row.state == DeliveryState::Succeeded)
                .count(),
            dead_letter: self
                .deliveries
                .values()
                .filter(|row| row.state == DeliveryState::DeadLetter)
                .count(),
        }
    }

    fn delivery_ids_for_event(&self, event_id: &str) -> Vec<String> {
        self.deliveries
            .values()
            .filter(|delivery| delivery.event_id == event_id)
            .map(|delivery| delivery.id.clone())
            .collect()
    }

    fn rebuild_indexes(&mut self) -> Result<(), HarborError> {
        self.dedupe.clear();
        for event in self.events.values() {
            let key = (event.tenant.clone(), event.dedupe_key.clone());
            if let Some(existing) = self.dedupe.insert(key, event.id.clone()) {
                return Err(HarborError::Journal(format!(
                    "duplicate dedupe key in events {existing} and {}",
                    event.id
                )));
            }
        }
        for delivery in self.deliveries.values() {
            if !self.events.contains_key(&delivery.event_id) {
                return Err(HarborError::Journal(format!(
                    "delivery {} references missing event {}",
                    delivery.id, delivery.event_id
                )));
            }
            if !self.routes.contains_key(&delivery.route_id) {
                return Err(HarborError::Journal(format!(
                    "delivery {} references missing route {}",
                    delivery.id, delivery.route_id
                )));
            }
        }
        self.event_sequence = self.events.len() as u64;
        self.delivery_sequence = self.deliveries.len() as u64;
        Ok(())
    }
}

fn classify(outcome: AttemptOutcome) -> (bool, bool, String) {
    match outcome {
        AttemptOutcome::Http(status) if (200..300).contains(&status) => {
            (true, false, String::new())
        }
        AttemptOutcome::Http(status)
            if status == 408 || status == 425 || status == 429 || (500..600).contains(&status) =>
        {
            (false, true, format!("http {status}"))
        }
        AttemptOutcome::Http(status) => (false, false, format!("http {status}")),
        AttemptOutcome::Transport(message) => (false, true, format!("transport: {message}")),
    }
}
