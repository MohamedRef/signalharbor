use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::journal::{FileJournal, Record};
use crate::model::{
    AttemptHistorySummary, AttemptOutcome, AttemptResult, BackfillRequest, BackfillResult,
    CircuitState, Delivery, DeliveryAttempt, DeliveryState, EndpointUpdateMode,
    EndpointUpdateResult, HarborError, HarborSummary, IngestResult, Route, RouteCircuitHealth,
    WebhookEvent, WebhookInput,
};

pub struct SignalHarbor {
    journal: FileJournal,
    routes: BTreeMap<String, Route>,
    events: BTreeMap<String, WebhookEvent>,
    deliveries: BTreeMap<String, Delivery>,
    attempts: BTreeMap<String, Vec<DeliveryAttempt>>,
    dedupe: HashMap<(String, String), String>,
    event_sequence: u64,
    delivery_sequence: u64,
    attempt_sequence: u64,
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
            attempts: BTreeMap::new(),
            dedupe: HashMap::new(),
            event_sequence: 0,
            delivery_sequence: 0,
            attempt_sequence: 0,
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
                Record::Attempt(attempt) => {
                    harbor
                        .attempts
                        .entry(attempt.delivery_id.clone())
                        .or_default()
                        .push(attempt);
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

    /// Rotates a route endpoint and optionally migrates safe pending work in one journal batch.
    ///
    /// Future-only rotation preserves every existing delivery snapshot. Migration updates only
    /// pending deliveries that have never been attempted and do not have a live or expired lease;
    /// attempted, leased, successful, and dead-letter deliveries retain their original endpoint.
    /// The route and all migrated deliveries are journaled before in-memory state changes.
    ///
    /// # Errors
    ///
    /// Returns validation for an unsupported endpoint, not-found for an unknown route, or a
    /// journal error without exposing a partial rotation.
    pub fn update_route_endpoint(
        &mut self,
        route_id: &str,
        new_endpoint: &str,
        mode: EndpointUpdateMode,
    ) -> Result<EndpointUpdateResult, HarborError> {
        let new_endpoint = crate::model::validate_endpoint(new_endpoint)?;
        let mut route = self
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;

        let mut migrated = Vec::new();
        let mut retained_delivery_ids = Vec::new();
        for delivery in self
            .deliveries
            .values()
            .filter(|delivery| delivery.route_id == route_id)
        {
            let eligible = mode == EndpointUpdateMode::MigrateUnattempted
                && delivery.state == DeliveryState::Pending
                && delivery.attempts == 0
                && delivery.lease_owner.is_none()
                && delivery.lease_expires_at.is_none();
            if eligible {
                let mut updated = delivery.clone();
                updated.endpoint.clone_from(&new_endpoint);
                migrated.push(updated);
            } else {
                retained_delivery_ids.push(delivery.id.clone());
            }
        }

        if route.endpoint == new_endpoint && migrated.is_empty() {
            return Ok(EndpointUpdateResult {
                route,
                migrated_delivery_ids: Vec::new(),
                retained_delivery_ids,
            });
        }
        route.endpoint = new_endpoint;
        let mut records = vec![Record::Route(route.clone())];
        records.extend(migrated.iter().cloned().map(Record::Delivery));
        self.journal.append_batch(&records)?;

        self.routes.insert(route.id.clone(), route.clone());
        let migrated_delivery_ids = migrated
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        for delivery in migrated {
            self.deliveries.insert(delivery.id.clone(), delivery);
        }
        Ok(EndpointUpdateResult {
            route,
            migrated_delivery_ids,
            retained_delivery_ids,
        })
    }

    /// Configures failure isolation for a route and resets its current circuit state.
    ///
    /// # Errors
    ///
    /// Returns validation errors for unsupported bounds, not-found for an unknown
    /// route, or a journal error when the configuration cannot be persisted.
    pub fn configure_circuit_breaker(
        &mut self,
        route_id: &str,
        failure_threshold: u16,
        cooldown_seconds: u64,
    ) -> Result<RouteCircuitHealth, HarborError> {
        if !(1..=100).contains(&failure_threshold) {
            return Err(HarborError::Validation(
                "failure_threshold must be from 1 through 100".into(),
            ));
        }
        if !(1..=86_400).contains(&cooldown_seconds) {
            return Err(HarborError::Validation(
                "cooldown_seconds must be from 1 through 86400".into(),
            ));
        }
        let mut route = self
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        route.failure_threshold = failure_threshold;
        route.cooldown_seconds = cooldown_seconds;
        route.consecutive_failures = 0;
        route.circuit_open_until = None;
        self.journal.append(&Record::Route(route.clone()))?;
        self.routes.insert(route.id.clone(), route);
        self.route_circuit_health(route_id, 0)
    }

    /// Reports whether a route is closed, cooling down, or ready for a probe.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] when the route id is unknown.
    pub fn route_circuit_health(
        &self,
        route_id: &str,
        now: u64,
    ) -> Result<RouteCircuitHealth, HarborError> {
        let route = self
            .routes
            .get(route_id)
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        let state = match route.circuit_open_until {
            Some(until) if until > now => CircuitState::Open { until },
            Some(_) => CircuitState::HalfOpen,
            None => CircuitState::Closed,
        };
        Ok(RouteCircuitHealth {
            route_id: route.id.clone(),
            state,
            consecutive_failures: route.consecutive_failures,
            failure_threshold: route.failure_threshold,
            cooldown_seconds: route.cooldown_seconds,
        })
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
                    lease_owner: None,
                    lease_expires_at: None,
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

    /// Creates deliveries for historical events that match a route but were not routed earlier.
    ///
    /// Matching is deterministic by received time and event id. The complete delivery batch is
    /// journaled before in-memory indexes advance, so a persistence error exposes no partial work.
    /// Repeating the same request is idempotent because existing event-route pairs are skipped.
    ///
    /// # Errors
    ///
    /// Returns not-found for an unknown route, conflict for an inactive route or exhausted id
    /// sequence, and a journal error when the batch cannot be persisted.
    pub fn backfill_route(
        &mut self,
        request: &BackfillRequest,
    ) -> Result<BackfillResult, HarborError> {
        let route = self
            .routes
            .get(&request.route_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("route {}", request.route_id)))?;
        if !route.active {
            return Err(HarborError::Conflict(format!(
                "route {} is inactive",
                request.route_id
            )));
        }

        let existing = self
            .deliveries
            .values()
            .filter(|delivery| delivery.route_id == route.id)
            .map(|delivery| delivery.event_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut matched = self
            .events
            .values()
            .filter(|event| {
                event.received_at >= request.received_from
                    && event.received_at <= request.received_through
                    && route.matches(&event.tenant, &event.topic)
            })
            .collect::<Vec<_>>();
        matched.sort_by(|left, right| {
            left.received_at
                .cmp(&right.received_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        let already_routed = matched
            .iter()
            .filter(|event| existing.contains(event.id.as_str()))
            .count();
        let missing = matched
            .iter()
            .filter(|event| !existing.contains(event.id.as_str()))
            .collect::<Vec<_>>();
        let truncated = missing.len() > request.maximum_deliveries;

        let mut next_sequence = self.delivery_sequence;
        let mut deliveries = Vec::with_capacity(missing.len().min(request.maximum_deliveries));
        for event in missing.into_iter().take(request.maximum_deliveries) {
            next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
                HarborError::Conflict("delivery identifier sequence is exhausted".into())
            })?;
            deliveries.push(Delivery {
                id: format!("del-{next_sequence:012}"),
                event_id: event.id.clone(),
                route_id: route.id.clone(),
                endpoint: route.endpoint.clone(),
                state: DeliveryState::Pending,
                attempts: 0,
                next_attempt_at: request.due_at,
                last_error: None,
                lease_owner: None,
                lease_expires_at: None,
            });
        }

        let records = deliveries
            .iter()
            .cloned()
            .map(Record::Delivery)
            .collect::<Vec<_>>();
        self.journal.append_batch(&records)?;
        self.delivery_sequence = next_sequence;
        let created_delivery_ids = deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        for delivery in deliveries {
            self.deliveries.insert(delivery.id.clone(), delivery);
        }
        Ok(BackfillResult {
            route_id: route.id,
            matched_events: matched.len(),
            already_routed,
            created_delivery_ids,
            truncated,
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

    /// Returns the immutable, durable attempt history for one delivery.
    ///
    /// Redriving a dead letter starts a new delivery attempt cycle but does not
    /// erase earlier records. Histories are ordered by their global journal
    /// sequence and returned as defensive clones.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] when the delivery id is unknown.
    pub fn attempt_history(&self, delivery_id: &str) -> Result<Vec<DeliveryAttempt>, HarborError> {
        if !self.deliveries.contains_key(delivery_id) {
            return Err(HarborError::NotFound(format!("delivery {delivery_id}")));
        }
        Ok(self.attempts.get(delivery_id).cloned().unwrap_or_default())
    }

    /// Aggregates persisted delivery attempts for operational monitoring.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] when the delivery id is unknown.
    pub fn attempt_history_summary(
        &self,
        delivery_id: &str,
    ) -> Result<AttemptHistorySummary, HarborError> {
        let history = self.attempt_history(delivery_id)?;
        Ok(AttemptHistorySummary {
            delivery_id: delivery_id.to_owned(),
            total: history.len(),
            retried: history.iter().filter(|attempt| attempt.retried).count(),
            succeeded: history
                .iter()
                .filter(|attempt| attempt.resulting_state == DeliveryState::Succeeded)
                .count(),
            dead_lettered: history
                .iter()
                .filter(|attempt| attempt.resulting_state == DeliveryState::DeadLetter)
                .count(),
            transport_failures: history
                .iter()
                .filter(|attempt| matches!(attempt.outcome, AttemptOutcome::Transport(_)))
                .count(),
            first_attempt_at: history.first().map(|attempt| attempt.attempted_at),
            last_attempt_at: history.last().map(|attempt| attempt.attempted_at),
        })
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
            .filter(|delivery| delivery.due(now) && self.route_available(&delivery.route_id, now))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (left.next_attempt_at, &left.id).cmp(&(right.next_attempt_at, &right.id))
        });
        rows.truncate(limit);
        Ok(rows)
    }

    /// Atomically leases due deliveries to one worker for a bounded interval.
    ///
    /// Expired leases are eligible for recovery by another worker. Every lease
    /// snapshot is journaled in one transaction before any in-memory delivery
    /// changes, so a restart cannot expose a claim that was not persisted.
    ///
    /// # Errors
    ///
    /// Returns validation errors for an invalid owner, limit, or lease duration,
    /// or a journal error without mutating in-memory state.
    pub fn claim_due(
        &mut self,
        worker: &str,
        now: u64,
        limit: usize,
        lease_seconds: u64,
    ) -> Result<Vec<Delivery>, HarborError> {
        let worker = crate::model::required(worker, "worker", 120)?;
        if limit == 0 || limit > 1_000 {
            return Err(HarborError::Validation(
                "claim limit must be from 1 through 1000".into(),
            ));
        }
        if !(1..=3_600).contains(&lease_seconds) {
            return Err(HarborError::Validation(
                "lease_seconds must be from 1 through 3600".into(),
            ));
        }
        let mut available = self
            .deliveries
            .values()
            .filter(|delivery| delivery.due(now) && self.route_available(&delivery.route_id, now))
            .cloned()
            .collect::<Vec<_>>();
        available.sort_by(|left, right| {
            (left.next_attempt_at, &left.id).cmp(&(right.next_attempt_at, &right.id))
        });
        available.truncate(limit);
        let expires_at = now.saturating_add(lease_seconds);
        let claimed = available
            .into_iter()
            .map(|mut delivery| {
                delivery.lease_owner = Some(worker.clone());
                delivery.lease_expires_at = Some(expires_at);
                delivery
            })
            .collect::<Vec<_>>();
        let records = claimed
            .iter()
            .cloned()
            .map(Record::Delivery)
            .collect::<Vec<_>>();
        self.journal.append_batch(&records)?;
        for delivery in &claimed {
            self.deliveries
                .insert(delivery.id.clone(), delivery.clone());
        }
        Ok(claimed)
    }

    /// Relinquishes a live claim without recording a delivery attempt.
    ///
    /// A worker can defer the pending delivery while shutting down or applying
    /// backpressure. Ownership, lease lifetime, and the bounded defer time are
    /// checked before the replacement snapshot is journaled. In-memory state is
    /// changed only after the durable append succeeds.
    ///
    /// # Errors
    ///
    /// Returns validation for an invalid worker or defer time, conflict when the
    /// worker does not own a live lease, not-found for an unknown delivery, or a
    /// journal error without exposing a partial release.
    pub fn release_claim(
        &mut self,
        delivery_id: &str,
        worker: &str,
        released_at: u64,
        next_attempt_at: u64,
    ) -> Result<Delivery, HarborError> {
        let worker = crate::model::required(worker, "worker", 120)?;
        if next_attempt_at < released_at || next_attempt_at > released_at.saturating_add(86_400) {
            return Err(HarborError::Validation(
                "released claim must be due from now through 86400 seconds".into(),
            ));
        }
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::Pending {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} is already terminal"
            )));
        }
        if current.lease_owner.as_deref() != Some(&worker)
            || current
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= released_at)
        {
            return Err(HarborError::Conflict(format!(
                "worker {worker} does not own a live lease for delivery {delivery_id}"
            )));
        }
        let updated = Delivery {
            next_attempt_at,
            lease_owner: None,
            lease_expires_at: None,
            ..current
        };
        self.journal.append(&Record::Delivery(updated.clone()))?;
        self.deliveries.insert(updated.id.clone(), updated.clone());
        Ok(updated)
    }

    /// Records an attempt only when the caller owns a live delivery lease.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a missing, expired, or differently owned lease.
    pub fn record_claimed_attempt(
        &mut self,
        delivery_id: &str,
        worker: &str,
        attempted_at: u64,
        outcome: AttemptOutcome,
    ) -> Result<AttemptResult, HarborError> {
        let worker = crate::model::required(worker, "worker", 120)?;
        self.record_attempt_internal(delivery_id, attempted_at, outcome, Some(&worker))
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
        self.record_attempt_internal(delivery_id, attempted_at, outcome, None)
    }

    fn record_attempt_internal(
        &mut self,
        delivery_id: &str,
        attempted_at: u64,
        outcome: AttemptOutcome,
        required_worker: Option<&str>,
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
        match required_worker {
            Some(worker)
                if current.lease_owner.as_deref() != Some(worker)
                    || current
                        .lease_expires_at
                        .is_none_or(|expires_at| expires_at <= attempted_at) =>
            {
                return Err(HarborError::Conflict(format!(
                    "worker {worker} does not own a live lease for delivery {delivery_id}"
                )));
            }
            None if current.leased(attempted_at) => {
                return Err(HarborError::Conflict(format!(
                    "delivery {delivery_id} has an active worker lease"
                )));
            }
            _ => {}
        }
        let mut route = self.route_for_attempt(&current.route_id, attempted_at)?;
        let previous_route = route.clone();
        let mut updated = current;
        updated.attempts += 1;
        updated.lease_owner = None;
        updated.lease_expires_at = None;
        let persisted_outcome = outcome.clone();
        let (successful, retryable, error) = classify(outcome);
        update_circuit(&mut route, successful, retryable, attempted_at);
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
        let next_sequence = self.attempt_sequence + 1;
        let attempt = DeliveryAttempt {
            id: format!("att-{next_sequence:012}"),
            sequence: next_sequence,
            delivery_id: updated.id.clone(),
            cycle_attempt: updated.attempts,
            attempted_at,
            worker: required_worker.map(str::to_owned),
            outcome: persisted_outcome,
            resulting_state: updated.state,
            retried,
            next_attempt_at: retried.then_some(updated.next_attempt_at),
            error: updated.last_error.clone(),
        };
        let mut records = vec![
            Record::Delivery(updated.clone()),
            Record::Attempt(attempt.clone()),
        ];
        if route != previous_route {
            records.push(Record::Route(route.clone()));
        }
        self.journal.append_batch(&records)?;
        self.deliveries.insert(updated.id.clone(), updated.clone());
        self.routes.insert(route.id.clone(), route);
        self.attempt_sequence = next_sequence;
        self.attempts
            .entry(updated.id.clone())
            .or_default()
            .push(attempt);
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
            lease_owner: None,
            lease_expires_at: None,
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

    fn route_available(&self, route_id: &str, now: u64) -> bool {
        self.routes.get(route_id).is_some_and(|route| {
            route.active
                && route
                    .circuit_open_until
                    .is_none_or(|open_until| open_until <= now)
        })
    }

    fn route_for_attempt(&self, route_id: &str, attempted_at: u64) -> Result<Route, HarborError> {
        let route = self
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| HarborError::Journal(format!("missing route {route_id}")))?;
        if let Some(open_until) = route
            .circuit_open_until
            .filter(|open_until| *open_until > attempted_at)
        {
            return Err(HarborError::Conflict(format!(
                "route {} circuit is open until {open_until}",
                route.id
            )));
        }
        Ok(route)
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
            if delivery.lease_owner.is_some() != delivery.lease_expires_at.is_some() {
                return Err(HarborError::Journal(format!(
                    "delivery {} has an incomplete lease",
                    delivery.id
                )));
            }
            if delivery.state != DeliveryState::Pending && delivery.lease_owner.is_some() {
                return Err(HarborError::Journal(format!(
                    "terminal delivery {} cannot retain a lease",
                    delivery.id
                )));
            }
        }
        for route in self.routes.values() {
            if !(1..=100).contains(&route.failure_threshold) {
                return Err(HarborError::Journal(format!(
                    "route {} has invalid failure threshold",
                    route.id
                )));
            }
            if !(1..=86_400).contains(&route.cooldown_seconds) {
                return Err(HarborError::Journal(format!(
                    "route {} has invalid circuit cooldown",
                    route.id
                )));
            }
        }
        let mut latest_attempt_sequence = 0;
        for (delivery_id, attempts) in &mut self.attempts {
            if !self.deliveries.contains_key(delivery_id) {
                return Err(HarborError::Journal(format!(
                    "attempt history references missing delivery {delivery_id}"
                )));
            }
            attempts.sort_by_key(|attempt| attempt.sequence);
            let mut previous = None;
            for attempt in attempts {
                if attempt.delivery_id != *delivery_id {
                    return Err(HarborError::Journal(format!(
                        "attempt {} is indexed under the wrong delivery",
                        attempt.id
                    )));
                }
                if attempt.sequence == 0 || previous.is_some_and(|value| value >= attempt.sequence)
                {
                    return Err(HarborError::Journal(format!(
                        "attempt sequence is invalid for delivery {delivery_id}"
                    )));
                }
                if attempt.retried != attempt.next_attempt_at.is_some() {
                    return Err(HarborError::Journal(format!(
                        "attempt {} has inconsistent retry scheduling",
                        attempt.id
                    )));
                }
                previous = Some(attempt.sequence);
                latest_attempt_sequence = latest_attempt_sequence.max(attempt.sequence);
            }
        }
        self.event_sequence = self.events.len() as u64;
        self.delivery_sequence = self.deliveries.len() as u64;
        self.attempt_sequence = latest_attempt_sequence;
        Ok(())
    }
}

fn update_circuit(route: &mut Route, successful: bool, retryable: bool, attempted_at: u64) {
    if successful || !retryable {
        route.consecutive_failures = 0;
        route.circuit_open_until = None;
        return;
    }
    route.consecutive_failures = route.consecutive_failures.saturating_add(1);
    if route.consecutive_failures >= route.failure_threshold {
        route.circuit_open_until = Some(attempted_at.saturating_add(route.cooldown_seconds));
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
