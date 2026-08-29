use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::journal::{FileJournal, Record};
use crate::model::{
    AttemptHistorySummary, AttemptOutcome, AttemptResult, BackfillRequest, BackfillResult,
    BulkRedriveResult, CircuitState, Delivery, DeliveryAttempt, DeliveryCancellation, DeliveryHold,
    DeliveryState, EndpointUpdateMode, EndpointUpdateResult, HarborError, HarborSummary,
    IngestResult, Route, RouteCircuitHealth, RouteConcurrencyHealth, RouteMaintenance,
    TenantIngressFreeze, WebhookEvent, WebhookInput,
};

pub struct SignalHarbor {
    journal: FileJournal,
    routes: BTreeMap<String, Route>,
    events: BTreeMap<String, WebhookEvent>,
    deliveries: BTreeMap<String, Delivery>,
    attempts: BTreeMap<String, Vec<DeliveryAttempt>>,
    cancellations: BTreeMap<String, DeliveryCancellation>,
    holds: BTreeMap<String, Vec<DeliveryHold>>,
    maintenance: BTreeMap<String, Vec<RouteMaintenance>>,
    ingress_freezes: BTreeMap<String, Vec<TenantIngressFreeze>>,
    dedupe: HashMap<(String, String), String>,
    event_sequence: u64,
    delivery_sequence: u64,
    attempt_sequence: u64,
    hold_sequence: u64,
    maintenance_sequence: u64,
    ingress_freeze_sequence: u64,
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
            cancellations: BTreeMap::new(),
            holds: BTreeMap::new(),
            maintenance: BTreeMap::new(),
            ingress_freezes: BTreeMap::new(),
            dedupe: HashMap::new(),
            event_sequence: 0,
            delivery_sequence: 0,
            attempt_sequence: 0,
            hold_sequence: 0,
            maintenance_sequence: 0,
            ingress_freeze_sequence: 0,
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
                Record::Cancellation(cancellation) => {
                    harbor
                        .cancellations
                        .insert(cancellation.delivery_id.clone(), cancellation);
                }
                Record::Hold(hold) => harbor.upsert_hold(hold),
                Record::Maintenance(maintenance) => harbor.upsert_maintenance(maintenance),
                Record::IngressFreeze(freeze) => harbor.upsert_ingress_freeze(freeze),
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

    /// Sets a durable upper bound on simultaneous live delivery leases for a route.
    ///
    /// # Errors
    ///
    /// Returns validation for a limit outside 1 through 1000, not-found for an
    /// unknown route, or a journal error when the replacement route cannot persist.
    pub fn configure_route_concurrency(
        &mut self,
        route_id: &str,
        max_in_flight: u16,
        now: u64,
    ) -> Result<RouteConcurrencyHealth, HarborError> {
        if !(1..=1_000).contains(&max_in_flight) {
            return Err(HarborError::Validation(
                "max_in_flight must be from 1 through 1000".into(),
            ));
        }
        let mut route = self
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        route.max_in_flight = max_in_flight;
        self.journal.append(&Record::Route(route.clone()))?;
        self.routes.insert(route.id.clone(), route);
        self.route_concurrency_health(route_id, now)
    }

    /// Reports live leases and remaining concurrency for one route.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] for an unknown route.
    pub fn route_concurrency_health(
        &self,
        route_id: &str,
        now: u64,
    ) -> Result<RouteConcurrencyHealth, HarborError> {
        let route = self
            .routes
            .get(route_id)
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        let leased = self
            .deliveries
            .values()
            .filter(|delivery| delivery.route_id == route_id && delivery.leased(now))
            .count();
        Ok(RouteConcurrencyHealth {
            route_id: route.id.clone(),
            max_in_flight: route.max_in_flight,
            leased,
            available: usize::from(route.max_in_flight).saturating_sub(leased),
        })
    }

    /// Schedules a bounded maintenance window that pauses claims for one route.
    ///
    /// The window may start immediately or within 30 days, can last at most one
    /// day, and cannot overlap another uncancelled window for the same route.
    /// Existing delivery state is unchanged; due work becomes available again
    /// automatically when the window ends.
    ///
    /// # Errors
    ///
    /// Returns validation for bad timing or operator context, conflict for an
    /// overlapping window, not-found for an unknown route, or a journal error
    /// without exposing an unpersisted schedule.
    pub fn schedule_route_maintenance(
        &mut self,
        route_id: &str,
        starts_at: u64,
        ends_at: u64,
        actor: &str,
        reason: &str,
        scheduled_at: u64,
    ) -> Result<RouteMaintenance, HarborError> {
        if !self.routes.contains_key(route_id) {
            return Err(HarborError::NotFound(format!("route {route_id}")));
        }
        let actor = crate::model::required(actor, "maintenance scheduler", 120)?;
        let reason = crate::model::required(reason, "maintenance reason", 1_000)?;
        if starts_at < scheduled_at
            || ends_at <= starts_at
            || ends_at > starts_at.saturating_add(86_400)
            || starts_at > scheduled_at.saturating_add(2_592_000)
        {
            return Err(HarborError::Validation(
                "maintenance must start within 30 days and last from 1 through 86400 seconds"
                    .into(),
            ));
        }
        if self.maintenance.get(route_id).is_some_and(|windows| {
            windows
                .iter()
                .any(|window| window.overlaps(starts_at, ends_at))
        }) {
            return Err(HarborError::Conflict(format!(
                "route {route_id} already has overlapping maintenance"
            )));
        }
        let sequence = self
            .maintenance_sequence
            .checked_add(1)
            .ok_or_else(|| HarborError::Conflict("maintenance sequence is exhausted".into()))?;
        let maintenance = RouteMaintenance {
            id: format!("maintenance-{sequence}"),
            sequence,
            route_id: route_id.to_owned(),
            starts_at,
            ends_at,
            scheduled_by: actor,
            reason,
            scheduled_at,
            cancelled_at: None,
            cancelled_by: None,
            cancellation_reason: None,
        };
        self.journal
            .append(&Record::Maintenance(maintenance.clone()))?;
        self.maintenance_sequence = sequence;
        self.upsert_maintenance(maintenance.clone());
        Ok(maintenance)
    }

    /// Cancels a future or active maintenance window with accountable context.
    ///
    /// # Errors
    ///
    /// Returns not-found for an unknown route or window, conflict when the
    /// window is already cancelled or ended, validation for missing context or
    /// an impossible cancellation time, or a journal error without mutation.
    pub fn cancel_route_maintenance(
        &mut self,
        route_id: &str,
        maintenance_id: &str,
        actor: &str,
        reason: &str,
        cancelled_at: u64,
    ) -> Result<RouteMaintenance, HarborError> {
        if !self.routes.contains_key(route_id) {
            return Err(HarborError::NotFound(format!("route {route_id}")));
        }
        let actor = crate::model::required(actor, "maintenance cancellation actor", 120)?;
        let reason = crate::model::required(reason, "maintenance cancellation reason", 1_000)?;
        let mut maintenance = self
            .maintenance
            .get(route_id)
            .and_then(|windows| windows.iter().find(|window| window.id == maintenance_id))
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("maintenance {maintenance_id}")))?;
        if maintenance.cancelled_at.is_some() {
            return Err(HarborError::Conflict(format!(
                "maintenance {maintenance_id} is already cancelled"
            )));
        }
        if cancelled_at < maintenance.scheduled_at {
            return Err(HarborError::Validation(
                "maintenance cancellation cannot predate scheduling".into(),
            ));
        }
        if cancelled_at >= maintenance.ends_at {
            return Err(HarborError::Conflict(format!(
                "maintenance {maintenance_id} has already ended"
            )));
        }
        maintenance.cancelled_at = Some(cancelled_at);
        maintenance.cancelled_by = Some(actor);
        maintenance.cancellation_reason = Some(reason);
        self.journal
            .append(&Record::Maintenance(maintenance.clone()))?;
        self.upsert_maintenance(maintenance.clone());
        Ok(maintenance)
    }

    /// Returns durable maintenance history in start and sequence order.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::NotFound`] for an unknown route.
    pub fn route_maintenance(&self, route_id: &str) -> Result<Vec<RouteMaintenance>, HarborError> {
        if !self.routes.contains_key(route_id) {
            return Err(HarborError::NotFound(format!("route {route_id}")));
        }
        let mut windows = self.maintenance.get(route_id).cloned().unwrap_or_default();
        windows.sort_by_key(|window| (window.starts_at, window.sequence));
        Ok(windows)
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

    /// Schedules a bounded tenant-level ingress freeze.
    ///
    /// The tenant must already own at least one registered route. Freeze
    /// intervals cannot overlap, may be scheduled at most 30 days ahead, and
    /// expire automatically within seven days. Persistence happens before the
    /// new control becomes visible in memory.
    ///
    /// # Errors
    ///
    /// Returns validation for invalid context or timing, not-found for an
    /// unknown tenant, conflict for overlapping controls or an exhausted id
    /// sequence, and journal errors without changing visible state.
    pub fn impose_ingress_freeze(
        &mut self,
        tenant: &str,
        starts_at: u64,
        expires_at: u64,
        actor: &str,
        reason: &str,
        imposed_at: u64,
    ) -> Result<TenantIngressFreeze, HarborError> {
        let tenant = crate::model::required(tenant, "tenant", 80)?.to_ascii_lowercase();
        let actor = crate::model::required(actor, "ingress freeze actor", 120)?;
        let reason = crate::model::required(reason, "ingress freeze reason", 1_000)?;
        if !self.routes.values().any(|route| route.tenant == tenant) {
            return Err(HarborError::NotFound(format!("tenant {tenant}")));
        }
        if starts_at < imposed_at
            || starts_at > imposed_at.saturating_add(2_592_000)
            || expires_at <= starts_at
            || expires_at > starts_at.saturating_add(604_800)
        {
            return Err(HarborError::Validation(
                "ingress freeze must start within 30 days and last from 1 second through 7 days"
                    .into(),
            ));
        }
        if self.ingress_freezes.get(&tenant).is_some_and(|freezes| {
            freezes
                .iter()
                .any(|freeze| freeze.overlaps(starts_at, expires_at))
        }) {
            return Err(HarborError::Conflict(format!(
                "tenant {tenant} already has an overlapping ingress freeze"
            )));
        }
        let sequence = self
            .ingress_freeze_sequence
            .checked_add(1)
            .ok_or_else(|| HarborError::Conflict("ingress freeze sequence is exhausted".into()))?;
        let freeze = TenantIngressFreeze {
            id: format!("freeze-{sequence:012}"),
            sequence,
            tenant,
            starts_at,
            expires_at,
            imposed_by: actor,
            reason,
            imposed_at,
            lifted_at: None,
            lifted_by: None,
            lift_reason: None,
        };
        self.journal
            .append(&Record::IngressFreeze(freeze.clone()))?;
        self.ingress_freeze_sequence = sequence;
        self.upsert_ingress_freeze(freeze.clone());
        Ok(freeze)
    }

    /// Lifts a future or active ingress freeze with durable operator context.
    ///
    /// # Errors
    ///
    /// Returns not-found for an unknown tenant or freeze, conflict for an
    /// already lifted or expired freeze, validation for incomplete context or
    /// an impossible timestamp, and journal errors without visible mutation.
    pub fn lift_ingress_freeze(
        &mut self,
        tenant: &str,
        freeze_id: &str,
        actor: &str,
        reason: &str,
        lifted_at: u64,
    ) -> Result<TenantIngressFreeze, HarborError> {
        let tenant = crate::model::required(tenant, "tenant", 80)?.to_ascii_lowercase();
        let actor = crate::model::required(actor, "ingress freeze lift actor", 120)?;
        let reason = crate::model::required(reason, "ingress freeze lift reason", 1_000)?;
        let mut freeze = self
            .ingress_freezes
            .get(&tenant)
            .and_then(|freezes| freezes.iter().find(|freeze| freeze.id == freeze_id))
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("ingress freeze {freeze_id}")))?;
        if freeze.lifted_at.is_some() {
            return Err(HarborError::Conflict(format!(
                "ingress freeze {freeze_id} is already lifted"
            )));
        }
        if lifted_at < freeze.imposed_at {
            return Err(HarborError::Validation(
                "ingress freeze lift cannot predate imposition".into(),
            ));
        }
        if lifted_at >= freeze.expires_at {
            return Err(HarborError::Conflict(format!(
                "ingress freeze {freeze_id} has already expired"
            )));
        }
        freeze.lifted_at = Some(lifted_at);
        freeze.lifted_by = Some(actor);
        freeze.lift_reason = Some(reason);
        self.journal
            .append(&Record::IngressFreeze(freeze.clone()))?;
        self.upsert_ingress_freeze(freeze.clone());
        Ok(freeze)
    }

    /// Returns deterministic ingress-freeze history for a registered tenant.
    ///
    /// # Errors
    ///
    /// Returns not-found when the tenant has no registered route.
    pub fn tenant_ingress_freezes(
        &self,
        tenant: &str,
    ) -> Result<Vec<TenantIngressFreeze>, HarborError> {
        let tenant = tenant.to_ascii_lowercase();
        if !self.routes.values().any(|route| route.tenant == tenant) {
            return Err(HarborError::NotFound(format!("tenant {tenant}")));
        }
        let mut freezes = self
            .ingress_freezes
            .get(&tenant)
            .cloned()
            .unwrap_or_default();
        freezes.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.sequence.cmp(&right.sequence))
        });
        Ok(freezes)
    }

    /// Lists currently active tenant freezes by expiry and identifier.
    #[must_use]
    pub fn active_ingress_freezes(&self, now: u64) -> Vec<TenantIngressFreeze> {
        let mut active = self
            .ingress_freezes
            .values()
            .flatten()
            .filter(|freeze| freeze.active_at(now))
            .cloned()
            .collect::<Vec<_>>();
        active.sort_by(|left, right| {
            left.expires_at
                .cmp(&right.expires_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        active
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

        if let Some(freeze) = self.active_ingress_freeze(&input.tenant, input.received_at) {
            return Err(HarborError::Conflict(format!(
                "tenant {} ingress is frozen until {} by {}",
                input.tenant, freeze.expires_at, freeze.id
            )));
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

    /// Cancels pending delivery work while retaining a durable operator record.
    ///
    /// A delivery with a live worker lease cannot be cancelled. Expired claims
    /// are cleared atomically with the terminal snapshot and cancellation
    /// metadata, so restart never exposes a cancelled delivery without its
    /// accountable reason.
    ///
    /// # Errors
    ///
    /// Returns not-found for an unknown delivery, conflict for terminal or
    /// actively leased work, validation for missing operator context, or a
    /// journal error without changing in-memory state.
    pub fn cancel_delivery(
        &mut self,
        delivery_id: &str,
        actor: &str,
        reason: &str,
        cancelled_at: u64,
    ) -> Result<DeliveryCancellation, HarborError> {
        let actor = crate::model::required(actor, "cancellation actor", 120)?;
        let reason = crate::model::required(reason, "cancellation reason", 1_000)?;
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::Pending {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} is already terminal"
            )));
        }
        if current.leased(cancelled_at) {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} has an active worker lease"
            )));
        }
        if self.cancellations.contains_key(delivery_id) {
            return Err(HarborError::Journal(format!(
                "delivery {delivery_id} already has cancellation metadata"
            )));
        }
        let updated = Delivery {
            state: DeliveryState::Cancelled,
            lease_owner: None,
            lease_expires_at: None,
            ..current
        };
        let cancellation = DeliveryCancellation {
            id: format!("cancel-{delivery_id}"),
            delivery_id: delivery_id.to_owned(),
            actor,
            reason,
            cancelled_at,
        };
        self.journal.append_batch(&[
            Record::Delivery(updated.clone()),
            Record::Cancellation(cancellation.clone()),
        ])?;
        self.deliveries.insert(updated.id.clone(), updated);
        self.cancellations
            .insert(delivery_id.to_owned(), cancellation.clone());
        Ok(cancellation)
    }

    /// Returns the durable cancellation metadata for one delivery.
    ///
    /// # Errors
    ///
    /// Returns not-found when the delivery exists but has not been cancelled,
    /// or when the delivery id is unknown.
    pub fn cancellation(&self, delivery_id: &str) -> Result<DeliveryCancellation, HarborError> {
        if !self.deliveries.contains_key(delivery_id) {
            return Err(HarborError::NotFound(format!("delivery {delivery_id}")));
        }
        self.cancellations
            .get(delivery_id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("cancellation for {delivery_id}")))
    }

    /// Places a bounded, durable operator hold on pending delivery work.
    ///
    /// A live worker lease prevents a hold. An expired lease is cleared in the
    /// same journal transaction, and the hold automatically stops applying at
    /// `release_at` even when nobody explicitly releases it.
    ///
    /// # Errors
    ///
    /// Returns validation for missing context or an invalid interval, conflict
    /// for terminal, leased, or already-held work, not-found for an unknown
    /// delivery, or a journal error without changing in-memory state.
    pub fn hold_delivery(
        &mut self,
        delivery_id: &str,
        actor: &str,
        reason: &str,
        held_at: u64,
        release_at: u64,
    ) -> Result<DeliveryHold, HarborError> {
        let actor = crate::model::required(actor, "hold actor", 120)?;
        let reason = crate::model::required(reason, "hold reason", 1_000)?;
        if release_at <= held_at || release_at > held_at.saturating_add(604_800) {
            return Err(HarborError::Validation(
                "hold release_at must be after held_at and within 7 days".into(),
            ));
        }
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::Pending {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} is already terminal"
            )));
        }
        if current.leased(held_at) {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} has an active worker lease"
            )));
        }
        if self.active_hold(delivery_id, held_at).is_some() {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} already has an active hold"
            )));
        }
        let sequence = self
            .hold_sequence
            .checked_add(1)
            .ok_or_else(|| HarborError::Conflict("delivery hold sequence is exhausted".into()))?;
        let hold = DeliveryHold {
            id: format!("hold-{sequence}"),
            sequence,
            delivery_id: delivery_id.to_owned(),
            actor,
            reason,
            held_at,
            release_at,
            released_at: None,
            released_by: None,
        };
        let updated = Delivery {
            lease_owner: None,
            lease_expires_at: None,
            ..current
        };
        self.journal.append_batch(&[
            Record::Delivery(updated.clone()),
            Record::Hold(hold.clone()),
        ])?;
        self.deliveries.insert(updated.id.clone(), updated);
        self.hold_sequence = sequence;
        self.upsert_hold(hold.clone());
        Ok(hold)
    }

    /// Explicitly releases the currently active hold and preserves accountability.
    ///
    /// # Errors
    ///
    /// Returns validation for a missing actor, not-found for an unknown
    /// delivery or active hold, and a journal error without a partial release.
    pub fn release_delivery_hold(
        &mut self,
        delivery_id: &str,
        actor: &str,
        released_at: u64,
    ) -> Result<DeliveryHold, HarborError> {
        let actor = crate::model::required(actor, "hold release actor", 120)?;
        if !self.deliveries.contains_key(delivery_id) {
            return Err(HarborError::NotFound(format!("delivery {delivery_id}")));
        }
        let mut hold = self
            .active_hold(delivery_id, released_at)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("active hold for {delivery_id}")))?;
        hold.released_at = Some(released_at);
        hold.released_by = Some(actor);
        self.journal.append(&Record::Hold(hold.clone()))?;
        self.upsert_hold(hold.clone());
        Ok(hold)
    }

    /// Returns every hold for one delivery in durable sequence order.
    ///
    /// # Errors
    ///
    /// Returns not-found when the delivery does not exist.
    pub fn hold_history(&self, delivery_id: &str) -> Result<Vec<DeliveryHold>, HarborError> {
        if !self.deliveries.contains_key(delivery_id) {
            return Err(HarborError::NotFound(format!("delivery {delivery_id}")));
        }
        Ok(self.holds.get(delivery_id).cloned().unwrap_or_default())
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
            .filter(|delivery| {
                delivery.due(now)
                    && !self.delivery_held(&delivery.id, now)
                    && self.route_available(&delivery.route_id, now)
            })
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
            .filter(|delivery| {
                delivery.due(now)
                    && !self.delivery_held(&delivery.id, now)
                    && self.route_available(&delivery.route_id, now)
            })
            .cloned()
            .collect::<Vec<_>>();
        available.sort_by(|left, right| {
            (left.next_attempt_at, &left.id).cmp(&(right.next_attempt_at, &right.id))
        });
        let mut capacity = self
            .routes
            .values()
            .map(|route| {
                let leased = self
                    .deliveries
                    .values()
                    .filter(|delivery| delivery.route_id == route.id && delivery.leased(now))
                    .count();
                (
                    route.id.clone(),
                    usize::from(route.max_in_flight).saturating_sub(leased),
                )
            })
            .collect::<HashMap<_, _>>();
        available.retain(|delivery| {
            let remaining = capacity.entry(delivery.route_id.clone()).or_default();
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
            true
        });
        available.truncate(limit);
        let expires_at = now.checked_add(lease_seconds).ok_or_else(|| {
            HarborError::Validation("lease expiry exceeds timestamp range".into())
        })?;
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
        update_circuit(&mut route, successful, retryable, attempted_at)?;
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
            updated.next_attempt_at = attempted_at.checked_add(delay).ok_or_else(|| {
                HarborError::Validation("retry due time exceeds timestamp range".into())
            })?;
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
        if self
            .attempts
            .get(delivery_id)
            .and_then(|history| history.last())
            .is_some_and(|attempt| due_at < attempt.attempted_at)
        {
            return Err(HarborError::Validation(
                "redrive due time cannot precede attempt history".into(),
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

    /// Resets a bounded, deterministic batch of one route's dead letters.
    ///
    /// Delivery identifiers define stable selection order. The complete batch
    /// is journaled as one transaction before any in-memory delivery changes,
    /// preventing a restart from exposing a partially redriven operator batch.
    /// Existing attempt history is retained while the delivery cycle resets.
    ///
    /// # Errors
    ///
    /// Returns validation for a limit outside 1 through 1,000, not-found for an
    /// unknown route, conflict for an inactive route, or a journal error without
    /// mutating the visible batch.
    pub fn redrive_route_dead_letters(
        &mut self,
        route_id: &str,
        due_at: u64,
        limit: usize,
    ) -> Result<BulkRedriveResult, HarborError> {
        if !(1..=1_000).contains(&limit) {
            return Err(HarborError::Validation(
                "bulk redrive limit must be from 1 through 1000".into(),
            ));
        }
        let route = self
            .routes
            .get(route_id)
            .ok_or_else(|| HarborError::NotFound(format!("route {route_id}")))?;
        if !route.active {
            return Err(HarborError::Conflict(format!(
                "cannot redrive while route {route_id} is inactive"
            )));
        }
        let mut candidates = self
            .deliveries
            .values()
            .filter(|delivery| {
                delivery.route_id == route_id && delivery.state == DeliveryState::DeadLetter
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.id.cmp(&right.id));
        let total = candidates.len();
        candidates.truncate(limit);
        if candidates.iter().any(|delivery| {
            self.attempts
                .get(&delivery.id)
                .and_then(|history| history.last())
                .is_some_and(|attempt| due_at < attempt.attempted_at)
        }) {
            return Err(HarborError::Validation(
                "bulk redrive due time cannot precede attempt history".into(),
            ));
        }
        let updated = candidates
            .into_iter()
            .map(|delivery| Delivery {
                state: DeliveryState::Pending,
                attempts: 0,
                next_attempt_at: due_at,
                last_error: None,
                lease_owner: None,
                lease_expires_at: None,
                ..delivery
            })
            .collect::<Vec<_>>();
        let records = updated
            .iter()
            .cloned()
            .map(Record::Delivery)
            .collect::<Vec<_>>();
        self.journal.append_batch(&records)?;
        let redriven_delivery_ids = updated
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        for delivery in updated {
            self.deliveries.insert(delivery.id.clone(), delivery);
        }
        let remaining_dead_letters = total.saturating_sub(redriven_delivery_ids.len());
        Ok(BulkRedriveResult {
            route_id: route_id.to_owned(),
            redriven_delivery_ids,
            remaining_dead_letters,
            truncated: total > limit,
        })
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
            cancelled: self
                .deliveries
                .values()
                .filter(|row| row.state == DeliveryState::Cancelled)
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
                && !self.route_under_maintenance(route_id, now)
        })
    }

    fn route_under_maintenance(&self, route_id: &str, now: u64) -> bool {
        self.maintenance
            .get(route_id)
            .is_some_and(|windows| windows.iter().any(|window| window.active_at(now)))
    }

    fn active_hold(&self, delivery_id: &str, now: u64) -> Option<&DeliveryHold> {
        self.holds
            .get(delivery_id)
            .and_then(|holds| holds.iter().rev().find(|hold| hold.active_at(now)))
    }

    fn delivery_held(&self, delivery_id: &str, now: u64) -> bool {
        self.active_hold(delivery_id, now).is_some()
    }

    fn upsert_hold(&mut self, hold: DeliveryHold) {
        let holds = self.holds.entry(hold.delivery_id.clone()).or_default();
        if let Some(existing) = holds.iter_mut().find(|existing| existing.id == hold.id) {
            *existing = hold;
        } else {
            holds.push(hold);
            holds.sort_by_key(|item| item.sequence);
        }
    }

    fn upsert_maintenance(&mut self, maintenance: RouteMaintenance) {
        let windows = self
            .maintenance
            .entry(maintenance.route_id.clone())
            .or_default();
        if let Some(existing) = windows
            .iter_mut()
            .find(|existing| existing.id == maintenance.id)
        {
            *existing = maintenance;
        } else {
            windows.push(maintenance);
            windows.sort_by_key(|window| window.sequence);
        }
    }

    fn active_ingress_freeze(&self, tenant: &str, now: u64) -> Option<&TenantIngressFreeze> {
        self.ingress_freezes
            .get(&tenant.to_ascii_lowercase())
            .and_then(|freezes| freezes.iter().find(|freeze| freeze.active_at(now)))
    }

    fn upsert_ingress_freeze(&mut self, freeze: TenantIngressFreeze) {
        let freezes = self
            .ingress_freezes
            .entry(freeze.tenant.clone())
            .or_default();
        if let Some(existing) = freezes.iter_mut().find(|existing| existing.id == freeze.id) {
            *existing = freeze;
        } else {
            freezes.push(freeze);
            freezes.sort_by_key(|item| item.sequence);
        }
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
        if let Some(window) = self
            .maintenance
            .get(route_id)
            .and_then(|windows| windows.iter().find(|window| window.active_at(attempted_at)))
        {
            return Err(HarborError::Conflict(format!(
                "route {} is in maintenance until {}",
                route.id, window.ends_at
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
        self.validate_cancellations()?;
        self.validate_holds()?;
        self.validate_maintenance()?;
        self.validate_ingress_freezes()?;
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
            if !(1..=1_000).contains(&route.max_in_flight) {
                return Err(HarborError::Journal(format!(
                    "route {} has invalid concurrency limit",
                    route.id
                )));
            }
        }
        let latest_attempt_sequence = self.validate_attempts()?;
        self.event_sequence = self.events.len() as u64;
        self.delivery_sequence = self.deliveries.len() as u64;
        self.attempt_sequence = latest_attempt_sequence;
        self.hold_sequence = self
            .holds
            .values()
            .flat_map(|holds| holds.iter().map(|hold| hold.sequence))
            .max()
            .unwrap_or(0);
        self.maintenance_sequence = self
            .maintenance
            .values()
            .flat_map(|windows| windows.iter().map(|window| window.sequence))
            .max()
            .unwrap_or(0);
        self.ingress_freeze_sequence = self
            .ingress_freezes
            .values()
            .flat_map(|freezes| freezes.iter().map(|freeze| freeze.sequence))
            .max()
            .unwrap_or(0);
        Ok(())
    }

    fn validate_attempts(&mut self) -> Result<u64, HarborError> {
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
        Ok(latest_attempt_sequence)
    }

    fn validate_cancellations(&self) -> Result<(), HarborError> {
        for delivery in self.deliveries.values() {
            if delivery.state == DeliveryState::Cancelled
                && !self.cancellations.contains_key(&delivery.id)
            {
                return Err(HarborError::Journal(format!(
                    "cancelled delivery {} is missing cancellation metadata",
                    delivery.id
                )));
            }
        }
        for cancellation in self.cancellations.values() {
            let delivery = self
                .deliveries
                .get(&cancellation.delivery_id)
                .ok_or_else(|| {
                    HarborError::Journal(format!(
                        "cancellation {} references missing delivery {}",
                        cancellation.id, cancellation.delivery_id
                    ))
                })?;
            if delivery.state != DeliveryState::Cancelled {
                return Err(HarborError::Journal(format!(
                    "cancellation {} references non-cancelled delivery {}",
                    cancellation.id, cancellation.delivery_id
                )));
            }
        }
        Ok(())
    }

    fn validate_holds(&self) -> Result<(), HarborError> {
        let mut sequences = std::collections::BTreeSet::new();
        for (delivery_id, holds) in &self.holds {
            if !self.deliveries.contains_key(delivery_id) {
                return Err(HarborError::Journal(format!(
                    "hold history references missing delivery {delivery_id}"
                )));
            }
            let mut active_intervals: Vec<(u64, u64)> = Vec::new();
            for hold in holds {
                if hold.delivery_id != *delivery_id || hold.sequence == 0 {
                    return Err(HarborError::Journal(format!(
                        "hold {} has invalid identity or sequence",
                        hold.id
                    )));
                }
                if !sequences.insert(hold.sequence) {
                    return Err(HarborError::Journal(format!(
                        "duplicate hold sequence {}",
                        hold.sequence
                    )));
                }
                if hold.release_at <= hold.held_at
                    || hold.release_at > hold.held_at.saturating_add(604_800)
                {
                    return Err(HarborError::Journal(format!(
                        "hold {} has an invalid interval",
                        hold.id
                    )));
                }
                if hold.released_at.is_some() != hold.released_by.is_some()
                    || hold.released_at.is_some_and(|released_at| {
                        released_at < hold.held_at || released_at >= hold.release_at
                    })
                {
                    return Err(HarborError::Journal(format!(
                        "hold {} has invalid release metadata",
                        hold.id
                    )));
                }
                let end = hold.released_at.unwrap_or(hold.release_at);
                if active_intervals
                    .iter()
                    .any(|(start, existing_end)| hold.held_at < *existing_end && *start < end)
                {
                    return Err(HarborError::Journal(format!(
                        "delivery {delivery_id} has overlapping hold intervals"
                    )));
                }
                active_intervals.push((hold.held_at, end));
            }
        }
        Ok(())
    }

    fn validate_maintenance(&self) -> Result<(), HarborError> {
        let mut sequences = std::collections::BTreeSet::new();
        for (route_id, windows) in &self.maintenance {
            if !self.routes.contains_key(route_id) {
                return Err(HarborError::Journal(format!(
                    "maintenance history references missing route {route_id}"
                )));
            }
            let mut active_intervals: Vec<(u64, u64)> = Vec::new();
            for window in windows {
                if window.route_id != *route_id || window.sequence == 0 {
                    return Err(HarborError::Journal(format!(
                        "maintenance {} has invalid identity or sequence",
                        window.id
                    )));
                }
                if !sequences.insert(window.sequence) {
                    return Err(HarborError::Journal(format!(
                        "duplicate maintenance sequence {}",
                        window.sequence
                    )));
                }
                if window.starts_at < window.scheduled_at
                    || window.ends_at <= window.starts_at
                    || window.ends_at > window.starts_at.saturating_add(86_400)
                    || window.starts_at > window.scheduled_at.saturating_add(2_592_000)
                {
                    return Err(HarborError::Journal(format!(
                        "maintenance {} has an invalid interval",
                        window.id
                    )));
                }
                let cancellation_fields = [
                    window.cancelled_at.is_some(),
                    window.cancelled_by.is_some(),
                    window.cancellation_reason.is_some(),
                ];
                if cancellation_fields.iter().any(|present| *present)
                    && !cancellation_fields.iter().all(|present| *present)
                {
                    return Err(HarborError::Journal(format!(
                        "maintenance {} has incomplete cancellation metadata",
                        window.id
                    )));
                }
                if window.cancelled_at.is_some_and(|cancelled_at| {
                    cancelled_at < window.scheduled_at || cancelled_at >= window.ends_at
                }) {
                    return Err(HarborError::Journal(format!(
                        "maintenance {} has an invalid cancellation time",
                        window.id
                    )));
                }
                if window.cancelled_at.is_none()
                    && active_intervals
                        .iter()
                        .any(|(start, end)| window.starts_at < *end && *start < window.ends_at)
                {
                    return Err(HarborError::Journal(format!(
                        "route {route_id} has overlapping maintenance windows"
                    )));
                }
                if window.cancelled_at.is_none() {
                    active_intervals.push((window.starts_at, window.ends_at));
                }
            }
        }
        Ok(())
    }

    fn validate_ingress_freezes(&self) -> Result<(), HarborError> {
        let mut sequences = std::collections::BTreeSet::new();
        for (tenant, freezes) in &self.ingress_freezes {
            if !self.routes.values().any(|route| route.tenant == *tenant) {
                return Err(HarborError::Journal(format!(
                    "ingress freeze history references unknown tenant {tenant}"
                )));
            }
            let mut active_intervals: Vec<(u64, u64)> = Vec::new();
            for freeze in freezes {
                if freeze.tenant != *tenant || freeze.sequence == 0 {
                    return Err(HarborError::Journal(format!(
                        "ingress freeze {} has invalid identity or sequence",
                        freeze.id
                    )));
                }
                if !sequences.insert(freeze.sequence) {
                    return Err(HarborError::Journal(format!(
                        "duplicate ingress freeze sequence {}",
                        freeze.sequence
                    )));
                }
                if freeze.starts_at < freeze.imposed_at
                    || freeze.starts_at > freeze.imposed_at.saturating_add(2_592_000)
                    || freeze.expires_at <= freeze.starts_at
                    || freeze.expires_at > freeze.starts_at.saturating_add(604_800)
                {
                    return Err(HarborError::Journal(format!(
                        "ingress freeze {} has an invalid interval",
                        freeze.id
                    )));
                }
                let lift_fields = [
                    freeze.lifted_at.is_some(),
                    freeze.lifted_by.is_some(),
                    freeze.lift_reason.is_some(),
                ];
                if lift_fields.iter().any(|present| *present)
                    && !lift_fields.iter().all(|present| *present)
                {
                    return Err(HarborError::Journal(format!(
                        "ingress freeze {} has incomplete lift metadata",
                        freeze.id
                    )));
                }
                if freeze.lifted_at.is_some_and(|lifted_at| {
                    lifted_at < freeze.imposed_at || lifted_at >= freeze.expires_at
                }) {
                    return Err(HarborError::Journal(format!(
                        "ingress freeze {} has an invalid lift time",
                        freeze.id
                    )));
                }
                let end = freeze.effective_end();
                if active_intervals
                    .iter()
                    .any(|(start, existing_end)| freeze.starts_at < *existing_end && *start < end)
                {
                    return Err(HarborError::Journal(format!(
                        "tenant {tenant} has overlapping ingress freezes"
                    )));
                }
                active_intervals.push((freeze.starts_at, end));
            }
        }
        Ok(())
    }
}

fn update_circuit(
    route: &mut Route,
    successful: bool,
    retryable: bool,
    attempted_at: u64,
) -> Result<(), HarborError> {
    if successful || !retryable {
        route.consecutive_failures = 0;
        route.circuit_open_until = None;
        return Ok(());
    }
    route.consecutive_failures = route.consecutive_failures.saturating_add(1);
    if route.consecutive_failures >= route.failure_threshold {
        route.circuit_open_until = Some(
            attempted_at
                .checked_add(route.cooldown_seconds)
                .ok_or_else(|| {
                    HarborError::Validation("circuit cooldown exceeds timestamp range".into())
                })?,
        );
    }
    Ok(())
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
