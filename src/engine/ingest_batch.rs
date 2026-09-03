use std::collections::HashMap;

use super::SignalHarbor;
use crate::journal::Record;
use crate::{Delivery, DeliveryState, HarborError, IngestResult, WebhookEvent, WebhookInput};

const MAX_EVENTS: usize = 1_000;
const MAX_BODY_BYTES: usize = 16 * 1_048_576;
const MAX_DELIVERIES: usize = 10_000;

impl SignalHarbor {
    /// Accepts a provider batch in one durable transaction, preserving input order.
    ///
    /// At most 1,000 inputs, 16 MiB of aggregate body data and 10,000 newly
    /// routed deliveries are accepted. An empty batch is a no-op. Duplicate
    /// keys are reconciled against both retained events and earlier batch
    /// members; matching duplicates do not consume identifiers or reroute.
    /// Every input is revalidated, including directly constructed public values.
    ///
    /// # Errors
    ///
    /// Returns validation for malformed inputs or exceeded batch bounds,
    /// conflict for changed duplicate content, frozen new ingress or exhausted
    /// identifiers, and journal errors on persistence failure. No in-memory
    /// changes or validation-side writes occur unless the entire batch succeeds.
    pub fn ingest_batch(
        &mut self,
        inputs: &[WebhookInput],
    ) -> Result<Vec<IngestResult>, HarborError> {
        validate_batch_size(inputs)?;
        let normalized = inputs
            .iter()
            .map(|input| {
                WebhookInput::new(
                    &input.tenant,
                    &input.topic,
                    &input.dedupe_key,
                    input.body.clone(),
                    input.received_at,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut plan = BatchPlan {
            events: Vec::new(),
            deliveries: Vec::new(),
            results: Vec::with_capacity(inputs.len()),
            staged: HashMap::new(),
            event_sequence: self.event_sequence,
            delivery_sequence: self.delivery_sequence,
        };
        for input in normalized {
            plan.accept(self, input)?;
        }
        let mut records = plan
            .events
            .iter()
            .cloned()
            .map(Record::Event)
            .collect::<Vec<_>>();
        records.extend(plan.deliveries.iter().cloned().map(Record::Delivery));
        self.journal.append_batch(&records)?;
        self.event_sequence = plan.event_sequence;
        self.delivery_sequence = plan.delivery_sequence;
        for event in plan.events {
            self.dedupe.insert(
                (event.tenant.clone(), event.dedupe_key.clone()),
                event.id.clone(),
            );
            self.events.insert(event.id.clone(), event);
        }
        for delivery in plan.deliveries {
            self.deliveries.insert(delivery.id.clone(), delivery);
        }
        Ok(plan.results)
    }
}

struct BatchPlan {
    events: Vec<WebhookEvent>,
    deliveries: Vec<Delivery>,
    results: Vec<IngestResult>,
    staged: HashMap<(String, String), usize>,
    event_sequence: u64,
    delivery_sequence: u64,
}

impl BatchPlan {
    fn accept(&mut self, harbor: &SignalHarbor, input: WebhookInput) -> Result<(), HarborError> {
        let key = (input.tenant.clone(), input.dedupe_key.clone());
        if let Some(index) = self.staged.get(&key) {
            let previous = &self.results[*index];
            require_matching_content(&previous.event, &input)?;
            self.results.push(IngestResult {
                duplicate: true,
                ..previous.clone()
            });
            return Ok(());
        }
        if let Some(id) = harbor.dedupe.get(&key) {
            let event = harbor.events.get(id).ok_or_else(|| {
                HarborError::Journal("dedupe index references a missing event".into())
            })?;
            require_matching_content(event, &input)?;
            self.results.push(IngestResult {
                event: event.clone(),
                duplicate: true,
                delivery_ids: harbor.delivery_ids_for_event(id),
            });
            return Ok(());
        }
        if let Some(freeze) = harbor.active_ingress_freeze(&input.tenant, input.received_at) {
            return Err(HarborError::Conflict(format!(
                "tenant {} ingress is frozen until {} by {}",
                input.tenant, freeze.expires_at, freeze.id
            )));
        }
        self.event_sequence = next_identifier(self.event_sequence, "event")?;
        let event = WebhookEvent {
            id: format!("evt-{:012}", self.event_sequence),
            tenant: input.tenant,
            topic: input.topic,
            dedupe_key: input.dedupe_key,
            body: input.body,
            received_at: input.received_at,
        };
        let mut delivery_ids = Vec::new();
        for route in harbor
            .routes
            .values()
            .filter(|route| route.matches(&event.tenant, &event.topic))
        {
            if self.deliveries.len() == MAX_DELIVERIES {
                return Err(HarborError::Validation(
                    "ingest batch exceeds 10000 new deliveries".into(),
                ));
            }
            self.delivery_sequence = next_identifier(self.delivery_sequence, "delivery")?;
            let id = format!("del-{:012}", self.delivery_sequence);
            delivery_ids.push(id.clone());
            self.deliveries.push(Delivery {
                id,
                event_id: event.id.clone(),
                route_id: route.id.clone(),
                endpoint: route.endpoint.clone(),
                state: DeliveryState::Pending,
                attempts: 0,
                next_attempt_at: event.received_at,
                last_error: None,
                lease_owner: None,
                lease_expires_at: None,
            });
        }
        self.staged.insert(key, self.results.len());
        self.events.push(event.clone());
        self.results.push(IngestResult {
            event,
            duplicate: false,
            delivery_ids,
        });
        Ok(())
    }
}

fn validate_batch_size(inputs: &[WebhookInput]) -> Result<(), HarborError> {
    if inputs.len() > MAX_EVENTS {
        return Err(HarborError::Validation(
            "ingest batch exceeds 1000 inputs".into(),
        ));
    }
    let total = inputs
        .iter()
        .try_fold(0_usize, |total, input| total.checked_add(input.body.len()));
    if total.is_none_or(|bytes| bytes > MAX_BODY_BYTES) {
        return Err(HarborError::Validation(
            "ingest batch exceeds 16 MiB of body data".into(),
        ));
    }
    Ok(())
}

fn require_matching_content(event: &WebhookEvent, input: &WebhookInput) -> Result<(), HarborError> {
    if event.topic != input.topic || event.body != input.body {
        return Err(HarborError::Conflict(format!(
            "dedupe key {} was already used with different content",
            input.dedupe_key
        )));
    }
    Ok(())
}

fn next_identifier(previous: u64, kind: &str) -> Result<u64, HarborError> {
    previous
        .checked_add(1)
        .ok_or_else(|| HarborError::Conflict(format!("{kind} identifier sequence is exhausted")))
}
