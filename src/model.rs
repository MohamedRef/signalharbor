use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarborError {
    Validation(String),
    Conflict(String),
    NotFound(String),
    NotDue { delivery_id: String, due_at: u64 },
    Journal(String),
}

impl Display for HarborError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation error: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::NotFound(message) => write!(formatter, "not found: {message}"),
            Self::NotDue {
                delivery_id,
                due_at,
            } => write!(
                formatter,
                "delivery {delivery_id} is not due until {due_at}"
            ),
            Self::Journal(message) => write!(formatter, "journal error: {message}"),
        }
    }
}

impl Error for HarborError {}

pub(crate) fn required(value: &str, field: &str, maximum: usize) -> Result<String, HarborError> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Err(HarborError::Validation(format!("{field} is required")));
    }
    if normalized.len() > maximum {
        return Err(HarborError::Validation(format!(
            "{field} must be at most {maximum} characters"
        )));
    }
    Ok(normalized)
}

pub(crate) fn validate_endpoint(value: &str) -> Result<String, HarborError> {
    let normalized = required(value, "endpoint", 2_000)?;
    if !(normalized.starts_with("https://") || normalized.starts_with("http://")) {
        return Err(HarborError::Validation(
            "endpoint must start with http:// or https://".into(),
        ));
    }
    Ok(normalized)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub id: String,
    pub tenant: String,
    pub topic_pattern: String,
    pub endpoint: String,
    pub max_attempts: u16,
    pub base_delay_seconds: u64,
    pub active: bool,
    pub failure_threshold: u16,
    pub cooldown_seconds: u64,
    pub consecutive_failures: u16,
    pub circuit_open_until: Option<u64>,
}

impl Route {
    /// Creates a validated tenant route with an exact topic or trailing wildcard.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Validation`] when identifiers, pattern, endpoint,
    /// attempt count, or retry delay fall outside their accepted bounds.
    pub fn new(
        id: &str,
        tenant: &str,
        topic_pattern: &str,
        endpoint: &str,
        max_attempts: u16,
        base_delay_seconds: u64,
    ) -> Result<Self, HarborError> {
        let pattern = required(topic_pattern, "topic pattern", 120)?.to_ascii_lowercase();
        let wildcard_count = pattern.matches('*').count();
        if wildcard_count > 1 || (wildcard_count == 1 && !pattern.ends_with(".*")) {
            return Err(HarborError::Validation(
                "topic pattern supports only a trailing .* wildcard".into(),
            ));
        }
        if !(1..=20).contains(&max_attempts) {
            return Err(HarborError::Validation(
                "max_attempts must be from 1 through 20".into(),
            ));
        }
        if !(1..=86_400).contains(&base_delay_seconds) {
            return Err(HarborError::Validation(
                "base_delay_seconds must be from 1 through 86400".into(),
            ));
        }
        Ok(Self {
            id: required(id, "route id", 80)?,
            tenant: required(tenant, "tenant", 80)?.to_ascii_lowercase(),
            topic_pattern: pattern,
            endpoint: validate_endpoint(endpoint)?,
            max_attempts,
            base_delay_seconds,
            active: true,
            failure_threshold: 3,
            cooldown_seconds: 60,
            consecutive_failures: 0,
            circuit_open_until: None,
        })
    }

    #[must_use]
    pub fn matches(&self, tenant: &str, topic: &str) -> bool {
        if !self.active || self.tenant != tenant.to_ascii_lowercase() {
            return false;
        }
        let normalized_topic = topic.to_ascii_lowercase();
        if let Some(prefix) = self.topic_pattern.strip_suffix('*') {
            normalized_topic.starts_with(prefix)
        } else {
            normalized_topic == self.topic_pattern
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open { until: u64 },
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteCircuitHealth {
    pub route_id: String,
    pub state: CircuitState,
    pub consecutive_failures: u16,
    pub failure_threshold: u16,
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookInput {
    pub tenant: String,
    pub topic: String,
    pub dedupe_key: String,
    pub body: Vec<u8>,
    pub received_at: u64,
}

impl WebhookInput {
    /// Creates normalized routing fields while preserving the binary body.
    ///
    /// # Errors
    ///
    /// Returns [`HarborError::Validation`] for missing routing fields, an empty
    /// body, or a body larger than one mebibyte.
    pub fn new(
        tenant: &str,
        topic: &str,
        dedupe_key: &str,
        body: impl Into<Vec<u8>>,
        received_at: u64,
    ) -> Result<Self, HarborError> {
        let body = body.into();
        if body.is_empty() {
            return Err(HarborError::Validation("webhook body is required".into()));
        }
        if body.len() > 1_048_576 {
            return Err(HarborError::Validation(
                "webhook body must not exceed 1 MiB".into(),
            ));
        }
        Ok(Self {
            tenant: required(tenant, "tenant", 80)?.to_ascii_lowercase(),
            topic: required(topic, "topic", 120)?.to_ascii_lowercase(),
            dedupe_key: required(dedupe_key, "dedupe key", 200)?,
            body,
            received_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEvent {
    pub id: String,
    pub tenant: String,
    pub topic: String,
    pub dedupe_key: String,
    pub body: Vec<u8>,
    pub received_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    Pending,
    Succeeded,
    DeadLetter,
    Cancelled,
}

impl DeliveryState {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::DeadLetter => "dead",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_code(value: &str) -> Result<Self, HarborError> {
        match value {
            "pending" => Ok(Self::Pending),
            "succeeded" => Ok(Self::Succeeded),
            "dead" => Ok(Self::DeadLetter),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(HarborError::Journal(format!(
                "unknown delivery state: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub id: String,
    pub event_id: String,
    pub route_id: String,
    pub endpoint: String,
    pub state: DeliveryState,
    pub attempts: u16,
    pub next_attempt_at: u64,
    pub last_error: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<u64>,
}

impl Delivery {
    #[must_use]
    pub fn due(&self, now: u64) -> bool {
        self.state == DeliveryState::Pending
            && self.next_attempt_at <= now
            && self
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= now)
    }

    #[must_use]
    pub fn leased(&self, now: u64) -> bool {
        self.state == DeliveryState::Pending
            && self.lease_owner.is_some()
            && self
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    Http(u16),
    Transport(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAttempt {
    pub id: String,
    pub sequence: u64,
    pub delivery_id: String,
    pub cycle_attempt: u16,
    pub attempted_at: u64,
    pub worker: Option<String>,
    pub outcome: AttemptOutcome,
    pub resulting_state: DeliveryState,
    pub retried: bool,
    pub next_attempt_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryCancellation {
    pub id: String,
    pub delivery_id: String,
    pub actor: String,
    pub reason: String,
    pub cancelled_at: u64,
}

/// An operator-controlled pause on a pending delivery.
///
/// Holds have a bounded automatic expiry so a forgotten intervention cannot
/// permanently strand work. An explicit release retains who ended the hold
/// and when; the journal keeps both the original and replacement snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryHold {
    pub id: String,
    pub sequence: u64,
    pub delivery_id: String,
    pub actor: String,
    pub reason: String,
    pub held_at: u64,
    pub release_at: u64,
    pub released_at: Option<u64>,
    pub released_by: Option<String>,
}

impl DeliveryHold {
    #[must_use]
    pub fn active_at(&self, now: u64) -> bool {
        self.released_at.is_none() && self.held_at <= now && now < self.release_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptHistorySummary {
    pub delivery_id: String,
    pub total: usize,
    pub retried: usize,
    pub succeeded: usize,
    pub dead_lettered: usize,
    pub transport_failures: usize,
    pub first_attempt_at: Option<u64>,
    pub last_attempt_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestResult {
    pub event: WebhookEvent,
    pub duplicate: bool,
    pub delivery_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillRequest {
    pub route_id: String,
    pub received_from: u64,
    pub received_through: u64,
    pub due_at: u64,
    pub maximum_deliveries: usize,
}

impl BackfillRequest {
    /// Creates a bounded historical routing request for one route.
    ///
    /// # Errors
    ///
    /// Returns validation errors for a missing route, inverted time window,
    /// or a delivery limit outside 1 through 10,000.
    pub fn new(
        route_id: &str,
        received_from: u64,
        received_through: u64,
        due_at: u64,
        maximum_deliveries: usize,
    ) -> Result<Self, HarborError> {
        if received_from > received_through {
            return Err(HarborError::Validation(
                "backfill received window is inverted".into(),
            ));
        }
        if !(1..=10_000).contains(&maximum_deliveries) {
            return Err(HarborError::Validation(
                "maximum_deliveries must be from 1 through 10000".into(),
            ));
        }
        Ok(Self {
            route_id: required(route_id, "route id", 80)?,
            received_from,
            received_through,
            due_at,
            maximum_deliveries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillResult {
    pub route_id: String,
    pub matched_events: usize,
    pub already_routed: usize,
    pub created_delivery_ids: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkRedriveResult {
    pub route_id: String,
    pub redriven_delivery_ids: Vec<String>,
    pub remaining_dead_letters: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointUpdateMode {
    FutureOnly,
    MigrateUnattempted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointUpdateResult {
    pub route: Route,
    pub migrated_delivery_ids: Vec<String>,
    pub retained_delivery_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptResult {
    pub delivery: Delivery,
    pub retried: bool,
    pub retry_delay_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarborSummary {
    pub events: usize,
    pub deliveries: usize,
    pub pending: usize,
    pub succeeded: usize,
    pub dead_letter: usize,
    pub cancelled: usize,
}
