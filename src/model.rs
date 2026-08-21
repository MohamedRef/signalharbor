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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub id: String,
    pub tenant: String,
    pub topic_pattern: String,
    pub endpoint: String,
    pub max_attempts: u16,
    pub base_delay_seconds: u64,
    pub active: bool,
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
        if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
            return Err(HarborError::Validation(
                "endpoint must start with http:// or https://".into(),
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
            endpoint: required(endpoint, "endpoint", 2_000)?,
            max_attempts,
            base_delay_seconds,
            active: true,
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
}

impl DeliveryState {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::DeadLetter => "dead",
        }
    }

    pub(crate) fn from_code(value: &str) -> Result<Self, HarborError> {
        match value {
            "pending" => Ok(Self::Pending),
            "succeeded" => Ok(Self::Succeeded),
            "dead" => Ok(Self::DeadLetter),
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
}
