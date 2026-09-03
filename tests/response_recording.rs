use signalharbor::{
    AttemptOutcome, CircuitState, DeliveryResponse, DeliveryState, HarborError, Route,
    SignalHarbor, WebhookInput,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "signalharbor-response-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn journal(&self) -> PathBuf {
        self.0.join("journal")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn setup(max_attempts: u16) -> (Workspace, SignalHarbor, String) {
    let workspace = Workspace::new();
    let mut harbor = SignalHarbor::open(workspace.journal()).unwrap();
    harbor
        .register_route(
            Route::new(
                "receiver",
                "tenant",
                "topic",
                "https://example.test/hook",
                max_attempts,
                10,
            )
            .unwrap(),
        )
        .unwrap();
    let delivery = harbor
        .ingest(WebhookInput::new("tenant", "topic", "one", b"body", 100).unwrap())
        .unwrap()
        .delivery_ids[0]
        .clone();
    (workspace, harbor, delivery)
}

fn response(status_code: u16, retry_after_seconds: Option<u64>) -> DeliveryResponse {
    DeliveryResponse {
        status_code,
        retry_after_seconds,
    }
}

#[test]
fn receiver_delay_is_persisted_with_the_attempt_and_survives_restart() {
    let (workspace, mut harbor, id) = setup(4);
    let result = harbor
        .record_response(&id, 100, response(429, Some(90)))
        .unwrap();
    assert_eq!(result.retry_delay_seconds, Some(90));
    assert_eq!(result.delivery.next_attempt_at, 190);
    assert_eq!(result.delivery.attempts, 1);
    let restored = SignalHarbor::open(workspace.journal()).unwrap();
    assert_eq!(restored.delivery(&id).unwrap(), result.delivery);
    let history = restored.attempt_history(&id).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, AttemptOutcome::Http(429));
    assert_eq!(history[0].next_attempt_at, Some(190));
    assert!(restored.due_deliveries(189, 10).unwrap().is_empty());
    assert_eq!(restored.due_deliveries(190, 10).unwrap().len(), 1);
}

#[test]
fn receiver_delay_never_shortens_route_backoff_and_one_day_is_supported() {
    for override_seconds in [None, Some(0), Some(3), Some(10), Some(86_400)] {
        let (_workspace, mut harbor, id) = setup(4);
        let result = harbor
            .record_response(&id, 100, response(503, override_seconds))
            .unwrap();
        assert_eq!(
            result.retry_delay_seconds,
            Some(override_seconds.unwrap_or(0).max(10))
        );
    }
    let (_workspace, mut harbor, id) = setup(4);
    harbor
        .record_response(&id, 100, response(503, Some(0)))
        .unwrap();
    let next = harbor
        .record_response(&id, 110, response(503, Some(5)))
        .unwrap();
    assert_eq!(next.retry_delay_seconds, Some(20));
    assert_eq!(next.delivery.next_attempt_at, 130);
}

#[test]
fn claimed_response_preserves_ownership_guards_and_updates_circuit_durably() {
    let (workspace, mut harbor, id) = setup(4);
    harbor.configure_circuit_breaker("receiver", 1, 5).unwrap();
    harbor.claim_due("worker", 100, 1, 30).unwrap();
    let before = fs::read(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.record_claimed_response(&id, "other", 105, response(503, Some(40))),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    let result = harbor
        .record_claimed_response(&id, "worker", 105, response(503, Some(40)))
        .unwrap();
    assert_eq!(result.delivery.next_attempt_at, 145);
    assert!(result.delivery.lease_owner.is_none());
    let restored = SignalHarbor::open(workspace.journal()).unwrap();
    assert_eq!(restored.delivery(&id).unwrap(), result.delivery);
    assert_eq!(
        restored.attempt_history(&id).unwrap()[0].worker.as_deref(),
        Some("worker")
    );
    assert_eq!(
        restored
            .route_circuit_health("receiver", 106)
            .unwrap()
            .state,
        CircuitState::Open { until: 110 }
    );
}

#[test]
fn terminal_responses_and_exhausted_budgets_do_not_gain_retry_deadlines() {
    for (code, expected) in [
        (204, DeliveryState::Succeeded),
        (404, DeliveryState::DeadLetter),
    ] {
        let (_workspace, mut harbor, id) = setup(4);
        let result = harbor
            .record_response(&id, 100, response(code, None))
            .unwrap();
        assert_eq!(result.delivery.state, expected);
        assert!(!result.retried);
        assert_eq!(result.retry_delay_seconds, None);
        assert_eq!(
            harbor.attempt_history(&id).unwrap()[0].next_attempt_at,
            None
        );
    }
    let (_workspace, mut harbor, id) = setup(1);
    let exhausted = harbor
        .record_response(&id, 100, response(429, Some(500)))
        .unwrap();
    assert_eq!(exhausted.delivery.state, DeliveryState::DeadLetter);
    assert_eq!(exhausted.retry_delay_seconds, None);
}

#[test]
fn malformed_responses_and_legacy_http_outcomes_are_rejected_before_any_write() {
    let (workspace, mut harbor, id) = setup(4);
    let before = fs::read(workspace.journal()).unwrap();
    let delivery = harbor.delivery(&id).unwrap();
    let routes = harbor.routes();
    for invalid in [
        response(99, None),
        response(600, None),
        response(429, Some(86_401)),
        response(204, Some(1)),
        response(404, Some(1)),
    ] {
        assert!(matches!(
            harbor.record_response(&id, 100, invalid),
            Err(HarborError::Validation(_))
        ));
    }
    for code in [0, 99, 600, u16::MAX] {
        assert!(matches!(
            harbor.record_attempt(&id, 100, AttemptOutcome::Http(code)),
            Err(HarborError::Validation(_))
        ));
    }
    assert_eq!(harbor.delivery(&id).unwrap(), delivery);
    assert_eq!(harbor.routes(), routes);
    assert!(harbor.attempt_history(&id).unwrap().is_empty());
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
}

#[test]
fn overflowing_receiver_deadline_does_not_partially_record_an_attempt() {
    let (workspace, mut harbor, id) = setup(4);
    let before = fs::read(workspace.journal()).unwrap();
    let delivery = harbor.delivery(&id).unwrap();
    assert!(matches!(
        harbor.record_response(&id, u64::MAX - 10, response(429, Some(20))),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
    assert_eq!(harbor.delivery(&id).unwrap(), delivery);
    assert!(harbor.attempt_history(&id).unwrap().is_empty());
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
}

#[test]
fn persistence_failure_preserves_claim_attempt_history_and_circuit_state() {
    let (workspace, mut harbor, id) = setup(4);
    harbor.claim_due("worker", 100, 1, 30).unwrap();
    let delivery = harbor.delivery(&id).unwrap();
    let routes = harbor.routes();
    fs::remove_file(workspace.journal()).unwrap();
    fs::create_dir(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.record_claimed_response(&id, "worker", 110, response(429, Some(60))),
        Err(HarborError::Journal(_))
    ));
    assert_eq!(harbor.delivery(&id).unwrap(), delivery);
    assert_eq!(harbor.routes(), routes);
    assert!(harbor.attempt_history(&id).unwrap().is_empty());
}

#[test]
fn response_recording_respects_due_times_holds_and_live_leases() {
    let (workspace, mut harbor, id) = setup(4);
    assert!(matches!(
        harbor.record_response(&id, 99, response(200, None)),
        Err(HarborError::NotDue { .. })
    ));
    harbor
        .hold_delivery(&id, "operator", "pause", 100, 110)
        .unwrap();
    assert!(matches!(
        harbor.record_response(&id, 109, response(200, None)),
        Err(HarborError::Conflict(_))
    ));
    harbor.claim_due("worker", 110, 1, 10).unwrap();
    let before = fs::read(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.record_response(&id, 115, response(200, None)),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.record_claimed_response(&id, "worker", 120, response(200, None)),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(
        harbor
            .record_response(&id, 120, response(200, None))
            .unwrap()
            .delivery
            .state,
        DeliveryState::Succeeded
    );
}
