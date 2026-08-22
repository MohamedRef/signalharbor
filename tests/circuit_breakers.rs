use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, CircuitState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-circuit-{}-{sequence}.journal",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn route(id: &str) -> Route {
    Route::new(
        id,
        "acme",
        "orders.*",
        &format!("https://hooks.example/{id}"),
        5,
        10,
    )
    .expect("valid route")
}

fn input(key: &str, received_at: u64) -> WebhookInput {
    WebhookInput::new(
        "acme",
        "orders.created",
        key,
        br#"{"order":"A-42"}"#.to_vec(),
        received_at,
    )
    .expect("valid input")
}

fn seeded() -> (Journal, SignalHarbor, String) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open journal");
    harbor.register_route(route("orders")).expect("route");
    harbor
        .configure_circuit_breaker("orders", 2, 60)
        .expect("configure circuit");
    let delivery_id = harbor
        .ingest(input("event-1", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();
    (journal, harbor, delivery_id)
}

#[test]
fn opens_after_the_configured_consecutive_failure_threshold() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(503))
        .expect("first retry");
    let closed = harbor.route_circuit_health("orders", 100).expect("health");
    assert_eq!(closed.state, CircuitState::Closed);
    assert_eq!(closed.consecutive_failures, 1);

    harbor
        .record_attempt(
            &delivery_id,
            110,
            AttemptOutcome::Transport("timeout".into()),
        )
        .expect("second retry");
    let open = harbor.route_circuit_health("orders", 110).expect("health");
    assert_eq!(open.state, CircuitState::Open { until: 170 });
    assert_eq!(open.consecutive_failures, 2);
    assert!(harbor.due_deliveries(130, 10).expect("due").is_empty());
}

#[test]
fn blocks_direct_attempts_during_cooldown_without_mutation() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(500))
        .expect("first retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(502))
        .expect("second retry");
    let before = harbor.delivery(&delivery_id).expect("delivery");
    let error = harbor
        .record_attempt(&delivery_id, 130, AttemptOutcome::Http(200))
        .expect_err("circuit must block attempt");
    assert!(matches!(error, HarborError::Conflict(message) if message.contains("open until 170")));
    assert_eq!(harbor.delivery(&delivery_id).expect("delivery"), before);
    assert_eq!(
        harbor.attempt_history(&delivery_id).expect("history").len(),
        2
    );
}

#[test]
fn successful_probe_closes_the_circuit_after_cooldown() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(500))
        .expect("first retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(500))
        .expect("second retry");
    assert_eq!(
        harbor
            .route_circuit_health("orders", 170)
            .expect("health")
            .state,
        CircuitState::HalfOpen
    );
    assert_eq!(harbor.due_deliveries(170, 10).expect("due").len(), 1);

    harbor
        .record_attempt(&delivery_id, 170, AttemptOutcome::Http(204))
        .expect("probe success");
    let health = harbor.route_circuit_health("orders", 170).expect("health");
    assert_eq!(health.state, CircuitState::Closed);
    assert_eq!(health.consecutive_failures, 0);
}

#[test]
fn failed_probe_reopens_for_a_fresh_cooldown() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(500))
        .expect("first retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(500))
        .expect("second retry");
    harbor
        .record_attempt(&delivery_id, 170, AttemptOutcome::Http(500))
        .expect("probe failure");
    let health = harbor.route_circuit_health("orders", 170).expect("health");
    assert_eq!(health.state, CircuitState::Open { until: 230 });
    assert_eq!(health.consecutive_failures, 3);
}

#[test]
fn open_state_and_failure_count_survive_restart() {
    let (journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(503))
        .expect("first retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(503))
        .expect("second retry");
    drop(harbor);

    let restored = SignalHarbor::open(journal.path()).expect("restore");
    let health = restored
        .route_circuit_health("orders", 120)
        .expect("health");
    assert_eq!(health.state, CircuitState::Open { until: 170 });
    assert_eq!(health.consecutive_failures, 2);
    assert!(restored.due_deliveries(130, 10).expect("due").is_empty());
}

#[test]
fn one_open_route_does_not_block_another_route() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open");
    harbor.register_route(route("unstable")).expect("route");
    harbor.register_route(route("healthy")).expect("route");
    harbor
        .configure_circuit_breaker("unstable", 1, 60)
        .expect("configure");
    let result = harbor.ingest(input("two-routes", 100)).expect("ingest");
    let unstable = result
        .delivery_ids
        .iter()
        .find(|id| harbor.delivery(id).expect("delivery").route_id == "unstable")
        .cloned()
        .expect("unstable delivery");
    harbor
        .record_attempt(&unstable, 100, AttemptOutcome::Http(503))
        .expect("open circuit");

    let claimed = harbor.claim_due("worker", 110, 10, 20).expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].route_id, "healthy");
}

#[test]
fn permanent_http_response_resets_transient_failure_streak() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(503))
        .expect("retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(400))
        .expect("permanent response");
    let health = harbor.route_circuit_health("orders", 110).expect("health");
    assert_eq!(health.state, CircuitState::Closed);
    assert_eq!(health.consecutive_failures, 0);
}

#[test]
fn configuration_validates_bounds_and_resets_runtime_state() {
    let (_journal, mut harbor, delivery_id) = seeded();
    assert!(matches!(
        harbor.configure_circuit_breaker("orders", 0, 60),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.configure_circuit_breaker("orders", 2, 0),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.configure_circuit_breaker("missing", 2, 60),
        Err(HarborError::NotFound(_))
    ));
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(500))
        .expect("failure");
    let reset = harbor
        .configure_circuit_breaker("orders", 4, 120)
        .expect("reconfigure");
    assert_eq!(reset.state, CircuitState::Closed);
    assert_eq!(reset.consecutive_failures, 0);
    assert_eq!(reset.failure_threshold, 4);
    assert_eq!(reset.cooldown_seconds, 120);
}

#[test]
fn legacy_route_records_receive_safe_circuit_defaults() {
    let journal = Journal::new();
    let route = "R|6f7264657273|61636d65|6f72646572732e2a|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|3|10|1";
    fs::write(journal.path(), format!("{route}\n")).expect("write legacy route");
    let harbor = SignalHarbor::open(journal.path()).expect("restore legacy route");
    let health = harbor
        .route_circuit_health("orders", 0)
        .expect("legacy health");
    assert_eq!(health.state, CircuitState::Closed);
    assert_eq!(health.failure_threshold, 3);
    assert_eq!(health.cooldown_seconds, 60);
}

#[test]
fn malformed_circuit_configuration_in_journal_fails_closed() {
    let journal = Journal::new();
    let invalid = "R|6f7264657273|61636d65|6f72646572732e2a|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|3|10|1|0|60|0|-";
    fs::write(journal.path(), format!("{invalid}\n")).expect("write invalid route");
    assert!(matches!(
        SignalHarbor::open(journal.path()),
        Err(HarborError::Journal(_))
    ));
}
