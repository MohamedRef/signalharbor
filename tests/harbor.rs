use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct TestJournal {
    path: PathBuf,
}

impl TestJournal {
    fn new() -> Self {
        let sequence = JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "signalharbor-{}-{sequence}.journal",
            std::process::id()
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestJournal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn harbor() -> (TestJournal, SignalHarbor) {
    let journal = TestJournal::new();
    let harbor = SignalHarbor::open(journal.path()).expect("open empty journal");
    (journal, harbor)
}

fn route(id: &str, pattern: &str) -> Route {
    Route::new(
        id,
        "acme",
        pattern,
        &format!("https://hooks.example/{id}"),
        3,
        10,
    )
    .expect("valid route")
}

fn webhook(key: &str, topic: &str, received_at: u64) -> WebhookInput {
    WebhookInput::new(
        "acme",
        topic,
        key,
        br#"{"order":"A-42"}"#.to_vec(),
        received_at,
    )
    .expect("valid webhook")
}

#[test]
fn validates_and_matches_routes() {
    let exact = route("exact", "orders.created");
    let wildcard = route("wild", "orders.*");
    assert!(exact.matches("ACME", "ORDERS.CREATED"));
    assert!(!exact.matches("acme", "orders.updated"));
    assert!(wildcard.matches("acme", "orders.updated"));
    assert!(!wildcard.matches("other", "orders.updated"));

    assert!(matches!(
        Route::new("bad", "acme", "orders*bad", "https://example.test", 3, 10),
        Err(HarborError::Validation(_))
    ));
    assert!(Route::new("bad", "acme", "orders", "ftp://example.test", 3, 10).is_err());
    assert!(Route::new("bad", "acme", "orders", "https://example.test", 0, 10).is_err());
    assert!(Route::new("bad", "acme", "orders", "https://example.test", 2, 0).is_err());
}

#[test]
fn validates_webhook_body_and_normalizes_routing_fields() {
    let input = WebhookInput::new(" Acme ", " Orders.Created ", " key-1 ", b"body".to_vec(), 7)
        .expect("valid input");
    assert_eq!(input.tenant, "acme");
    assert_eq!(input.topic, "orders.created");
    assert_eq!(input.dedupe_key, "key-1");
    assert!(WebhookInput::new("acme", "orders", "key", Vec::new(), 0).is_err());
    assert!(WebhookInput::new("", "orders", "key", b"x".to_vec(), 0).is_err());
    assert!(WebhookInput::new("acme", "", "key", b"x".to_vec(), 0).is_err());
}

#[test]
fn routes_one_event_to_exact_and_wildcard_subscribers() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("exact", "orders.created"))
        .expect("register exact");
    harbor
        .register_route(route("wild", "orders.*"))
        .expect("register wildcard");
    harbor
        .register_route(
            Route::new(
                "other-tenant",
                "other",
                "orders.*",
                "https://other.example/hook",
                2,
                5,
            )
            .expect("other route"),
        )
        .expect("register other route");

    let result = harbor
        .ingest(webhook("key-1", "orders.created", 100))
        .expect("ingest");

    assert!(!result.duplicate);
    assert_eq!(result.event.id, "evt-000000000001");
    assert_eq!(result.delivery_ids.len(), 2);
    assert_eq!(harbor.summary().events, 1);
    assert_eq!(harbor.summary().pending, 2);
}

#[test]
fn accepts_events_without_routes() {
    let (_journal, mut harbor) = harbor();
    let result = harbor
        .ingest(webhook("key-1", "orders.created", 100))
        .expect("ingest");
    assert!(result.delivery_ids.is_empty());
    assert_eq!(harbor.event(&result.event.id).expect("event"), result.event);
}

#[test]
fn duplicate_ingest_is_idempotent_but_changed_content_conflicts() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let first = harbor
        .ingest(webhook("same-key", "orders.created", 100))
        .expect("first ingest");
    let duplicate = harbor
        .ingest(webhook("same-key", "orders.created", 200))
        .expect("duplicate ingest");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.event.id, first.event.id);
    assert_eq!(duplicate.delivery_ids, first.delivery_ids);
    assert_eq!(harbor.summary().events, 1);

    let changed = WebhookInput::new(
        "acme",
        "orders.created",
        "same-key",
        b"changed".to_vec(),
        300,
    )
    .expect("changed input");
    assert!(matches!(
        harbor.ingest(changed),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn dedupe_keys_are_isolated_by_tenant() {
    let (_journal, mut harbor) = harbor();
    let first = harbor
        .ingest(webhook("shared", "orders.created", 1))
        .expect("acme event");
    let second = harbor
        .ingest(
            WebhookInput::new("other", "orders.created", "shared", b"other".to_vec(), 2)
                .expect("other input"),
        )
        .expect("other event");
    assert_ne!(first.event.id, second.event.id);
}

#[test]
fn route_deactivation_stops_new_deliveries_and_survives_restart() {
    let (journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let disabled = harbor
        .set_route_active("orders", false)
        .expect("disable route");
    assert!(!disabled.active);
    let result = harbor
        .ingest(webhook("key", "orders.created", 10))
        .expect("ingest");
    assert!(result.delivery_ids.is_empty());
    drop(harbor);

    let restored = SignalHarbor::open(journal.path()).expect("replay");
    assert!(!restored.routes()[0].active);
}

#[test]
fn due_queue_is_sorted_and_limited() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    harbor
        .ingest(webhook("later", "orders.created", 200))
        .expect("later event");
    harbor
        .ingest(webhook("earlier", "orders.created", 100))
        .expect("earlier event");

    let due = harbor.due_deliveries(200, 1).expect("due deliveries");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].next_attempt_at, 100);
    assert!(harbor.due_deliveries(200, 0).is_err());
}

#[test]
fn successful_attempt_is_terminal() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let delivery_id = harbor
        .ingest(webhook("key", "orders.created", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();

    let result = harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(204))
        .expect("record success");
    assert_eq!(result.delivery.state, DeliveryState::Succeeded);
    assert!(!result.retried);
    assert_eq!(harbor.summary().succeeded, 1);
    assert!(matches!(
        harbor.record_attempt(&delivery_id, 100, AttemptOutcome::Http(200)),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn retryable_failures_back_off_and_cannot_run_early() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let delivery_id = harbor
        .ingest(webhook("key", "orders.created", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();

    let first = harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(503))
        .expect("first failure");
    assert!(first.retried);
    assert_eq!(first.retry_delay_seconds, Some(10));
    assert_eq!(first.delivery.next_attempt_at, 110);
    assert!(matches!(
        harbor.record_attempt(&delivery_id, 109, AttemptOutcome::Http(200)),
        Err(HarborError::NotDue { due_at: 110, .. })
    ));
    let second = harbor
        .record_attempt(
            &delivery_id,
            110,
            AttemptOutcome::Transport("timeout".into()),
        )
        .expect("second failure");
    assert_eq!(second.retry_delay_seconds, Some(20));
    assert_eq!(second.delivery.next_attempt_at, 130);
}

#[test]
fn permanent_http_failure_goes_directly_to_dead_letter() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let delivery_id = harbor
        .ingest(webhook("key", "orders.created", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();
    let result = harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(400))
        .expect("record failure");
    assert_eq!(result.delivery.state, DeliveryState::DeadLetter);
    assert_eq!(result.delivery.last_error.as_deref(), Some("http 400"));
    assert_eq!(harbor.dead_letters(), vec![result.delivery]);
}

#[test]
fn retry_exhaustion_and_redrive_reset_delivery() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(
            Route::new(
                "orders",
                "acme",
                "orders.*",
                "https://hooks.example/orders",
                2,
                5,
            )
            .expect("route"),
        )
        .expect("register route");
    let delivery_id = harbor
        .ingest(webhook("key", "orders.created", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(429))
        .expect("first attempt");
    let exhausted = harbor
        .record_attempt(&delivery_id, 105, AttemptOutcome::Http(500))
        .expect("second attempt");
    assert_eq!(exhausted.delivery.state, DeliveryState::DeadLetter);
    assert_eq!(exhausted.delivery.attempts, 2);

    let redriven = harbor.redrive(&delivery_id, 500).expect("redrive");
    assert_eq!(redriven.state, DeliveryState::Pending);
    assert_eq!(redriven.attempts, 0);
    assert_eq!(redriven.next_attempt_at, 500);
    assert!(redriven.last_error.is_none());
}

#[test]
fn redrive_requires_dead_letter_and_active_route() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let delivery_id = harbor
        .ingest(webhook("key", "orders.created", 100))
        .expect("ingest")
        .delivery_ids[0]
        .clone();
    assert!(matches!(
        harbor.redrive(&delivery_id, 200),
        Err(HarborError::Conflict(_))
    ));
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(404))
        .expect("dead letter");
    harbor
        .set_route_active("orders", false)
        .expect("disable route");
    assert!(matches!(
        harbor.redrive(&delivery_id, 200),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn journal_replay_restores_binary_payload_delivery_and_sequences() {
    let (journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    let binary = WebhookInput::new(
        "acme",
        "orders.created",
        "binary-key",
        vec![0, 1, 2, 0xff],
        100,
    )
    .expect("binary webhook");
    let first = harbor.ingest(binary).expect("ingest binary");
    harbor
        .record_attempt(
            &first.delivery_ids[0],
            100,
            AttemptOutcome::Transport("network reset".into()),
        )
        .expect("record retry");
    drop(harbor);

    let mut restored = SignalHarbor::open(journal.path()).expect("replay journal");
    assert_eq!(
        restored.event(&first.event.id).expect("event").body,
        vec![0, 1, 2, 0xff]
    );
    assert_eq!(
        restored
            .delivery(&first.delivery_ids[0])
            .expect("delivery")
            .next_attempt_at,
        110
    );
    let duplicate = restored
        .ingest(
            WebhookInput::new(
                "acme",
                "orders.created",
                "binary-key",
                vec![0, 1, 2, 0xff],
                999,
            )
            .expect("duplicate input"),
        )
        .expect("dedupe after replay");
    assert!(duplicate.duplicate);
    let next = restored
        .ingest(webhook("next", "orders.created", 200))
        .expect("next event");
    assert_eq!(next.event.id, "evt-000000000002");
}

#[test]
fn corrupt_journal_fails_closed_with_line_number() {
    let journal = TestJournal::new();
    fs::write(journal.path(), "E|not-enough-fields\n").expect("write corrupt journal");
    let Err(error) = SignalHarbor::open(journal.path()) else {
        panic!("corrupt journal should fail");
    };
    assert!(matches!(error, HarborError::Journal(_)));
    assert!(error.to_string().contains("line 1"));
}

#[test]
fn missing_records_and_route_conflicts_are_explicit() {
    let (_journal, mut harbor) = harbor();
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register route");
    assert!(matches!(
        harbor.register_route(route("orders", "other.*")),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.set_route_active("missing", false),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.event("missing"),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.delivery("missing"),
        Err(HarborError::NotFound(_))
    ));
}
