use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-attempts-{}-{sequence}.journal",
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

fn seeded(max_attempts: u16) -> (Journal, SignalHarbor, String) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open journal");
    harbor
        .register_route(
            Route::new(
                "orders",
                "acme",
                "orders.*",
                "https://hooks.example/orders",
                max_attempts,
                10,
            )
            .expect("route"),
        )
        .expect("register route");
    let delivery_id = harbor
        .ingest(
            WebhookInput::new(
                "acme",
                "orders.created",
                "event-1",
                br#"{"order":"A-1"}"#.to_vec(),
                100,
            )
            .expect("input"),
        )
        .expect("ingest")
        .delivery_ids[0]
        .clone();
    (journal, harbor, delivery_id)
}

#[test]
fn records_retry_and_success_as_ordered_attempts() {
    let (_journal, mut harbor, delivery_id) = seeded(3);
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(503))
        .expect("retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(204))
        .expect("success");

    let history = harbor.attempt_history(&delivery_id).expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, "att-000000000001");
    assert_eq!(history[0].cycle_attempt, 1);
    assert_eq!(history[0].resulting_state, DeliveryState::Pending);
    assert!(history[0].retried);
    assert_eq!(history[0].next_attempt_at, Some(110));
    assert_eq!(history[0].error.as_deref(), Some("http 503"));
    assert_eq!(history[1].id, "att-000000000002");
    assert_eq!(history[1].cycle_attempt, 2);
    assert_eq!(history[1].resulting_state, DeliveryState::Succeeded);
    assert!(!history[1].retried);
    assert!(history[1].next_attempt_at.is_none());
    assert!(history[1].error.is_none());
}

#[test]
fn retry_rejects_a_due_time_beyond_the_timestamp_range() {
    let (_journal, mut harbor, delivery_id) = seeded(3);
    assert!(matches!(
        harbor.record_attempt(&delivery_id, u64::MAX, AttemptOutcome::Http(503)),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
    assert!(harbor.attempt_history(&delivery_id).unwrap().is_empty());
    let delivery = harbor.delivery(&delivery_id).unwrap();
    assert_eq!(delivery.attempts, 0);
    assert_eq!(delivery.state, DeliveryState::Pending);
}

#[test]
fn transport_failure_preserves_worker_and_safe_error_text() {
    let (_journal, mut harbor, delivery_id) = seeded(3);
    harbor
        .claim_due("worker-west", 100, 1, 30)
        .expect("claim delivery");
    harbor
        .record_claimed_attempt(
            &delivery_id,
            "worker-west",
            105,
            AttemptOutcome::Transport("reset | upstream; retry".into()),
        )
        .expect("record claimed attempt");

    let attempt = &harbor.attempt_history(&delivery_id).expect("history")[0];
    assert_eq!(attempt.worker.as_deref(), Some("worker-west"));
    assert_eq!(
        attempt.outcome,
        AttemptOutcome::Transport("reset | upstream; retry".into())
    );
    assert_eq!(
        attempt.error.as_deref(),
        Some("transport: reset | upstream; retry")
    );
}

#[test]
fn history_and_global_sequence_survive_restart() {
    let (journal, mut harbor, delivery_id) = seeded(3);
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(500))
        .expect("first attempt");
    drop(harbor);

    let mut restored = SignalHarbor::open(journal.path()).expect("restore journal");
    let history = restored
        .attempt_history(&delivery_id)
        .expect("restored history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].attempted_at, 100);
    restored
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(200))
        .expect("second attempt");
    let history = restored
        .attempt_history(&delivery_id)
        .expect("updated history");
    assert_eq!(history[1].id, "att-000000000002");
}

#[test]
fn redrive_starts_new_cycle_without_erasing_history() {
    let (_journal, mut harbor, delivery_id) = seeded(1);
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(400))
        .expect("dead letter");
    harbor.redrive(&delivery_id, 500).expect("redrive");
    harbor
        .record_attempt(&delivery_id, 500, AttemptOutcome::Http(200))
        .expect("redrive success");

    let history = harbor.attempt_history(&delivery_id).expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].cycle_attempt, 1);
    assert_eq!(history[0].resulting_state, DeliveryState::DeadLetter);
    assert_eq!(history[1].cycle_attempt, 1);
    assert_eq!(history[1].resulting_state, DeliveryState::Succeeded);
    assert!(history[0].sequence < history[1].sequence);
}

#[test]
fn attempt_summary_counts_outcomes_and_time_bounds() {
    let (_journal, mut harbor, delivery_id) = seeded(3);
    harbor
        .record_attempt(
            &delivery_id,
            100,
            AttemptOutcome::Transport("timeout".into()),
        )
        .expect("transport retry");
    harbor
        .record_attempt(&delivery_id, 110, AttemptOutcome::Http(500))
        .expect("http retry");
    harbor
        .record_attempt(&delivery_id, 130, AttemptOutcome::Http(400))
        .expect("permanent failure");

    let summary = harbor
        .attempt_history_summary(&delivery_id)
        .expect("attempt summary");
    assert_eq!(summary.delivery_id, delivery_id);
    assert_eq!(summary.total, 3);
    assert_eq!(summary.retried, 2);
    assert_eq!(summary.succeeded, 0);
    assert_eq!(summary.dead_lettered, 1);
    assert_eq!(summary.transport_failures, 1);
    assert_eq!(summary.first_attempt_at, Some(100));
    assert_eq!(summary.last_attempt_at, Some(130));
}

#[test]
fn successful_summary_reports_terminal_success() {
    let (_journal, mut harbor, delivery_id) = seeded(2);
    harbor
        .record_attempt(&delivery_id, 100, AttemptOutcome::Http(201))
        .expect("success");
    let summary = harbor
        .attempt_history_summary(&delivery_id)
        .expect("attempt summary");
    assert_eq!(summary.total, 1);
    assert_eq!(summary.succeeded, 1);
    assert_eq!(summary.dead_lettered, 0);
}

#[test]
fn untouched_delivery_has_empty_history_and_summary() {
    let (_journal, harbor, delivery_id) = seeded(2);
    assert!(
        harbor
            .attempt_history(&delivery_id)
            .expect("empty history")
            .is_empty()
    );
    let summary = harbor
        .attempt_history_summary(&delivery_id)
        .expect("empty summary");
    assert_eq!(summary.total, 0);
    assert_eq!(summary.first_attempt_at, None);
    assert_eq!(summary.last_attempt_at, None);
}

#[test]
fn history_queries_reject_unknown_deliveries() {
    let (_journal, harbor, _delivery_id) = seeded(2);
    assert!(matches!(
        harbor.attempt_history("missing"),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.attempt_history_summary("missing"),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn old_journal_without_attempt_records_remains_compatible() {
    let journal = Journal::new();
    let route = "R|6f7264657273|61636d65|6f72646572732e2a|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|3|10|1";
    let event = "E|6576742d303030303030303030303031|61636d65|6f72646572732e63726561746564|6f6c642d6b6579|7b7d|100";
    let delivery = "D|64656c2d303030303030303030303031|6576742d303030303030303030303031|6f7264657273|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|pending|0|100|-";
    fs::write(journal.path(), format!("{route}\n{event}\n{delivery}\n"))
        .expect("write legacy journal");
    let harbor = SignalHarbor::open(journal.path()).expect("open legacy journal");
    assert!(
        harbor
            .attempt_history("del-000000000001")
            .expect("legacy empty history")
            .is_empty()
    );
}

#[test]
fn malformed_attempt_record_fails_closed_with_line_context() {
    let journal = Journal::new();
    fs::write(
        journal.path(),
        "A|626164|1|64656c|1|100|-|unknown|200|pending|0|-|-\n",
    )
    .expect("write malformed attempt");
    let error = SignalHarbor::open(journal.path()).err().expect("must fail");
    assert!(matches!(error, HarborError::Journal(_)));
    assert!(error.to_string().contains("line 1"));
    assert!(error.to_string().contains("unknown attempt outcome"));
}
