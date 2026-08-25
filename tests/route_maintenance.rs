use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-maintenance-{}-{sequence}.journal",
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

fn route(id: &str, topic: &str) -> Route {
    Route::new(
        id,
        "acme",
        topic,
        &format!("https://hooks.example/{id}"),
        3,
        10,
    )
    .expect("route")
}

fn seeded() -> (Journal, SignalHarbor, String) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open");
    harbor
        .register_route(route("orders", "orders.*"))
        .expect("register");
    let delivery_id = harbor
        .ingest(
            WebhookInput::new(
                "acme",
                "orders.created",
                "event-1",
                br#"{"order":"A-42"}"#.to_vec(),
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
fn pauses_due_and_claim_queues_only_inside_the_window() {
    let (_journal, mut harbor, delivery_id) = seeded();
    let window = harbor
        .schedule_route_maintenance(
            "orders",
            120,
            180,
            "operator",
            "Receiver database upgrade",
            110,
        )
        .expect("schedule");
    assert_eq!(window.id, "maintenance-1");
    assert_eq!(harbor.due_deliveries(119, 10).expect("before").len(), 1);
    assert!(harbor.due_deliveries(120, 10).expect("start").is_empty());
    assert!(
        harbor
            .claim_due("worker", 150, 10, 30)
            .expect("claim")
            .is_empty()
    );
    assert_eq!(harbor.due_deliveries(180, 10).expect("after").len(), 1);
    assert_eq!(harbor.delivery(&delivery_id).expect("delivery").attempts, 0);
}

#[test]
fn blocks_direct_attempts_during_maintenance_without_mutation() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor
        .schedule_route_maintenance("orders", 120, 180, "operator", "Cutover", 110)
        .expect("schedule");
    let before = harbor.delivery(&delivery_id).expect("delivery");
    let error = harbor
        .record_attempt(&delivery_id, 150, AttemptOutcome::Http(204))
        .expect_err("maintenance must block attempt");
    assert!(
        matches!(error, HarborError::Conflict(message) if message.contains("maintenance until 180"))
    );
    assert_eq!(harbor.delivery(&delivery_id).expect("delivery"), before);
    assert!(
        harbor
            .attempt_history(&delivery_id)
            .expect("history")
            .is_empty()
    );
}

#[test]
fn cancellation_restores_availability_and_survives_restart() {
    let (journal, mut harbor, _) = seeded();
    let window = harbor
        .schedule_route_maintenance("orders", 120, 180, "operator", "Cutover", 110)
        .expect("schedule");
    let cancelled = harbor
        .cancel_route_maintenance(
            "orders",
            &window.id,
            "incident lead",
            "Upgrade postponed",
            130,
        )
        .expect("cancel");
    assert_eq!(cancelled.cancelled_by.as_deref(), Some("incident lead"));
    assert_eq!(
        cancelled.cancellation_reason.as_deref(),
        Some("Upgrade postponed")
    );
    assert_eq!(harbor.due_deliveries(150, 10).expect("available").len(), 1);
    drop(harbor);

    let restored = SignalHarbor::open(journal.path()).expect("restore");
    assert_eq!(
        restored.route_maintenance("orders").expect("history"),
        vec![cancelled]
    );
    assert_eq!(restored.due_deliveries(150, 10).expect("due").len(), 1);
}

#[test]
fn rejects_overlaps_and_allows_adjacent_or_cancelled_replacements() {
    let (_journal, mut harbor, _) = seeded();
    let first = harbor
        .schedule_route_maintenance("orders", 120, 180, "operator", "First", 100)
        .expect("first");
    assert!(matches!(
        harbor.schedule_route_maintenance("orders", 150, 200, "operator", "Overlap", 100),
        Err(HarborError::Conflict(_))
    ));
    harbor
        .schedule_route_maintenance("orders", 180, 200, "operator", "Adjacent", 100)
        .expect("adjacent");
    harbor
        .cancel_route_maintenance("orders", &first.id, "operator", "Replanned", 110)
        .expect("cancel");
    harbor
        .schedule_route_maintenance("orders", 130, 170, "operator", "Replacement", 120)
        .expect("replacement");
    assert_eq!(
        harbor.route_maintenance("orders").expect("history").len(),
        3
    );
}

#[test]
fn windows_are_route_scoped_and_history_is_deterministic() {
    let (_journal, mut harbor, _) = seeded();
    harbor
        .register_route(route("billing", "billing.*"))
        .expect("billing route");
    harbor
        .ingest(
            WebhookInput::new("acme", "billing.posted", "bill-1", b"{}".to_vec(), 100)
                .expect("input"),
        )
        .expect("ingest");
    harbor
        .schedule_route_maintenance("orders", 150, 180, "operator", "Later", 100)
        .expect("later");
    harbor
        .schedule_route_maintenance("orders", 120, 140, "operator", "Earlier", 100)
        .expect("earlier");
    let history = harbor.route_maintenance("orders").expect("history");
    assert_eq!(
        history
            .iter()
            .map(|window| window.reason.as_str())
            .collect::<Vec<_>>(),
        vec!["Earlier", "Later"]
    );
    let due = harbor.due_deliveries(130, 10).expect("due");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].route_id, "billing");
}

#[test]
fn validates_timing_context_and_terminal_cancellation() {
    let (_journal, mut harbor, _) = seeded();
    for (start, end, scheduled) in [
        (99, 120, 100),
        (120, 120, 100),
        (120, 86_521, 100),
        (2_592_101, 2_592_120, 100),
    ] {
        assert!(matches!(
            harbor
                .schedule_route_maintenance("orders", start, end, "operator", "Window", scheduled),
            Err(HarborError::Validation(_))
        ));
    }
    assert!(matches!(
        harbor.schedule_route_maintenance("missing", 120, 180, "operator", "Window", 100),
        Err(HarborError::NotFound(_))
    ));
    assert!(
        harbor
            .schedule_route_maintenance("orders", 120, 180, "", "Window", 100)
            .is_err()
    );
    let window = harbor
        .schedule_route_maintenance("orders", 120, 180, "operator", "Window", 100)
        .expect("window");
    assert!(matches!(
        harbor.cancel_route_maintenance("orders", &window.id, "operator", "Late", 180),
        Err(HarborError::Conflict(_))
    ));
    let cancelled = harbor
        .cancel_route_maintenance("orders", &window.id, "operator", "Withdrawn", 110)
        .expect("cancel");
    assert!(matches!(
        harbor.cancel_route_maintenance("orders", &cancelled.id, "operator", "Again", 115),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.route_maintenance("missing"),
        Err(HarborError::NotFound(_))
    ));
}
