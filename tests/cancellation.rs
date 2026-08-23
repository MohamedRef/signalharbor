use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
struct Journal(PathBuf);

impl Journal {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-cancel-{}-{sequence}.journal",
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

fn seeded() -> (Journal, SignalHarbor, String) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open journal");
    harbor
        .register_route(
            Route::new(
                "route",
                "acme",
                "orders.*",
                "https://hooks.example/orders",
                3,
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
                "cancel-key",
                b"payload".to_vec(),
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
fn cancellation_is_terminal_audited_and_removed_from_due_work() {
    let (_journal, mut harbor, delivery_id) = seeded();
    let cancellation = harbor
        .cancel_delivery(
            &delivery_id,
            "operator",
            "customer revoked subscription",
            110,
        )
        .expect("cancel");
    assert_eq!(cancellation.delivery_id, delivery_id);
    assert_eq!(
        harbor.delivery(&delivery_id).expect("delivery").state,
        DeliveryState::Cancelled
    );
    assert!(harbor.due_deliveries(200, 10).expect("due").is_empty());
    assert_eq!(harbor.summary().cancelled, 1);
    assert_eq!(
        harbor.cancellation(&delivery_id).expect("metadata"),
        cancellation
    );
    assert!(matches!(
        harbor.cancel_delivery(&delivery_id, "operator", "again", 120),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn live_worker_lease_blocks_cancellation_but_expired_lease_does_not() {
    let (_journal, mut harbor, delivery_id) = seeded();
    harbor.claim_due("worker-a", 100, 1, 20).expect("claim");
    assert!(matches!(
        harbor.cancel_delivery(&delivery_id, "operator", "route retired", 110),
        Err(HarborError::Conflict(_))
    ));
    let cancelled = harbor
        .cancel_delivery(&delivery_id, "operator", "route retired", 120)
        .expect("cancel after expiry");
    assert_eq!(cancelled.cancelled_at, 120);
    let delivery = harbor.delivery(&delivery_id).expect("delivery");
    assert!(delivery.lease_owner.is_none());
    assert!(delivery.lease_expires_at.is_none());
}

#[test]
fn cancellation_and_operator_context_survive_restart() {
    let (journal, mut harbor, delivery_id) = seeded();
    let expected = harbor
        .cancel_delivery(
            &delivery_id,
            "operations-lead",
            "duplicate downstream contract",
            130,
        )
        .expect("cancel");
    drop(harbor);
    let restored = SignalHarbor::open(journal.path()).expect("restore");
    assert_eq!(
        restored.delivery(&delivery_id).expect("delivery").state,
        DeliveryState::Cancelled
    );
    assert_eq!(
        restored.cancellation(&delivery_id).expect("metadata"),
        expected
    );
}

#[test]
fn cancellation_validates_actor_reason_and_known_delivery() {
    let (_journal, mut harbor, delivery_id) = seeded();
    assert!(matches!(
        harbor.cancel_delivery(&delivery_id, "", "reason", 110),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.cancel_delivery(&delivery_id, "actor", "", 110),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.cancel_delivery("missing", "actor", "reason", 110),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.cancellation(&delivery_id),
        Err(HarborError::NotFound(_))
    ));
}
