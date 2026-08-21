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
            "signalharbor-lease-{}-{sequence}.journal",
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

fn seeded(count: usize) -> (Journal, SignalHarbor, Vec<String>) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open journal");
    harbor
        .register_route(
            Route::new(
                "orders",
                "acme",
                "orders.*",
                "https://hooks.example/orders",
                3,
                10,
            )
            .expect("route"),
        )
        .expect("register route");
    let ids = (0..count)
        .map(|index| {
            harbor
                .ingest(
                    WebhookInput::new(
                        "acme",
                        "orders.created",
                        &format!("event-{index}"),
                        format!("{{\"index\":{index}}}").into_bytes(),
                        100 + index as u64,
                    )
                    .expect("input"),
                )
                .expect("ingest")
                .delivery_ids[0]
                .clone()
        })
        .collect();
    (journal, harbor, ids)
}

#[test]
fn claims_due_deliveries_in_stable_limited_batches() {
    let (_journal, mut harbor, ids) = seeded(3);
    let claimed = harbor.claim_due("worker-a", 200, 2, 30).expect("claim");
    assert_eq!(
        claimed.iter().map(|row| row.id.clone()).collect::<Vec<_>>(),
        ids[..2]
    );
    assert!(claimed.iter().all(|row| {
        row.lease_owner.as_deref() == Some("worker-a") && row.lease_expires_at == Some(230)
    }));
    assert_eq!(
        harbor.due_deliveries(200, 10).expect("due"),
        vec![harbor.delivery(&ids[2]).expect("third")]
    );
    assert_eq!(
        harbor
            .claim_due("worker-b", 200, 10, 30)
            .expect("second claim")[0]
            .id,
        ids[2]
    );
}

#[test]
fn only_the_live_lease_owner_can_record_an_attempt() {
    let (_journal, mut harbor, ids) = seeded(1);
    harbor.claim_due("worker-a", 100, 1, 30).expect("claim");
    assert!(matches!(
        harbor.record_claimed_attempt(&ids[0], "worker-b", 110, AttemptOutcome::Http(204)),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.record_attempt(&ids[0], 110, AttemptOutcome::Http(204)),
        Err(HarborError::Conflict(_))
    ));
    let result = harbor
        .record_claimed_attempt(&ids[0], "worker-a", 110, AttemptOutcome::Http(204))
        .expect("owner attempt");
    assert_eq!(result.delivery.state, DeliveryState::Succeeded);
    assert!(result.delivery.lease_owner.is_none());
    assert!(result.delivery.lease_expires_at.is_none());
}

#[test]
fn expired_lease_is_recoverable_by_another_worker() {
    let (_journal, mut harbor, ids) = seeded(1);
    harbor.claim_due("worker-a", 100, 1, 10).expect("claim");
    assert!(matches!(
        harbor.record_claimed_attempt(&ids[0], "worker-a", 110, AttemptOutcome::Http(200)),
        Err(HarborError::Conflict(_))
    ));
    let reclaimed = harbor.claim_due("worker-b", 110, 1, 20).expect("reclaim");
    assert_eq!(reclaimed[0].lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(reclaimed[0].lease_expires_at, Some(130));
    let result = harbor
        .record_claimed_attempt(&ids[0], "worker-b", 120, AttemptOutcome::Http(503))
        .expect("retry");
    assert!(result.retried);
    assert_eq!(result.delivery.next_attempt_at, 130);
    assert!(result.delivery.lease_owner.is_none());
}

#[test]
fn lease_state_survives_restart() {
    let (journal, mut harbor, ids) = seeded(1);
    harbor.claim_due("worker-a", 100, 1, 60).expect("claim");
    drop(harbor);

    let mut restored = SignalHarbor::open(journal.path()).expect("restore");
    let delivery = restored.delivery(&ids[0]).expect("delivery");
    assert_eq!(delivery.lease_owner.as_deref(), Some("worker-a"));
    assert_eq!(delivery.lease_expires_at, Some(160));
    assert!(restored.due_deliveries(120, 10).expect("due").is_empty());
    assert!(matches!(
        restored.record_attempt(&ids[0], 120, AttemptOutcome::Http(200)),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(
        restored.claim_due("worker-b", 160, 1, 30).expect("reclaim")[0]
            .lease_owner
            .as_deref(),
        Some("worker-b")
    );
}

#[test]
fn validates_claim_parameters_and_worker_names() {
    let (_journal, mut harbor, _ids) = seeded(1);
    assert!(matches!(
        harbor.claim_due("", 100, 1, 30),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.claim_due("worker", 100, 0, 30),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.claim_due("worker", 100, 1, 0),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.claim_due("worker", 100, 1, 3_601),
        Err(HarborError::Validation(_))
    ));
}

#[test]
fn old_delivery_records_replay_without_lease_fields() {
    let journal = Journal::new();
    let route = "R|6f7264657273|61636d65|6f72646572732e2a|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|3|10|1";
    let event = "E|6576742d303030303030303030303031|61636d65|6f72646572732e63726561746564|6f6c642d6b6579|7b7d|100";
    let delivery = "D|64656c2d303030303030303030303031|6576742d303030303030303030303031|6f7264657273|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|pending|0|100|-";
    fs::write(journal.path(), format!("{route}\n{event}\n{delivery}\n"))
        .expect("write old journal");
    let harbor = SignalHarbor::open(journal.path()).expect("replay old journal");
    let restored = harbor.delivery("del-000000000001").expect("delivery");
    assert!(restored.lease_owner.is_none());
    assert!(restored.lease_expires_at.is_none());
    assert!(restored.due(100));
}
