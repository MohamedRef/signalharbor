use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "signalharbor-claim-release-{label}-{}-{}.journal",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn pending(label: &str) -> (SignalHarbor, PathBuf, String) {
    let journal = path(label);
    let mut harbor = SignalHarbor::open(&journal).unwrap();
    harbor
        .register_route(
            Route::new(
                "orders",
                "tenant-a",
                "orders.*",
                "https://example.test/hooks",
                3,
                10,
            )
            .unwrap(),
        )
        .unwrap();
    let input =
        WebhookInput::new("tenant-a", "orders.created", label, b"{}".to_vec(), 100).unwrap();
    let delivery_id = harbor.ingest(input).unwrap().delivery_ids[0].clone();
    (harbor, journal, delivery_id)
}

#[test]
fn worker_releases_claim_to_a_bounded_future_time() {
    let (mut harbor, journal, delivery_id) = pending("normal");
    let claim = harbor.claim_due("worker-a", 100, 1, 60).unwrap();
    assert_eq!(claim[0].lease_expires_at, Some(160));

    let released = harbor
        .release_claim(&delivery_id, "worker-a", 120, 150)
        .unwrap();
    assert_eq!(released.state, DeliveryState::Pending);
    assert_eq!(released.next_attempt_at, 150);
    assert!(released.lease_owner.is_none());
    assert!(harbor.due_deliveries(149, 10).unwrap().is_empty());
    assert_eq!(harbor.due_deliveries(150, 10).unwrap()[0].id, delivery_id);
    assert!(harbor.attempt_history(&delivery_id).unwrap().is_empty());
    std::fs::remove_file(journal).ok();
}

#[test]
fn released_claim_survives_restart_and_can_be_reclaimed() {
    let (mut harbor, journal, delivery_id) = pending("restart");
    harbor.claim_due("worker-a", 100, 1, 60).unwrap();
    harbor
        .release_claim(&delivery_id, "worker-a", 110, 140)
        .unwrap();
    drop(harbor);

    let mut reopened = SignalHarbor::open(&journal).unwrap();
    let restored = reopened.delivery(&delivery_id).unwrap();
    assert_eq!(restored.next_attempt_at, 140);
    assert!(restored.lease_owner.is_none());
    assert!(
        reopened
            .claim_due("worker-b", 139, 1, 30)
            .unwrap()
            .is_empty()
    );
    let claimed = reopened.claim_due("worker-b", 140, 1, 30).unwrap();
    assert_eq!(claimed[0].lease_owner.as_deref(), Some("worker-b"));
    std::fs::remove_file(journal).ok();
}

#[test]
fn wrong_worker_and_expired_lease_cannot_release() {
    let (mut harbor, journal, delivery_id) = pending("ownership");
    harbor.claim_due("worker-a", 100, 1, 20).unwrap();
    assert!(matches!(
        harbor.release_claim(&delivery_id, "worker-b", 110, 115),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.release_claim(&delivery_id, "worker-a", 120, 125),
        Err(HarborError::Conflict(_))
    ));
    let unchanged = harbor.delivery(&delivery_id).unwrap();
    assert_eq!(unchanged.lease_owner.as_deref(), Some("worker-a"));
    assert_eq!(unchanged.lease_expires_at, Some(120));
    std::fs::remove_file(journal).ok();
}

#[test]
fn validates_defer_window_and_delivery_lifecycle() {
    let (mut harbor, journal, delivery_id) = pending("validation");
    harbor.claim_due("worker-a", 100, 1, 60).unwrap();
    assert!(matches!(
        harbor.release_claim(&delivery_id, "worker-a", 110, 109),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.release_claim(&delivery_id, "worker-a", 110, 86_511),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.release_claim("missing", "worker-a", 110, 120),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.release_claim(&delivery_id, " ", 110, 120),
        Err(HarborError::Validation(_))
    ));
    std::fs::remove_file(journal).ok();
}
