use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, HarborError, Route, SignalHarbor, WebhookInput};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "signalharbor-renew-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn journal(&self) -> PathBuf {
        self.0.join("leases.journal")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn claimed(now: u64) -> (Workspace, SignalHarbor, String) {
    let workspace = Workspace::new();
    let mut harbor = SignalHarbor::open(workspace.journal()).unwrap();
    harbor
        .register_route(
            Route::new(
                "route",
                "acme",
                "orders.*",
                "https://receiver.example",
                3,
                10,
            )
            .unwrap(),
        )
        .unwrap();
    let id = harbor
        .ingest(WebhookInput::new("acme", "orders.created", "key", b"body", now).unwrap())
        .unwrap()
        .delivery_ids[0]
        .clone();
    harbor.claim_due("worker-a", now, 1, 20).unwrap();
    (workspace, harbor, id)
}

#[test]
fn renews_durably_without_changing_attempt_or_retry_metadata() {
    let (workspace, mut harbor, id) = claimed(100);
    let original = harbor.delivery(&id).unwrap();
    let renewed = harbor.renew_claim(&id, " worker-a ", 120, 110, 30).unwrap();
    assert_eq!(renewed.lease_expires_at, Some(140));
    assert_eq!(renewed.attempts, original.attempts);
    assert_eq!(renewed.next_attempt_at, original.next_attempt_at);
    assert_eq!(renewed.endpoint, original.endpoint);
    assert!(harbor.attempt_history(&id).unwrap().is_empty());
    assert!(harbor.claim_due("worker-b", 120, 1, 20).unwrap().is_empty());
    let mut reopened = SignalHarbor::open(workspace.journal()).unwrap();
    assert_eq!(reopened.delivery(&id).unwrap(), renewed);
    assert_eq!(
        reopened.claim_due("worker-b", 140, 1, 20).unwrap()[0].id,
        id
    );
}

#[test]
fn stale_expected_expiry_cannot_overwrite_a_newer_renewal() {
    let (workspace, mut harbor, id) = claimed(100);
    harbor.renew_claim(&id, "worker-a", 120, 110, 30).unwrap();
    let before = fs::read(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.renew_claim(&id, "worker-a", 120, 115, 50),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(harbor.delivery(&id).unwrap().lease_expires_at, Some(140));
}

#[test]
fn expired_or_wrong_worker_claims_cannot_be_renewed() {
    let (_workspace, mut harbor, id) = claimed(100);
    for (worker, now) in [("worker-b", 110), ("worker-a", 120), ("worker-a", 121)] {
        assert!(matches!(
            harbor.renew_claim(&id, worker, 120, now, 30),
            Err(HarborError::Conflict(_))
        ));
    }
    assert_eq!(harbor.delivery(&id).unwrap().lease_expires_at, Some(120));
}

#[test]
fn a_reclaimed_delivery_rejects_the_former_lease_even_for_the_same_worker() {
    let (_workspace, mut harbor, id) = claimed(100);
    harbor.claim_due("worker-a", 130, 1, 20).unwrap();
    assert!(matches!(
        harbor.renew_claim(&id, "worker-a", 120, 135, 40),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(
        harbor
            .renew_claim(&id, "worker-a", 150, 140, 30)
            .unwrap()
            .lease_expires_at,
        Some(170)
    );
}

#[test]
fn rejects_invalid_durations_backdated_or_nonextending_deadlines() {
    let (workspace, mut harbor, id) = claimed(100);
    let before = fs::read(workspace.journal()).unwrap();
    for (now, seconds) in [(110, 0), (110, 3_601), (110, 10), (110, 9), (99, 40)] {
        assert!(matches!(
            harbor.renew_claim(&id, "worker-a", 120, now, seconds),
            Err(HarborError::Validation(_))
        ));
    }
    assert!(harbor.renew_claim(&id, "", 120, 110, 40).is_err());
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
}

#[test]
fn checked_expiry_arithmetic_rejects_overflow() {
    let (_workspace, mut harbor, id) = claimed(u64::MAX - 30);
    assert!(matches!(
        harbor.renew_claim(&id, "worker-a", u64::MAX - 10, u64::MAX - 20, 30),
        Err(HarborError::Validation(_))
    ));
    assert_eq!(
        harbor.delivery(&id).unwrap().lease_expires_at,
        Some(u64::MAX - 10)
    );
}

#[test]
fn terminal_and_unknown_deliveries_cannot_receive_renewals() {
    let (_workspace, mut harbor, id) = claimed(100);
    harbor
        .record_claimed_attempt(&id, "worker-a", 110, AttemptOutcome::Http(204))
        .unwrap();
    assert!(matches!(
        harbor.renew_claim(&id, "worker-a", 120, 115, 30),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.renew_claim("unknown", "worker-a", 120, 115, 30),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn journal_failure_leaves_original_live_lease_unchanged() {
    let (workspace, mut harbor, id) = claimed(100);
    let original = harbor.delivery(&id).unwrap();
    let saved = workspace.0.join("saved");
    fs::rename(workspace.journal(), &saved).unwrap();
    fs::create_dir(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.renew_claim(&id, "worker-a", 120, 110, 40),
        Err(HarborError::Journal(_))
    ));
    assert_eq!(harbor.delivery(&id).unwrap(), original);
    fs::remove_dir(workspace.journal()).unwrap();
    fs::rename(saved, workspace.journal()).unwrap();
    assert_eq!(
        SignalHarbor::open(workspace.journal())
            .unwrap()
            .delivery(&id)
            .unwrap(),
        original
    );
}
