use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new(label: &str) -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-hold-{label}-{}-{sequence}.journal",
            std::process::id()
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        if self.0.is_dir() {
            let _ = fs::remove_dir(&self.0);
        } else {
            let _ = fs::remove_file(&self.0);
        }
    }
}

fn seeded(label: &str) -> (Journal, SignalHarbor, String) {
    let journal = Journal::new(label);
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
                &format!("hold-{label}"),
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
fn hold_removes_delivery_from_due_and_claim_queues_until_expiry() {
    let (_journal, mut harbor, delivery_id) = seeded("queue");
    let hold = harbor
        .hold_delivery(
            &delivery_id,
            "risk operator",
            "verify receiver ownership before dispatch",
            110,
            200,
        )
        .expect("hold delivery");

    assert!(hold.active_at(110));
    assert!(hold.active_at(199));
    assert!(!hold.active_at(200));
    assert!(harbor.due_deliveries(150, 10).expect("due").is_empty());
    assert!(
        harbor
            .claim_due("worker-a", 150, 10, 30)
            .expect("claim")
            .is_empty()
    );
    assert_eq!(
        harbor.due_deliveries(200, 10).expect("due after expiry")[0].id,
        delivery_id
    );
}

#[test]
fn explicit_release_is_accountable_and_restores_eligibility() {
    let (_journal, mut harbor, delivery_id) = seeded("release");
    harbor
        .hold_delivery(
            &delivery_id,
            "fraud desk",
            "investigate suspicious subscription change",
            110,
            300,
        )
        .expect("hold");
    let released = harbor
        .release_delivery_hold(&delivery_id, "incident commander", 160)
        .expect("release");

    assert_eq!(released.released_at, Some(160));
    assert_eq!(released.released_by.as_deref(), Some("incident commander"));
    assert_eq!(
        harbor.hold_history(&delivery_id).expect("history"),
        [released]
    );
    assert_eq!(harbor.due_deliveries(160, 1).expect("due").len(), 1);
    assert!(matches!(
        harbor.release_delivery_hold(&delivery_id, "operator", 170),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn hold_and_release_survive_restart_without_losing_history() {
    let (journal, mut harbor, delivery_id) = seeded("restart");
    let first = harbor
        .hold_delivery(
            &delivery_id,
            "operator-a",
            "await vendor response",
            110,
            150,
        )
        .expect("first hold");
    harbor
        .release_delivery_hold(&delivery_id, "operator-b", 130)
        .expect("release first");
    let second = harbor
        .hold_delivery(
            &delivery_id,
            "operator-c",
            "validate new signing key",
            140,
            220,
        )
        .expect("second hold");
    drop(harbor);

    let reopened = SignalHarbor::open(journal.path()).expect("reopen journal");
    let history = reopened.hold_history(&delivery_id).expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, first.id);
    assert_eq!(history[0].released_by.as_deref(), Some("operator-b"));
    assert_eq!(history[1], second);
    assert!(
        reopened
            .due_deliveries(180, 1)
            .expect("held due")
            .is_empty()
    );
    assert_eq!(reopened.due_deliveries(220, 1).expect("expired").len(), 1);
}

#[test]
fn live_lease_blocks_hold_but_expired_lease_is_cleared_atomically() {
    let (journal, mut harbor, delivery_id) = seeded("lease");
    harbor.claim_due("worker-a", 100, 1, 20).expect("claim");
    assert!(matches!(
        harbor.hold_delivery(&delivery_id, "operator", "pause active route", 110, 200),
        Err(HarborError::Conflict(_))
    ));

    harbor
        .hold_delivery(&delivery_id, "operator", "pause expired claim", 120, 200)
        .expect("hold after lease expiry");
    let delivery = harbor.delivery(&delivery_id).expect("delivery");
    assert_eq!(delivery.state, DeliveryState::Pending);
    assert_eq!(delivery.lease_owner, None);
    assert_eq!(delivery.lease_expires_at, None);
    drop(harbor);

    let reopened = SignalHarbor::open(journal.path()).expect("reopen");
    assert_eq!(
        reopened
            .delivery(&delivery_id)
            .expect("delivery")
            .lease_owner,
        None
    );
    assert!(reopened.due_deliveries(150, 1).expect("held").is_empty());
}

#[test]
fn validates_hold_interval_actor_reason_and_delivery_state() {
    let (_journal, mut harbor, delivery_id) = seeded("validation");
    for (actor, reason, held_at, release_at) in [
        ("", "reason", 100, 200),
        ("operator", "", 100, 200),
        ("operator", "reason", 200, 200),
        ("operator", "reason", 200, 804_801),
    ] {
        assert!(matches!(
            harbor.hold_delivery(&delivery_id, actor, reason, held_at, release_at),
            Err(HarborError::Validation(_))
        ));
    }
    harbor
        .hold_delivery(&delivery_id, "operator", "first hold", 110, 200)
        .expect("hold");
    assert!(matches!(
        harbor.hold_delivery(&delivery_id, "operator", "overlap", 120, 210),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.hold_history("missing"),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn terminal_delivery_cannot_be_held() {
    let (_journal, mut harbor, delivery_id) = seeded("terminal");
    harbor
        .cancel_delivery(&delivery_id, "operator", "no longer subscribed", 110)
        .expect("cancel");
    assert!(matches!(
        harbor.hold_delivery(&delivery_id, "operator", "should fail", 120, 200),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn journal_failure_does_not_expose_partial_hold_or_release() {
    let (journal, mut harbor, delivery_id) = seeded("failure");
    fs::remove_file(journal.path()).expect("remove journal");
    fs::create_dir(journal.path()).expect("replace journal with directory");

    assert!(matches!(
        harbor.hold_delivery(&delivery_id, "operator", "must persist", 110, 200),
        Err(HarborError::Journal(_))
    ));
    assert!(
        harbor
            .hold_history(&delivery_id)
            .expect("history")
            .is_empty()
    );
    assert_eq!(harbor.due_deliveries(120, 1).expect("still due").len(), 1);
}

#[test]
fn exhausted_hold_sequence_fails_without_appending_a_duplicate() {
    let (journal, harbor, delivery_id) = seeded("sequence-exhaustion");
    drop(harbor);
    let encode = |value: &str| {
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.as_bytes() {
            write!(&mut encoded, "{byte:02x}").expect("encode text");
        }
        encoded
    };
    let record = format!(
        "H|{}|{}|{}|{}|{}|101|102|101|{}\n",
        encode("hold-exhausted"),
        u64::MAX,
        encode(&delivery_id),
        encode("operator"),
        encode("historical hold"),
        encode("operator"),
    );
    fs::OpenOptions::new()
        .append(true)
        .open(journal.path())
        .expect("open journal")
        .write_all(record.as_bytes())
        .expect("append exhausted sequence");

    let before = fs::read(journal.path()).expect("journal before");
    let mut restored = SignalHarbor::open(journal.path()).expect("restore exhausted sequence");
    let error = restored
        .hold_delivery(&delivery_id, "operator", "new hold", 110, 120)
        .expect_err("exhausted sequence must fail");
    assert!(
        matches!(error, HarborError::Conflict(message) if message.contains("sequence is exhausted"))
    );
    assert_eq!(fs::read(journal.path()).expect("journal after"), before);
    assert_eq!(
        restored.hold_history(&delivery_id).expect("history").len(),
        1
    );
}
