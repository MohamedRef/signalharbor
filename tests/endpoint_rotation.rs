use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{
    AttemptOutcome, EndpointUpdateMode, HarborError, Route, SignalHarbor, WebhookInput,
};

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal {
    path: PathBuf,
}

impl Journal {
    fn new() -> Self {
        let sequence = JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "signalharbor-endpoint-{}-{sequence}.journal",
                std::process::id()
            )),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn route() -> Route {
    Route::new(
        "orders",
        "acme",
        "orders.*",
        "https://old.example/hooks",
        4,
        10,
    )
    .unwrap()
}

fn webhook(key: &str, at: u64) -> WebhookInput {
    WebhookInput::new(
        "acme",
        "orders.created",
        key,
        format!("{{\"key\":\"{key}\"}}").into_bytes(),
        at,
    )
    .unwrap()
}

fn harbor() -> (Journal, SignalHarbor) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor.register_route(route()).unwrap();
    (journal, harbor)
}

#[test]
fn future_only_rotation_preserves_existing_delivery_snapshots() {
    let (_journal, mut harbor) = harbor();
    let existing = harbor.ingest(webhook("before", 100)).unwrap();
    let result = harbor
        .update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::FutureOnly,
        )
        .unwrap();
    assert!(result.migrated_delivery_ids.is_empty());
    assert_eq!(result.retained_delivery_ids, existing.delivery_ids);
    assert_eq!(
        harbor.delivery(&existing.delivery_ids[0]).unwrap().endpoint,
        "https://old.example/hooks"
    );
    let future = harbor.ingest(webhook("after", 200)).unwrap();
    assert_eq!(
        harbor.delivery(&future.delivery_ids[0]).unwrap().endpoint,
        "https://new.example/hooks"
    );
}

#[test]
fn migration_updates_only_unattempted_unleased_pending_work() {
    let (_journal, mut harbor) = harbor();
    let migratable = harbor.ingest(webhook("migrate", 200)).unwrap();
    let attempted = harbor.ingest(webhook("attempted", 100)).unwrap();
    harbor
        .record_attempt(
            &attempted.delivery_ids[0],
            100,
            AttemptOutcome::Transport("timeout".into()),
        )
        .unwrap();
    let leased = harbor.ingest(webhook("leased", 50)).unwrap();
    let claims = harbor.claim_due("worker-a", 50, 1, 60).unwrap();
    assert_eq!(claims[0].id, leased.delivery_ids[0]);

    let result = harbor
        .update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::MigrateUnattempted,
        )
        .unwrap();
    assert_eq!(result.migrated_delivery_ids, migratable.delivery_ids);
    assert_eq!(
        result.retained_delivery_ids,
        [
            attempted.delivery_ids[0].clone(),
            leased.delivery_ids[0].clone()
        ]
    );
    assert_eq!(
        harbor
            .delivery(&migratable.delivery_ids[0])
            .unwrap()
            .endpoint,
        "https://new.example/hooks"
    );
    assert_eq!(
        harbor
            .delivery(&attempted.delivery_ids[0])
            .unwrap()
            .endpoint,
        "https://old.example/hooks"
    );
}

#[test]
fn terminal_deliveries_retain_their_endpoint() {
    let (_journal, mut harbor) = harbor();
    let succeeded = harbor.ingest(webhook("ok", 100)).unwrap();
    harbor
        .record_attempt(&succeeded.delivery_ids[0], 100, AttemptOutcome::Http(204))
        .unwrap();
    let result = harbor
        .update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::MigrateUnattempted,
        )
        .unwrap();
    assert!(result.migrated_delivery_ids.is_empty());
    assert_eq!(result.retained_delivery_ids, succeeded.delivery_ids);
    assert_eq!(
        harbor
            .delivery(&succeeded.delivery_ids[0])
            .unwrap()
            .endpoint,
        "https://old.example/hooks"
    );
}

#[test]
fn rotation_and_migrations_survive_restart_together() {
    let journal = Journal::new();
    let delivery_id;
    {
        let mut harbor = SignalHarbor::open(journal.path()).unwrap();
        harbor.register_route(route()).unwrap();
        delivery_id = harbor
            .ingest(webhook("persisted", 100))
            .unwrap()
            .delivery_ids[0]
            .clone();
        harbor
            .update_route_endpoint(
                "orders",
                "https://new.example/hooks",
                EndpointUpdateMode::MigrateUnattempted,
            )
            .unwrap();
    }
    let mut reopened = SignalHarbor::open(journal.path()).unwrap();
    assert_eq!(
        reopened.delivery(&delivery_id).unwrap().endpoint,
        "https://new.example/hooks"
    );
    let future = reopened.ingest(webhook("future", 200)).unwrap();
    assert_eq!(
        reopened.delivery(&future.delivery_ids[0]).unwrap().endpoint,
        "https://new.example/hooks"
    );
}

#[test]
fn invalid_or_unknown_rotation_does_not_change_state() {
    let (_journal, mut harbor) = harbor();
    assert!(matches!(
        harbor.update_route_endpoint("orders", "ftp://invalid", EndpointUpdateMode::FutureOnly),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.update_route_endpoint(
            "missing",
            "https://new.example/hooks",
            EndpointUpdateMode::FutureOnly
        ),
        Err(HarborError::NotFound(_))
    ));
    assert_eq!(harbor.routes()[0].endpoint, "https://old.example/hooks");
}

#[test]
fn repeated_rotation_is_idempotent() {
    let (_journal, mut harbor) = harbor();
    let delivery = harbor.ingest(webhook("one", 100)).unwrap();
    let first = harbor
        .update_route_endpoint(
            "orders",
            " https://new.example/hooks ",
            EndpointUpdateMode::MigrateUnattempted,
        )
        .unwrap();
    let second = harbor
        .update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::MigrateUnattempted,
        )
        .unwrap();
    assert_eq!(first.migrated_delivery_ids, delivery.delivery_ids);
    assert_eq!(second.migrated_delivery_ids, delivery.delivery_ids);
    assert!(second.retained_delivery_ids.is_empty());
}

#[test]
fn journal_failure_exposes_no_partial_endpoint_change() {
    let (journal, mut harbor) = harbor();
    let delivery = harbor.ingest(webhook("one", 100)).unwrap();
    fs::remove_file(journal.path()).unwrap();
    fs::create_dir(journal.path()).unwrap();
    assert!(matches!(
        harbor.update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::MigrateUnattempted
        ),
        Err(HarborError::Journal(_))
    ));
    assert_eq!(harbor.routes()[0].endpoint, "https://old.example/hooks");
    assert_eq!(
        harbor.delivery(&delivery.delivery_ids[0]).unwrap().endpoint,
        "https://old.example/hooks"
    );
}

#[test]
fn migration_result_is_stable_by_delivery_identifier() {
    let (_journal, mut harbor) = harbor();
    let first = harbor.ingest(webhook("one", 300)).unwrap();
    let second = harbor.ingest(webhook("two", 100)).unwrap();
    let result = harbor
        .update_route_endpoint(
            "orders",
            "https://new.example/hooks",
            EndpointUpdateMode::MigrateUnattempted,
        )
        .unwrap();
    assert_eq!(
        result.migrated_delivery_ids,
        [
            first.delivery_ids[0].clone(),
            second.delivery_ids[0].clone()
        ]
    );
}
