use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new() -> Self {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "signalharbor-concurrency-{}-{sequence}.journal",
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

fn seeded() -> (Journal, SignalHarbor, Vec<String>) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open");
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
        .expect("register");
    let ids = (0..3)
        .map(|index| {
            harbor
                .ingest(
                    WebhookInput::new(
                        "acme",
                        "orders.created",
                        &format!("key-{index}"),
                        format!("{{\"index\":{index}}}").into_bytes(),
                        100 + index,
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
fn route_limit_caps_live_claims_and_recovers_expired_capacity() {
    let (_journal, mut harbor, ids) = seeded();
    let configured = harbor
        .configure_route_concurrency("orders", 1, 200)
        .expect("configure");
    assert_eq!(configured.available, 1);
    let first = harbor.claim_due("worker-a", 200, 10, 30).expect("claim");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].id, ids[0]);
    assert!(
        harbor
            .claim_due("worker-b", 200, 10, 30)
            .expect("blocked")
            .is_empty()
    );
    let health = harbor
        .route_concurrency_health("orders", 200)
        .expect("health");
    assert_eq!((health.leased, health.available), (1, 0));
    let recovered = harbor
        .claim_due("worker-b", 230, 10, 30)
        .expect("recovered");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, ids[0]);
}

#[test]
fn concurrency_limit_survives_restart() {
    let (journal, mut harbor, _) = seeded();
    harbor
        .configure_route_concurrency("orders", 2, 200)
        .expect("configure");
    harbor.claim_due("worker", 200, 10, 60).expect("claim");
    drop(harbor);
    let restored = SignalHarbor::open(journal.path()).expect("restore");
    let route = restored
        .routes()
        .into_iter()
        .find(|route| route.id == "orders")
        .expect("route");
    assert_eq!(route.max_in_flight, 2);
    let health = restored
        .route_concurrency_health("orders", 200)
        .expect("health");
    assert_eq!((health.leased, health.available), (2, 0));
}

#[test]
fn routes_have_independent_capacity() {
    let (_journal, mut harbor, _) = seeded();
    harbor
        .register_route(
            Route::new(
                "billing",
                "acme",
                "billing.*",
                "https://hooks.example/billing",
                3,
                10,
            )
            .expect("route"),
        )
        .expect("register");
    harbor
        .configure_route_concurrency("orders", 1, 200)
        .expect("orders limit");
    harbor
        .configure_route_concurrency("billing", 1, 200)
        .expect("billing limit");
    harbor
        .ingest(
            WebhookInput::new("acme", "billing.posted", "bill", b"{}".to_vec(), 100)
                .expect("input"),
        )
        .expect("ingest");
    let claimed = harbor.claim_due("worker", 200, 10, 30).expect("claim");
    assert_eq!(claimed.len(), 2);
    assert_ne!(claimed[0].route_id, claimed[1].route_id);
}

#[test]
fn validates_limits_and_missing_routes_without_mutation() {
    let (_journal, mut harbor, _) = seeded();
    assert!(matches!(
        harbor.configure_route_concurrency("orders", 0, 100),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.configure_route_concurrency("missing", 1, 100),
        Err(HarborError::NotFound(_))
    ));
    assert!(matches!(
        harbor.route_concurrency_health("missing", 100),
        Err(HarborError::NotFound(_))
    ));
    assert_eq!(harbor.routes()[0].max_in_flight, 1_000);
}
