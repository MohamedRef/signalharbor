use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{BackfillRequest, HarborError, Route, SignalHarbor, WebhookInput};

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal {
    path: PathBuf,
}

impl Journal {
    fn new() -> Self {
        let sequence = JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "signalharbor-backfill-{}-{sequence}.journal",
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

fn route(id: &str, tenant: &str, pattern: &str) -> Route {
    Route::new(
        id,
        tenant,
        pattern,
        &format!("https://hooks.example/{id}"),
        4,
        10,
    )
    .unwrap()
}

fn input(tenant: &str, key: &str, topic: &str, at: u64) -> WebhookInput {
    WebhookInput::new(
        tenant,
        topic,
        key,
        format!("{{\"key\":\"{key}\"}}").into_bytes(),
        at,
    )
    .unwrap()
}

fn request(
    route_id: &str,
    from: u64,
    through: u64,
    due_at: u64,
    maximum: usize,
) -> BackfillRequest {
    BackfillRequest::new(route_id, from, through, due_at, maximum).unwrap()
}

#[test]
fn backfills_events_that_predate_route_registration() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    let first = harbor
        .ingest(input("acme", "one", "orders.created", 100))
        .unwrap();
    let second = harbor
        .ingest(input("acme", "two", "orders.updated", 200))
        .unwrap();
    harbor
        .ingest(input("acme", "three", "customers.created", 150))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();

    let result = harbor
        .backfill_route(&request("orders", 0, 300, 500, 100))
        .unwrap();
    assert_eq!(result.matched_events, 2);
    assert_eq!(result.already_routed, 0);
    assert_eq!(result.created_delivery_ids.len(), 2);
    assert!(!result.truncated);
    let first_delivery = harbor.delivery(&result.created_delivery_ids[0]).unwrap();
    let second_delivery = harbor.delivery(&result.created_delivery_ids[1]).unwrap();
    assert_eq!(first_delivery.event_id, first.event.id);
    assert_eq!(second_delivery.event_id, second.event.id);
    assert_eq!(first_delivery.next_attempt_at, 500);
}

#[test]
fn repeated_backfill_is_idempotent() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor
        .ingest(input("acme", "one", "orders.created", 100))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    let first = harbor
        .backfill_route(&request("orders", 0, 200, 300, 10))
        .unwrap();
    let second = harbor
        .backfill_route(&request("orders", 0, 200, 400, 10))
        .unwrap();
    assert_eq!(first.created_delivery_ids.len(), 1);
    assert!(second.created_delivery_ids.is_empty());
    assert_eq!(second.matched_events, 1);
    assert_eq!(second.already_routed, 1);
    assert_eq!(harbor.summary().deliveries, 1);
}

#[test]
fn window_is_inclusive_and_routing_still_filters_tenant_and_topic() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor
        .ingest(input("acme", "lower", "orders.created", 100))
        .unwrap();
    harbor
        .ingest(input("acme", "upper", "orders.created", 200))
        .unwrap();
    harbor
        .ingest(input("acme", "before", "orders.created", 99))
        .unwrap();
    harbor
        .ingest(input("acme", "other-topic", "billing.created", 150))
        .unwrap();
    harbor
        .ingest(input("other", "other-tenant", "orders.created", 150))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    let result = harbor
        .backfill_route(&request("orders", 100, 200, 300, 10))
        .unwrap();
    assert_eq!(result.matched_events, 2);
    assert_eq!(result.created_delivery_ids.len(), 2);
}

#[test]
fn limit_reports_truncation_and_followup_drains_remaining_events() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    for index in 0..5 {
        harbor
            .ingest(input(
                "acme",
                &format!("key-{index}"),
                "orders.created",
                100 + index,
            ))
            .unwrap();
    }
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    let first = harbor
        .backfill_route(&request("orders", 0, 200, 300, 2))
        .unwrap();
    assert!(first.truncated);
    assert_eq!(first.created_delivery_ids.len(), 2);
    let second = harbor
        .backfill_route(&request("orders", 0, 200, 300, 2))
        .unwrap();
    assert!(second.truncated);
    assert_eq!(second.already_routed, 2);
    let third = harbor
        .backfill_route(&request("orders", 0, 200, 300, 2))
        .unwrap();
    assert!(!third.truncated);
    assert_eq!(third.created_delivery_ids.len(), 1);
    assert_eq!(third.already_routed, 4);
}

#[test]
fn initial_and_backfilled_deliveries_do_not_duplicate_pairs() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor
        .ingest(input("acme", "historical", "orders.created", 100))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    let current = harbor
        .ingest(input("acme", "current", "orders.created", 200))
        .unwrap();
    assert_eq!(current.delivery_ids.len(), 1);
    let result = harbor
        .backfill_route(&request("orders", 0, 300, 400, 10))
        .unwrap();
    assert_eq!(result.matched_events, 2);
    assert_eq!(result.already_routed, 1);
    assert_eq!(result.created_delivery_ids.len(), 1);
    assert_eq!(harbor.summary().deliveries, 2);
}

#[test]
fn backfilled_deliveries_survive_restart_and_sequence_continues() {
    let journal = Journal::new();
    {
        let mut harbor = SignalHarbor::open(journal.path()).unwrap();
        harbor
            .ingest(input("acme", "historical", "orders.created", 100))
            .unwrap();
        harbor
            .register_route(route("orders", "acme", "orders.*"))
            .unwrap();
        let backfill = harbor
            .backfill_route(&request("orders", 0, 200, 300, 10))
            .unwrap();
        assert_eq!(backfill.created_delivery_ids, ["del-000000000001"]);
    }
    let mut reopened = SignalHarbor::open(journal.path()).unwrap();
    assert_eq!(reopened.summary().deliveries, 1);
    let current = reopened
        .ingest(input("acme", "current", "orders.created", 400))
        .unwrap();
    assert_eq!(current.delivery_ids, ["del-000000000002"]);
}

#[test]
fn inactive_and_unknown_routes_fail_without_creating_work() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor
        .ingest(input("acme", "one", "orders.created", 100))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    harbor.set_route_active("orders", false).unwrap();
    assert!(matches!(
        harbor.backfill_route(&request("orders", 0, 200, 300, 10)),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.backfill_route(&request("missing", 0, 200, 300, 10)),
        Err(HarborError::NotFound(_))
    ));
    assert_eq!(harbor.summary().deliveries, 0);
}

#[test]
fn validates_backfill_window_limit_and_route_id() {
    assert!(matches!(
        BackfillRequest::new("orders", 200, 100, 300, 1),
        Err(HarborError::Validation(_))
    ));
    assert!(BackfillRequest::new("orders", 0, 100, 300, 0).is_err());
    assert!(BackfillRequest::new("orders", 0, 100, 300, 10_001).is_err());
    assert!(BackfillRequest::new(" ", 0, 100, 300, 1).is_err());
}

#[test]
fn journal_failure_does_not_expose_partial_backfill() {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).unwrap();
    harbor
        .ingest(input("acme", "one", "orders.created", 100))
        .unwrap();
    harbor
        .register_route(route("orders", "acme", "orders.*"))
        .unwrap();
    fs::remove_file(journal.path()).unwrap();
    fs::create_dir(journal.path()).unwrap();
    assert!(matches!(
        harbor.backfill_route(&request("orders", 0, 200, 300, 10)),
        Err(HarborError::Journal(_))
    ));
    assert_eq!(harbor.summary().deliveries, 0);
}
