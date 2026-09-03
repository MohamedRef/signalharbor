use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{
    AttemptOutcome, BackfillRequest, DeliverySearchQuery, DeliveryState, HarborError, Route,
    SignalHarbor, WebhookInput,
};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "signalharbor-search-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn journal(&self) -> PathBuf {
        self.0.join("search.journal")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn route(id: &str, tenant: &str) -> Route {
    Route::new(id, tenant, "orders.*", "https://receiver.example", 3, 10).unwrap()
}

fn ingest(harbor: &mut SignalHarbor, key: &str, tenant: &str, time: u64) -> String {
    harbor
        .ingest(WebhookInput::new(tenant, "orders.created", key, b"body", time).unwrap())
        .unwrap()
        .delivery_ids[0]
        .clone()
}

fn seeded() -> (Workspace, SignalHarbor, Vec<String>) {
    let workspace = Workspace::new();
    let mut harbor = SignalHarbor::open(workspace.journal()).unwrap();
    harbor.register_route(route("primary", "acme")).unwrap();
    harbor
        .register_route(route("private-other", "other"))
        .unwrap();
    let ids = vec![
        ingest(&mut harbor, "one", "acme", 30),
        ingest(&mut harbor, "two", "acme", 10),
        ingest(&mut harbor, "three", "acme", 30),
    ];
    ingest(&mut harbor, "other", "other", 5);
    (workspace, harbor, ids)
}

#[test]
fn inspection_is_tenant_scoped_and_orders_received_time_then_identifier() {
    let (workspace, harbor, ids) = seeded();
    let before = fs::read(workspace.journal()).unwrap();
    let query = DeliverySearchQuery::new(" ACME ", 10).unwrap();
    let page = harbor.search_deliveries(&query, None).unwrap();
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.delivery.id.clone())
            .collect::<Vec<_>>(),
        [ids[1].clone(), ids[0].clone(), ids[2].clone()]
    );
    assert!(
        page.rows
            .iter()
            .all(|row| row.tenant == "acme" && row.topic == "orders.created")
    );
    assert!(page.next_cursor.is_none());
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
}

#[test]
fn traverses_all_pages_without_duplicate_rows_and_survives_reopen() {
    let (workspace, harbor, ids) = seeded();
    let query = DeliverySearchQuery::new("acme", 1).unwrap();
    let first = harbor.search_deliveries(&query, None).unwrap();
    assert_eq!(first.rows[0].delivery.id, ids[1]);
    let reopened = SignalHarbor::open(workspace.journal()).unwrap();
    let second = reopened
        .search_deliveries(&query, first.next_cursor.as_ref())
        .unwrap();
    assert_eq!(second.rows[0].delivery.id, ids[0]);
    let third = reopened
        .search_deliveries(&query, second.next_cursor.as_ref())
        .unwrap();
    assert_eq!(third.rows[0].delivery.id, ids[2]);
    assert!(third.next_cursor.is_none());
}

#[test]
fn continuation_excludes_later_ingress_even_with_an_older_receive_time() {
    let (_workspace, mut harbor, ids) = seeded();
    let query = DeliverySearchQuery::new("acme", 1).unwrap();
    let first = harbor.search_deliveries(&query, None).unwrap();
    let new_id = ingest(&mut harbor, "later-upload", "acme", 20);
    let second = harbor
        .search_deliveries(&query, first.next_cursor.as_ref())
        .unwrap();
    assert_eq!(second.rows[0].delivery.id, ids[0]);
    let third = harbor
        .search_deliveries(&query, second.next_cursor.as_ref())
        .unwrap();
    assert_eq!(third.rows[0].delivery.id, ids[2]);
    assert!(third.next_cursor.is_none());
    let fresh = DeliverySearchQuery::new("acme", 10).unwrap();
    assert!(
        harbor
            .search_deliveries(&fresh, None)
            .unwrap()
            .rows
            .iter()
            .any(|row| row.delivery.id == new_id)
    );
}

#[test]
fn continuation_excludes_later_backfill_for_preexisting_events() {
    let (_workspace, mut harbor, ids) = seeded();
    let query = DeliverySearchQuery::new("acme", 2).unwrap();
    let first = harbor.search_deliveries(&query, None).unwrap();
    harbor.register_route(route("new-route", "acme")).unwrap();
    let added = harbor
        .backfill_route(&BackfillRequest::new("new-route", 0, 30, 40, 10).unwrap())
        .unwrap();
    assert_eq!(added.created_delivery_ids.len(), 3);
    let last = harbor
        .search_deliveries(&query, first.next_cursor.as_ref())
        .unwrap();
    assert_eq!(last.rows.len(), 1);
    assert_eq!(last.rows[0].delivery.id, ids[2]);
    assert!(last.next_cursor.is_none());
}

#[test]
fn filters_combine_route_topic_state_and_inclusive_receive_window() {
    let (_workspace, mut harbor, ids) = seeded();
    harbor
        .record_attempt(&ids[0], 30, AttemptOutcome::Http(204))
        .unwrap();
    let mut query = DeliverySearchQuery::new("acme", 10).unwrap();
    query.route_id = Some(" primary ".into());
    query.topic = Some(" ORDERS.CREATED ".into());
    query.state = Some(DeliveryState::Succeeded);
    query.received_from = Some(30);
    query.received_through = Some(30);
    let page = harbor.search_deliveries(&query, None).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].delivery.id, ids[0]);
    query.route_id = Some("private-other".into());
    assert!(
        harbor
            .search_deliveries(&query, None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn search_includes_terminal_and_live_leased_work_not_only_due_queue() {
    let (_workspace, mut harbor, ids) = seeded();
    harbor
        .record_attempt(&ids[0], 30, AttemptOutcome::Http(400))
        .unwrap();
    harbor
        .cancel_delivery(&ids[2], "operator", "duplicate business request", 30)
        .unwrap();
    harbor.claim_due("worker", 40, 10, 20).unwrap();
    let page = harbor
        .search_deliveries(&DeliverySearchQuery::new("acme", 10).unwrap(), None)
        .unwrap();
    assert_eq!(page.rows.len(), 3);
    assert!(
        page.rows
            .iter()
            .any(|row| row.delivery.state == DeliveryState::Cancelled)
    );
    assert!(
        page.rows
            .iter()
            .any(|row| row.delivery.state == DeliveryState::DeadLetter)
    );
    assert!(
        page.rows
            .iter()
            .any(|row| row.delivery.lease_owner.as_deref() == Some("worker"))
    );
}

#[test]
fn cursor_rejects_changed_query_scope_or_page_size() {
    let (_workspace, harbor, _) = seeded();
    let query = DeliverySearchQuery::new("acme", 1).unwrap();
    let page = harbor.search_deliveries(&query, None).unwrap();
    let mut other_tenant = query.clone();
    other_tenant.tenant = "other".into();
    let mut other_state = query.clone();
    other_state.state = Some(DeliveryState::Pending);
    let mut other_size = query.clone();
    other_size.page_size = 2;
    for changed in [other_tenant, other_state, other_size] {
        assert!(matches!(
            harbor.search_deliveries(&changed, page.next_cursor.as_ref()),
            Err(HarborError::Conflict(_))
        ));
    }
}

#[test]
fn mutable_fields_are_live_while_paging_and_results_are_defensive_copies() {
    let (_workspace, mut harbor, ids) = seeded();
    let query = DeliverySearchQuery::new("acme", 1).unwrap();
    let first = harbor.search_deliveries(&query, None).unwrap();
    harbor
        .record_attempt(&ids[0], 30, AttemptOutcome::Http(204))
        .unwrap();
    let mut second = harbor
        .search_deliveries(&query, first.next_cursor.as_ref())
        .unwrap();
    assert_eq!(second.rows[0].delivery.state, DeliveryState::Succeeded);
    second.rows[0].delivery.endpoint = "https://changed.example".into();
    assert_eq!(
        harbor.delivery(&ids[0]).unwrap().endpoint,
        "https://receiver.example"
    );
}

#[test]
fn validates_every_public_query_before_search_and_handles_no_matches() {
    let (_workspace, harbor, _) = seeded();
    assert!(DeliverySearchQuery::new("", 1).is_err());
    assert!(DeliverySearchQuery::new("acme", 0).is_err());
    assert!(DeliverySearchQuery::new("acme", 1_001).is_err());
    let mut query = DeliverySearchQuery::new("acme", 1).unwrap();
    query.received_from = Some(50);
    query.received_through = Some(10);
    assert!(matches!(
        harbor.search_deliveries(&query, None),
        Err(HarborError::Validation(_))
    ));
    query.received_through = None;
    let page = harbor.search_deliveries(&query, None).unwrap();
    assert!(page.rows.is_empty());
    assert!(page.next_cursor.is_none());
    query.page_size = 0;
    assert!(harbor.search_deliveries(&query, None).is_err());
}
