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
            "signalharbor-ingress-freeze-{}-{sequence}.journal",
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

fn route(id: &str, tenant: &str) -> Route {
    Route::new(
        id,
        tenant,
        "orders.*",
        &format!("https://hooks.example/{id}"),
        3,
        10,
    )
    .expect("route")
}

fn input(tenant: &str, key: &str, received_at: u64) -> WebhookInput {
    WebhookInput::new(
        tenant,
        "orders.created",
        key,
        format!(r#"{{"key":"{key}"}}"#).into_bytes(),
        received_at,
    )
    .expect("input")
}

fn seeded() -> (Journal, SignalHarbor) {
    let journal = Journal::new();
    let mut harbor = SignalHarbor::open(journal.path()).expect("open");
    harbor
        .register_route(route("orders", "acme"))
        .expect("register");
    (journal, harbor)
}

#[test]
fn blocks_only_new_events_inside_the_tenant_freeze() {
    let (_journal, mut harbor) = seeded();
    let original = harbor
        .ingest(input("acme", "accepted", 90))
        .expect("ingest");
    let freeze = harbor
        .impose_ingress_freeze(
            "ACME",
            100,
            200,
            "incident commander",
            "Validate unexpected source volume",
            95,
        )
        .expect("freeze");
    assert_eq!(freeze.id, "freeze-000000000001");

    let before = harbor.ingest(input("acme", "before", 99)).expect("before");
    assert!(!before.duplicate);
    let blocked = harbor
        .ingest(input("acme", "blocked", 100))
        .expect_err("freeze must reject a new event");
    assert!(
        matches!(blocked, HarborError::Conflict(message) if message.contains("frozen until 200"))
    );

    let duplicate = harbor
        .ingest(input("acme", "accepted", 150))
        .expect("idempotent duplicate");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.event.id, original.event.id);
    assert!(harbor.ingest(input("acme", "after", 200)).is_ok());
    assert_eq!(harbor.summary().events, 3);
}

#[test]
fn freeze_is_tenant_scoped_and_active_queue_is_deterministic() {
    let (_journal, mut harbor) = seeded();
    harbor
        .register_route(route("globex-orders", "globex"))
        .expect("globex route");
    let later = harbor
        .impose_ingress_freeze("acme", 100, 180, "operator", "Acme incident", 90)
        .expect("acme freeze");
    let earlier = harbor
        .impose_ingress_freeze("globex", 100, 150, "operator", "Globex incident", 90)
        .expect("globex freeze");

    assert!(harbor.ingest(input("acme", "acme-1", 120)).is_err());
    assert!(harbor.ingest(input("globex", "globex-1", 120)).is_err());
    let active = harbor.active_ingress_freezes(120);
    assert_eq!(
        active.iter().map(|freeze| &freeze.id).collect::<Vec<_>>(),
        vec![&earlier.id, &later.id]
    );
    assert_eq!(harbor.active_ingress_freezes(180), Vec::new());
}

#[test]
fn lifting_restores_ingress_and_survives_restart() {
    let (journal, mut harbor) = seeded();
    let freeze = harbor
        .impose_ingress_freeze("acme", 120, 240, "operator", "Containment", 100)
        .expect("freeze");
    let lifted = harbor
        .lift_ingress_freeze(
            "ACME",
            &freeze.id,
            "incident commander",
            "Source was isolated",
            150,
        )
        .expect("lift");
    assert_eq!(lifted.lifted_at, Some(150));
    assert_eq!(lifted.lifted_by.as_deref(), Some("incident commander"));
    assert_eq!(lifted.lift_reason.as_deref(), Some("Source was isolated"));
    assert!(harbor.ingest(input("acme", "restored", 151)).is_ok());
    drop(harbor);

    let mut restored = SignalHarbor::open(journal.path()).expect("reopen");
    assert_eq!(
        restored.tenant_ingress_freezes("acme").expect("history"),
        vec![lifted]
    );
    assert!(restored.ingest(input("acme", "after-restart", 160)).is_ok());
    let next = restored
        .impose_ingress_freeze("acme", 170, 180, "operator", "New incident", 160)
        .expect("next freeze");
    assert_eq!(next.id, "freeze-000000000002");
}

#[test]
fn rejects_overlaps_but_allows_adjacent_and_lifted_replacements() {
    let (_journal, mut harbor) = seeded();
    let first = harbor
        .impose_ingress_freeze("acme", 120, 180, "operator", "First", 100)
        .expect("first");
    assert!(matches!(
        harbor.impose_ingress_freeze("acme", 150, 200, "operator", "Overlap", 100),
        Err(HarborError::Conflict(_))
    ));
    harbor
        .impose_ingress_freeze("acme", 180, 200, "operator", "Adjacent", 100)
        .expect("adjacent");
    harbor
        .lift_ingress_freeze("acme", &first.id, "operator", "Recovered early", 130)
        .expect("lift");
    harbor
        .impose_ingress_freeze("acme", 140, 170, "operator", "Replacement", 135)
        .expect("replacement");
    let history = harbor.tenant_ingress_freezes("acme").expect("history");
    assert_eq!(
        history
            .iter()
            .map(|freeze| freeze.reason.as_str())
            .collect::<Vec<_>>(),
        vec!["First", "Replacement", "Adjacent"]
    );
}

#[test]
fn validates_tenant_context_intervals_and_lift_lifecycle() {
    let (_journal, mut harbor) = seeded();
    for (start, expiry, imposed) in [
        (99, 120, 100),
        (120, 120, 100),
        (120, 604_921, 100),
        (2_592_101, 2_592_120, 100),
    ] {
        assert!(matches!(
            harbor.impose_ingress_freeze("acme", start, expiry, "operator", "Incident", imposed),
            Err(HarborError::Validation(_))
        ));
    }
    assert!(matches!(
        harbor.impose_ingress_freeze("missing", 120, 180, "operator", "Incident", 100),
        Err(HarborError::NotFound(_))
    ));
    assert!(
        harbor
            .impose_ingress_freeze("acme", 120, 180, "", "Incident", 100)
            .is_err()
    );
    let freeze = harbor
        .impose_ingress_freeze("acme", 120, 180, "operator", "Incident", 100)
        .expect("freeze");
    assert!(matches!(
        harbor.lift_ingress_freeze("acme", &freeze.id, "operator", "Too early", 99),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.lift_ingress_freeze("acme", &freeze.id, "operator", "Too late", 180),
        Err(HarborError::Conflict(_))
    ));
    let lifted = harbor
        .lift_ingress_freeze("acme", &freeze.id, "operator", "Recovered", 130)
        .expect("lift");
    assert!(matches!(
        harbor.lift_ingress_freeze("acme", &lifted.id, "operator", "Again", 140),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        harbor.tenant_ingress_freezes("missing"),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn corrupt_freeze_metadata_is_rejected_on_replay() {
    let journal = Journal::new();
    let route_record = "R|6f7264657273|61636d65|6f72646572732e2a|68747470733a2f2f686f6f6b732e6578616d706c652f6f7264657273|3|10|1";
    let corrupt_freeze = "F|667265657a652d303030303030303030303031|1|61636d65|120|180|6f70657261746f72|696e636964656e74|100|130|-|7265636f7665726564";
    fs::write(
        journal.path(),
        format!("{route_record}\n{corrupt_freeze}\n"),
    )
    .expect("write journal");
    assert!(matches!(
        SignalHarbor::open(journal.path()),
        Err(HarborError::Journal(message)) if message.contains("incomplete lift metadata")
    ));
}
