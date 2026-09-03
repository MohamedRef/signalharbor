use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{HarborError, Route, SignalHarbor, WebhookInput};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "signalharbor-batch-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create test workspace");
        Self(path)
    }

    fn journal(&self) -> PathBuf {
        self.0.join("events.journal")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn seeded(route_count: usize) -> (Workspace, SignalHarbor) {
    let workspace = Workspace::new();
    let mut harbor = SignalHarbor::open(workspace.journal()).expect("open");
    for index in 0..route_count {
        harbor
            .register_route(
                Route::new(
                    &format!("route-{index}"),
                    "acme",
                    "orders.*",
                    "https://receiver.example/events",
                    3,
                    10,
                )
                .expect("route"),
            )
            .expect("register");
    }
    (workspace, harbor)
}

fn input(key: &str, time: u64) -> WebhookInput {
    WebhookInput::new("acme", "orders.created", key, b"body", time).expect("input")
}

#[test]
fn batch_routes_in_input_order_and_replays_as_one_transaction() {
    let (workspace, mut harbor) = seeded(2);
    let before = fs::read_to_string(workspace.journal())
        .unwrap()
        .lines()
        .count();
    let results = harbor
        .ingest_batch(&[input("first", 20), input("second", 10)])
        .unwrap();
    assert_eq!(results[0].event.id, "evt-000000000001");
    assert_eq!(results[1].event.id, "evt-000000000002");
    assert_eq!(
        results[0].delivery_ids,
        ["del-000000000001", "del-000000000002"]
    );
    assert_eq!(
        results[1].delivery_ids,
        ["del-000000000003", "del-000000000004"]
    );
    let encoded = fs::read_to_string(workspace.journal()).unwrap();
    assert_eq!(encoded.lines().count(), before + 1);
    assert!(encoded.lines().last().unwrap().starts_with("T|"));
    let mut reopened = SignalHarbor::open(workspace.journal()).unwrap();
    assert_eq!(reopened.summary(), harbor.summary());
    for result in &results {
        assert_eq!(reopened.event(&result.event.id).unwrap(), result.event);
    }
    assert_eq!(
        reopened.ingest(input("third", 30)).unwrap().event.id,
        "evt-000000000003"
    );
}

#[test]
fn reconciles_retained_and_same_batch_duplicates_without_rerouting() {
    let (workspace, mut harbor) = seeded(1);
    let retained = harbor.ingest(input("retained", 1)).unwrap();
    let results = harbor
        .ingest_batch(&[
            input("retained", 2),
            input("new", 3),
            input("new", 4),
            input("retained", 5),
        ])
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|item| item.duplicate)
            .collect::<Vec<_>>(),
        [true, false, true, true]
    );
    assert_eq!(results[0].event, retained.event);
    assert_eq!(results[1].event, results[2].event);
    assert_eq!(results[1].delivery_ids, results[2].delivery_ids);
    let before = fs::read(workspace.journal()).unwrap();
    harbor
        .ingest_batch(&[input("retained", 6), input("new", 7)])
        .unwrap();
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(harbor.summary().events, 2);
    assert_eq!(harbor.summary().deliveries, 2);
}

#[test]
fn conflicting_late_member_rolls_back_the_entire_batch() {
    let (workspace, mut harbor) = seeded(1);
    let before = fs::read(workspace.journal()).unwrap();
    let mut changed = input("first", 2);
    changed.body = b"different".to_vec();
    assert!(matches!(
        harbor.ingest_batch(&[input("first", 1), changed]),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(harbor.summary().events, 0);
    assert_eq!(
        harbor.ingest(input("first", 3)).unwrap().event.id,
        "evt-000000000001"
    );
}

#[test]
fn invalid_public_input_cannot_partially_commit_or_bypass_single_ingestion() {
    let (workspace, mut harbor) = seeded(1);
    let before = fs::read(workspace.journal()).unwrap();
    let mut invalid = input("bad", 2);
    invalid.body.clear();
    assert!(matches!(
        harbor.ingest_batch(&[input("good", 1), invalid.clone()]),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.ingest(invalid),
        Err(HarborError::Validation(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(harbor.summary().events, 0);
}

#[test]
fn normalization_and_tenant_isolation_apply_to_every_batch_member() {
    let (_workspace, mut harbor) = seeded(1);
    let mut raw = input("key", 1);
    raw.tenant = " ACME ".into();
    raw.topic = " ORDERS.CREATED ".into();
    raw.dedupe_key = " key ".into();
    let other = WebhookInput::new("other", "orders.created", "key", b"body", 2).unwrap();
    let results = harbor.ingest_batch(&[raw, input("key", 2), other]).unwrap();
    assert_eq!(results[0].event.tenant, "acme");
    assert!(results[1].duplicate);
    assert_ne!(results[0].event.id, results[2].event.id);
    assert!(results[2].delivery_ids.is_empty());
}

#[test]
fn empty_batch_does_not_create_a_journal() {
    let (workspace, mut harbor) = seeded(0);
    assert!(harbor.ingest_batch(&[]).unwrap().is_empty());
    assert!(!workspace.journal().exists());
}

#[test]
fn interrupted_batch_cannot_replay_only_its_complete_record_prefix() {
    let (workspace, mut harbor) = seeded(0);
    harbor
        .ingest_batch(&[input("one", 1), input("two", 2)])
        .unwrap();
    let frame = fs::read_to_string(workspace.journal()).unwrap();
    let prefix = frame.split(';').next().unwrap();
    assert!(prefix.starts_with("T|E|"));
    fs::write(workspace.journal(), prefix).unwrap();
    assert!(
        matches!(SignalHarbor::open(workspace.journal()), Err(HarborError::Journal(reason)) if reason.contains("incomplete transaction at line 1"))
    );
    assert_eq!(fs::read_to_string(workspace.journal()).unwrap(), prefix);
}

#[test]
fn newline_complete_crlf_batch_remains_replayable() {
    let (workspace, mut harbor) = seeded(1);
    harbor
        .ingest_batch(&[input("one", 1), input("two", 2)])
        .unwrap();
    let frames = fs::read_to_string(workspace.journal()).unwrap();
    fs::write(workspace.journal(), frames.replace('\n', "\r\n")).unwrap();
    let restored = SignalHarbor::open(workspace.journal()).unwrap();
    assert_eq!(restored.summary(), harbor.summary());
}

#[test]
fn frozen_new_ingress_rolls_back_batch_but_retained_duplicates_remain_safe() {
    let (workspace, mut harbor) = seeded(1);
    let original = harbor.ingest(input("retained", 1)).unwrap();
    harbor
        .impose_ingress_freeze("acme", 10, 20, "operator", "receiver outage", 5)
        .unwrap();
    let before = fs::read(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.ingest_batch(&[input("before-freeze", 9), input("blocked", 10)]),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    let duplicate = harbor.ingest_batch(&[input("retained", 15)]).unwrap();
    assert!(duplicate[0].duplicate);
    assert_eq!(duplicate[0].event, original.event);
    assert_eq!(harbor.summary().events, 1);
    assert!(!harbor.ingest_batch(&[input("after-freeze", 20)]).unwrap()[0].duplicate);
}

#[test]
fn validates_input_count_and_aggregate_body_budget_before_writing() {
    let (workspace, mut harbor) = seeded(0);
    assert!(harbor.ingest_batch(&vec![input("key", 1); 1_001]).is_err());
    let large = WebhookInput::new("acme", "orders.created", "big", vec![0; 1_048_576], 1).unwrap();
    assert!(harbor.ingest_batch(&vec![large; 17]).is_err());
    assert!(!workspace.journal().exists());
    let accepted = harbor.ingest_batch(&vec![input("same", 1); 1_000]).unwrap();
    assert_eq!(accepted.len(), 1_000);
    assert_eq!(harbor.summary().events, 1);
}

#[test]
fn excessive_fanout_is_rejected_without_consuming_identifiers() {
    let (workspace, mut harbor) = seeded(11);
    let before = fs::read(workspace.journal()).unwrap();
    let inputs = (0..910)
        .map(|index| input(&format!("key-{index}"), 1))
        .collect::<Vec<_>>();
    assert!(
        matches!(harbor.ingest_batch(&inputs), Err(HarborError::Validation(reason)) if reason.contains("10000"))
    );
    assert_eq!(fs::read(workspace.journal()).unwrap(), before);
    assert_eq!(harbor.summary().events, 0);
    assert_eq!(
        harbor.ingest(input("single", 2)).unwrap().event.id,
        "evt-000000000001"
    );
}

#[test]
fn persistence_failure_exposes_no_partial_batch() {
    let (workspace, mut harbor) = seeded(1);
    let saved = workspace.0.join("saved.journal");
    fs::rename(workspace.journal(), &saved).unwrap();
    fs::create_dir(workspace.journal()).unwrap();
    assert!(matches!(
        harbor.ingest_batch(&[input("one", 1), input("two", 2)]),
        Err(HarborError::Journal(_))
    ));
    assert_eq!(harbor.summary().events, 0);
    fs::remove_dir(workspace.journal()).unwrap();
    fs::rename(saved, workspace.journal()).unwrap();
    assert_eq!(
        harbor.ingest(input("one", 3)).unwrap().event.id,
        "evt-000000000001"
    );
}
