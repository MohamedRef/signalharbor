use signalharbor::{HarborError, PayloadRetention, PayloadState};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn journal() -> PathBuf {
    std::env::temp_dir().join(format!(
        "signalharbor-retention-{}-{}.log",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn terminal_payload_is_purged_to_replayable_tombstone() {
    let path = journal();
    let mut retention = PayloadRetention::open(&path).unwrap();
    let captured = retention
        .capture("event-1", "tenant-a", br#"{"order":42}"#, 100, 30)
        .unwrap();
    assert_eq!(captured.state, PayloadState::Available);
    assert_eq!(captured.payload_len, 12);
    assert!(captured.fingerprint.starts_with("fnv1a64:"));
    let terminal = retention.mark_terminal("event-1", 110, 1).unwrap();
    assert!(!terminal.purge_eligible(129));
    assert!(terminal.purge_eligible(130));

    let result = retention.sweep(130).unwrap();
    assert_eq!(result.purged_event_ids, ["event-1"]);
    assert_eq!(result.bytes_released, 12);
    let tombstone = retention.record("event-1").unwrap();
    assert_eq!(tombstone.state, PayloadState::Purged);
    assert_eq!(tombstone.payload, None);
    assert_eq!(tombstone.payload_len, 12);
    assert_eq!(tombstone.purged_at, Some(130));

    drop(retention);
    let replayed = PayloadRetention::open(path).unwrap();
    assert_eq!(replayed.record("event-1").unwrap(), tombstone);
}

#[test]
fn nonterminal_payload_never_purges_even_after_deadline() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    retention
        .capture("event-1", "tenant", b"payload", 10, 5)
        .unwrap();
    assert!(retention.due(100, None).is_empty());
    assert!(retention.sweep(100).unwrap().purged_event_ids.is_empty());
    assert_eq!(
        retention.record("event-1").unwrap().state,
        PayloadState::Available
    );
}

#[test]
fn hold_blocks_purge_until_explicit_release() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    let record = retention
        .capture("event-1", "tenant", b"payload", 10, 5)
        .unwrap();
    let record = retention
        .place_hold("event-1", "payment dispute investigation", record.version)
        .unwrap();
    let record = retention
        .mark_terminal("event-1", 12, record.version)
        .unwrap();
    assert!(retention.due(20, None).is_empty());
    assert!(retention.sweep(20).unwrap().purged_event_ids.is_empty());
    let record = retention.release_hold("event-1", record.version).unwrap();
    assert!(record.purge_eligible(20));
    assert_eq!(retention.sweep(20).unwrap().purged_event_ids, ["event-1"]);
}

#[test]
fn due_queue_is_ordered_and_tenant_filterable() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    for (event, tenant, received, duration) in [
        ("event-b", "tenant-a", 10, 10),
        ("event-a", "tenant-a", 10, 10),
        ("event-c", "tenant-b", 10, 5),
    ] {
        let row = retention
            .capture(event, tenant, event.as_bytes(), received, duration)
            .unwrap();
        retention.mark_terminal(event, 11, row.version).unwrap();
    }
    assert_eq!(
        retention
            .due(20, None)
            .into_iter()
            .map(|row| row.event_id)
            .collect::<Vec<_>>(),
        ["event-a", "event-b", "event-c"]
    );
    assert_eq!(
        retention
            .due(20, Some("tenant-b"))
            .into_iter()
            .map(|row| row.event_id)
            .collect::<Vec<_>>(),
        ["event-c"]
    );
}

#[test]
fn sweep_only_purges_records_eligible_at_observed_time() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    let early = retention.capture("early", "tenant", b"123", 10, 5).unwrap();
    retention.mark_terminal("early", 11, early.version).unwrap();
    let later = retention
        .capture("later", "tenant", b"12345", 10, 50)
        .unwrap();
    retention.mark_terminal("later", 11, later.version).unwrap();
    let result = retention.sweep(20).unwrap();
    assert_eq!(result.purged_event_ids, ["early"]);
    assert_eq!(result.bytes_released, 3);
    assert_eq!(
        retention.record("later").unwrap().payload.as_deref(),
        Some(b"12345".as_slice())
    );
}

#[test]
fn capture_validates_identity_payload_policy_and_overflow() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    assert!(matches!(
        retention.capture("", "tenant", b"payload", 0, 1),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        retention.capture("event", "tenant", b"", 0, 1),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        retention.capture("event", "tenant", b"payload", 0, 0),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        retention.capture("event", "tenant", b"payload", u64::MAX, 1),
        Err(HarborError::Validation(_))
    ));
    retention
        .capture("event", "tenant", b"payload", 0, 1)
        .unwrap();
    assert!(matches!(
        retention.capture("event", "tenant", b"other", 0, 1),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn lifecycle_operations_enforce_versions_and_states() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    let row = retention
        .capture("event", "tenant", b"payload", 10, 5)
        .unwrap();
    assert!(matches!(
        retention.mark_terminal("event", 11, 9),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        retention.mark_terminal("event", 9, row.version),
        Err(HarborError::Validation(_))
    ));
    let row = retention.mark_terminal("event", 11, row.version).unwrap();
    assert!(matches!(
        retention.mark_terminal("event", 12, row.version),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        retention.release_hold("event", row.version),
        Err(HarborError::Conflict(_))
    ));
    retention.sweep(15).unwrap();
    let tombstone = retention.record("event").unwrap();
    assert!(matches!(
        retention.place_hold("event", "late hold", tombstone.version),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn missing_records_have_typed_errors() {
    let mut retention = PayloadRetention::open(journal()).unwrap();
    assert!(matches!(
        retention.record("missing"),
        Err(HarborError::NotFound(id)) if id == "missing"
    ));
    assert!(matches!(
        retention.place_hold("missing", "reason", 1),
        Err(HarborError::NotFound(id)) if id == "missing"
    ));
}

#[test]
fn replay_rejects_corrupt_hex_and_noncontiguous_versions() {
    let path = journal();
    fs::write(
        &path,
        "zz\t00\tavailable\t00\t1\tfnv1a64:0\t0\t1\t\t\t\t1\n",
    )
    .unwrap();
    assert!(matches!(
        PayloadRetention::open(&path),
        Err(HarborError::Journal(message)) if message.contains("invalid hex")
    ));

    let path = journal();
    let mut retention = PayloadRetention::open(&path).unwrap();
    retention
        .capture("event", "tenant", b"payload", 10, 5)
        .unwrap();
    drop(retention);
    let original = fs::read_to_string(&path).unwrap();
    let skipped = original.trim_end().rsplit_once('\t').unwrap().0.to_owned() + "\t3\n";
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(skipped.as_bytes())
        .unwrap();
    assert!(matches!(
        PayloadRetention::open(path),
        Err(HarborError::Journal(message)) if message.contains("non-contiguous")
    ));
}

#[test]
fn replay_rejects_payload_integrity_mismatch() {
    let path = journal();
    let mut retention = PayloadRetention::open(&path).unwrap();
    retention
        .capture("event", "tenant", b"payload", 10, 5)
        .unwrap();
    drop(retention);
    let mut line = fs::read_to_string(&path).unwrap();
    line = line.replace("7061796c6f6164", "7061796c6f6165");
    fs::write(&path, line).unwrap();
    assert!(matches!(
        PayloadRetention::open(path),
        Err(HarborError::Journal(message)) if message.contains("integrity mismatch")
    ));
}

use std::io::Write;
