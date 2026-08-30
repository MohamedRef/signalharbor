use signalharbor::{HarborError, IngressGate, IngressQuota, IngressRejectReason};

fn gate() -> IngressGate {
    IngressGate::new(IngressQuota::new(60, 2, 1_000, 700).unwrap(), 300).unwrap()
}

#[test]
fn admits_within_event_and_byte_budgets() {
    let mut gate = gate();
    let first = gate.admit("Tenant-A", "request-1", 400, 125).unwrap();
    assert!(first.accepted);
    assert_eq!(first.window_started_at, 120);
    assert_eq!(first.window_ends_at, 180);
    assert_eq!(first.remaining_events, 1);
    assert_eq!(first.remaining_bytes, 600);
    let second = gate.admit("tenant-a", "request-2", 500, 126).unwrap();
    assert!(second.accepted);
    assert_eq!(second.remaining_events, 0);
    assert_eq!(second.remaining_bytes, 100);
}

#[test]
fn returns_stable_duplicate_decisions_without_double_charging() {
    let mut gate = gate();
    let original = gate.admit("tenant", "same-request", 300, 1).unwrap();
    let duplicate = gate.admit("tenant", "same-request", 300, 59).unwrap();
    assert!(original.accepted && duplicate.accepted && duplicate.duplicate);
    let usage = gate.usage("tenant", 59).unwrap();
    assert_eq!(usage.accepted_events, 1);
    assert_eq!(usage.accepted_bytes, 300);
    assert!(matches!(
        gate.admit("tenant", "same-request", 301, 59),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn distinguishes_size_event_and_byte_rejections() {
    let mut byte_gate = gate();
    let oversized = byte_gate.admit("tenant", "large", 701, 1).unwrap();
    assert_eq!(oversized.reason, Some(IngressRejectReason::EventTooLarge));
    byte_gate.admit("tenant", "one", 600, 2).unwrap();
    let bytes = byte_gate.admit("tenant", "two", 500, 3).unwrap();
    assert_eq!(bytes.reason, Some(IngressRejectReason::ByteQuotaExceeded));

    let mut event_gate = gate();
    event_gate.admit("tenant", "one", 1, 2).unwrap();
    event_gate.admit("tenant", "two", 1, 3).unwrap();
    let events = event_gate.admit("tenant", "three", 1, 4).unwrap();
    assert_eq!(events.reason, Some(IngressRejectReason::EventQuotaExceeded));
    assert_eq!(event_gate.usage("tenant", 4).unwrap().rejected_events, 1);
}

#[test]
fn resets_usage_on_aligned_window_boundaries() {
    let mut gate = gate();
    gate.admit("tenant", "one", 600, 119).unwrap();
    let reset = gate.admit("tenant", "two", 600, 120).unwrap();
    assert!(reset.accepted);
    let usage = gate.usage("tenant", 120).unwrap();
    assert_eq!(usage.accepted_events, 1);
    assert_eq!(usage.window_started_at, 120);
}

#[test]
fn tenant_override_changes_capacity_and_can_be_removed() {
    let mut gate = gate();
    gate.set_tenant_quota("priority", IngressQuota::new(60, 4, 2_000, 1_000).unwrap())
        .unwrap();
    let decision = gate.admit("priority", "large", 900, 1).unwrap();
    assert!(decision.accepted);
    assert_eq!(decision.remaining_events, 3);
    gate.remove_tenant_quota("priority").unwrap();
    assert!(matches!(
        gate.remove_tenant_quota("priority"),
        Err(HarborError::NotFound(_))
    ));
}

#[test]
fn snapshot_round_trip_preserves_usage_overrides_and_receipts() {
    let mut gate = gate();
    gate.set_tenant_quota("priority", IngressQuota::new(30, 4, 2_000, 1_000).unwrap())
        .unwrap();
    let original = gate.admit("priority", "request", 800, 61).unwrap();
    let snapshot = gate.snapshot();
    let mut restored = IngressGate::restore(&snapshot).unwrap();
    assert_eq!(restored.usage("priority", 61).unwrap().accepted_bytes, 800);
    let duplicate = restored.admit("priority", "request", 800, 62).unwrap();
    assert_eq!(duplicate.remaining_bytes, original.remaining_bytes);
    assert!(duplicate.duplicate);
    assert_eq!(restored.snapshot(), snapshot);
}

#[test]
fn expired_receipts_stop_reserving_request_identifiers() {
    let mut gate = gate();
    gate.admit("tenant", "request", 100, 1).unwrap();
    let fresh = gate.admit("tenant", "request", 200, 301).unwrap();
    assert!(fresh.accepted);
    assert!(!fresh.duplicate);
}

#[test]
fn validates_quotas_commands_and_snapshots() {
    assert!(IngressQuota::new(0, 1, 1, 1).is_err());
    assert!(IngressQuota::new(60, 0, 1, 1).is_err());
    assert!(IngressQuota::new(60, 1, 10, 11).is_err());
    assert!(IngressGate::new(IngressQuota::new(60, 1, 1, 1).unwrap(), 0).is_err());
    let mut gate = gate();
    assert!(gate.admit("", "request", 1, 1).is_err());
    assert!(gate.admit("tenant", "request", 0, 1).is_err());
    assert!(IngressGate::restore("broken\n").is_err());
    assert!(IngressGate::restore("INGRESS|1|60|2|1000|700|300\nU|74656e616e74|0|3|1|0\n").is_err());
    assert!(
        IngressGate::restore("INGRESS|1|60|2|1000|700|300\nR|zz|aa|1|1|1|-|0|60|1|1|0\n").is_err()
    );
}

#[test]
fn rejects_ingress_windows_that_exceed_the_timestamp_range() {
    let mut gate = gate();
    assert!(matches!(
        gate.admit("tenant", "request", 1, u64::MAX),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
    assert!(matches!(
        gate.usage("tenant", u64::MAX),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
    assert!(!gate.snapshot().contains("request"));
}
