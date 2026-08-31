use signalharbor::{
    HarborError, QuarantinedEvent, TenantQuarantineLimit, assess_quarantine_capacity,
};

#[test]
fn reports_count_and_byte_quarantine_pressure() {
    let events = vec![
        QuarantinedEvent {
            event_id: "a-1".into(),
            tenant: "a".into(),
            payload_bytes: 80,
        },
        QuarantinedEvent {
            event_id: "a-2".into(),
            tenant: "a".into(),
            payload_bytes: 80,
        },
        QuarantinedEvent {
            event_id: "b-1".into(),
            tenant: "b".into(),
            payload_bytes: 30,
        },
    ];
    let limits = vec![
        TenantQuarantineLimit {
            tenant: "a".into(),
            maximum_events: 1,
            maximum_bytes: 100,
        },
        TenantQuarantineLimit {
            tenant: "b".into(),
            maximum_events: 2,
            maximum_bytes: 100,
        },
    ];
    let got = assess_quarantine_capacity(&events, &limits).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].tenant, "a");
    assert_eq!(got[0].excess_events, 1);
    assert_eq!(got[0].excess_bytes, 60);
}

#[test]
fn rejects_events_without_tenant_limits() {
    let event = QuarantinedEvent {
        event_id: "e".into(),
        tenant: "missing".into(),
        payload_bytes: 1,
    };
    assert!(matches!(
        assess_quarantine_capacity(&[event], &[]),
        Err(HarborError::Validation(_))
    ));
}
