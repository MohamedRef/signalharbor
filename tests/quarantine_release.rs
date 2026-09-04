use signalharbor::{
    HarborError, QuarantineReleaseRequest, ReleaseCapacity, allocate_quarantine_releases,
};

#[test]
fn allocates_approved_releases_by_priority_and_capacity() {
    let rows = allocate_quarantine_releases(
        &[ReleaseCapacity {
            route_id: "orders".into(),
            events_per_minute: 10,
            maximum_batch: 80,
        }],
        &[
            QuarantineReleaseRequest {
                request_id: "later".into(),
                route_id: "orders".into(),
                event_count: 80,
                priority: 2,
                evidence_approved: true,
            },
            QuarantineReleaseRequest {
                request_id: "first".into(),
                route_id: "ORDERS".into(),
                event_count: 70,
                priority: 1,
                evidence_approved: true,
            },
        ],
        10,
    )
    .unwrap();
    assert_eq!((rows[0].released_events, rows[1].released_events), (70, 30));
    assert_eq!(rows[0].minutes_required, 7);
}

#[test]
fn blocks_unapproved_release_and_rejects_missing_capacity() {
    let capacity = [ReleaseCapacity {
        route_id: "orders".into(),
        events_per_minute: 10,
        maximum_batch: 80,
    }];
    let request = QuarantineReleaseRequest {
        request_id: "request".into(),
        route_id: "orders".into(),
        event_count: 20,
        priority: 1,
        evidence_approved: false,
    };
    let rows = allocate_quarantine_releases(&capacity, std::slice::from_ref(&request), 1).unwrap();
    assert_eq!(
        rows[0].blocked_reason.as_deref(),
        Some("release_evidence_not_approved")
    );
    assert!(matches!(
        allocate_quarantine_releases(&[], &[request], 1),
        Err(HarborError::Validation(_))
    ));
}
