use signalharbor::{HarborError, ReplayWindowReadiness, ReplayWindowRequest, assess_replay_window};

fn request(from: u64) -> ReplayWindowRequest {
    ReplayWindowRequest {
        route: "orders".into(),
        requested_from_epoch: from,
        requested_to_epoch: 500,
        retained_from_epoch: 200,
        idempotency_from_epoch: 300,
    }
}

#[test]
fn distinguishes_payload_and_deduplication_replay_limits() {
    let missing = assess_replay_window(&request(100)).unwrap();
    assert_eq!(missing.readiness, ReplayWindowReadiness::PayloadUnavailable);
    assert_eq!(missing.effective_from_epoch, 300);
    let unsafe_replay = assess_replay_window(&request(250)).unwrap();
    assert_eq!(
        unsafe_replay.readiness,
        ReplayWindowReadiness::DeduplicationExpired
    );
    let ready = assess_replay_window(&request(350)).unwrap();
    assert_eq!(ready.readiness, ReplayWindowReadiness::Ready);
}

#[test]
fn rejects_empty_routes_and_reversed_windows() {
    let mut invalid = request(400);
    invalid.route = " ".into();
    assert!(matches!(
        assess_replay_window(&invalid),
        Err(HarborError::Validation(_))
    ));
    invalid.route = "orders".into();
    invalid.requested_from_epoch = 500;
    assert!(matches!(
        assess_replay_window(&invalid),
        Err(HarborError::Validation(_))
    ));
}
