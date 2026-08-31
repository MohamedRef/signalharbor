use signalharbor::{ClockSkewDecision, ClockSkewInput, HarborError, assess_clock_skew};

fn input(signed_at_epoch: u64) -> ClockSkewInput {
    ClockSkewInput {
        event_id: "evt-1".into(),
        signed_at_epoch,
        received_at_epoch: 1_000,
        maximum_past_skew_seconds: 100,
        maximum_future_skew_seconds: 20,
    }
}

#[test]
fn distinguishes_stale_and_future_dated_events() {
    assert_eq!(
        assess_clock_skew(&input(899)).unwrap().decision,
        ClockSkewDecision::Stale
    );
    assert_eq!(
        assess_clock_skew(&input(1_021)).unwrap().decision,
        ClockSkewDecision::FutureDated
    );
    let boundary = assess_clock_skew(&input(900)).unwrap();
    assert_eq!(boundary.decision, ClockSkewDecision::Accept);
    assert_eq!(boundary.absolute_skew_seconds, 100);
}

#[test]
fn rejects_missing_skew_tolerances() {
    let mut row = input(1_000);
    row.maximum_past_skew_seconds = 0;
    assert!(matches!(
        assess_clock_skew(&row),
        Err(HarborError::Validation(_))
    ));
}
