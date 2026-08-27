use signalharbor::{DrainInput, DrainReadiness, HarborError, assess_endpoint_drain};

fn input(queued: u64, in_flight: u64, rate: u64, deadline: u64) -> DrainInput {
    DrainInput {
        endpoint: "https://hooks.example.test/orders".into(),
        queued,
        in_flight,
        completions_per_minute: rate,
        deadline_minutes: deadline,
    }
}

#[test]
fn ready_and_on_track_drains_are_distinguished() {
    let ready = assess_endpoint_drain(&input(0, 0, 0, 0)).unwrap();
    assert_eq!(ready.readiness, DrainReadiness::Ready);
    assert_eq!(ready.estimated_minutes, Some(0));

    let draining = assess_endpoint_drain(&input(10, 3, 5, 3)).unwrap();
    assert_eq!(draining.outstanding, 13);
    assert_eq!(draining.estimated_minutes, Some(3));
    assert_eq!(draining.readiness, DrainReadiness::OnTrack);
}

#[test]
fn exposes_at_risk_and_stalled_removals() {
    let late = assess_endpoint_drain(&input(10, 1, 2, 5)).unwrap();
    assert_eq!(late.estimated_minutes, Some(6));
    assert_eq!(late.readiness, DrainReadiness::AtRisk);

    let stalled = assess_endpoint_drain(&input(1, 0, 0, 10)).unwrap();
    assert_eq!(stalled.estimated_minutes, None);
    assert_eq!(stalled.readiness, DrainReadiness::Stalled);
}

#[test]
fn rejects_unactionable_drain_inputs() {
    let mut blank = input(1, 0, 1, 1);
    blank.endpoint = " ".into();
    assert!(matches!(
        assess_endpoint_drain(&blank),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        assess_endpoint_drain(&input(1, 0, 1, 0)),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        assess_endpoint_drain(&input(u64::MAX, 1, 1, 1)),
        Err(HarborError::Validation(_))
    ));
}
