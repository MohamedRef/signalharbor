use signalharbor::receiver_clock_verification::{
    ReceiverClockVerificationDisposition, ReceiverClockVerificationObservation,
    assess_receiver_clock_verification,
};

fn observation(id: &str) -> ReceiverClockVerificationObservation {
    ReceiverClockVerificationObservation {
        id: id.to_owned(),
        owner: "operations".to_owned(),
        due_minute: 80,
        completed_minute: None,
        required_signals: vec!["approval".to_owned()],
        observed_signals: vec!["approval".to_owned()],
        exposure_units: 10,
    }
}

#[test]
fn orders_blocked_evidence_before_ready_observations() {
    let mut ready = observation("ready-1");
    ready.due_minute = 100;
    ready.completed_minute = Some(90);
    ready.exposure_units = 20;
    let mut blocked = observation("blocked-1");
    blocked.required_signals = vec!["approval".to_owned(), "receipt".to_owned()];
    blocked.observed_signals = vec!["approval".to_owned()];
    blocked.exposure_units = 90;

    let findings =
        assess_receiver_clock_verification(&[ready, blocked], 100).expect("valid assessment");
    assert_eq!(
        findings
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["blocked-1", "ready-1"]
    );
    assert_eq!(
        findings[0].disposition,
        ReceiverClockVerificationDisposition::Blocked
    );
    assert_eq!(findings[0].missing_signals, ["receipt"]);
    assert_eq!(findings[0].overdue_minutes, 20);
}

#[test]
fn distinguishes_future_gaps_from_late_completion() {
    let mut future = observation("future-gap");
    future.due_minute = 120;
    future.required_signals = vec!["handoff".to_owned()];
    future.observed_signals.clear();
    let mut late = observation("late-complete");
    late.due_minute = 60;
    late.completed_minute = Some(90);
    late.required_signals = vec!["handoff".to_owned()];
    late.observed_signals = vec!["handoff".to_owned()];
    late.exposure_units = 20;

    let findings =
        assess_receiver_clock_verification(&[future, late], 100).expect("valid assessment");
    assert_eq!(findings[0].id, "late-complete");
    assert_eq!(findings[0].overdue_minutes, 30);
    assert_eq!(
        findings[1].disposition,
        ReceiverClockVerificationDisposition::Attention
    );
    assert_eq!(findings[1].overdue_minutes, 0);
}

#[test]
fn rejects_ambiguous_identity_and_evidence() {
    let first = observation("same");
    let second = observation(" SAME ");
    assert!(
        assess_receiver_clock_verification(&[first, second], 100)
            .unwrap_err()
            .contains("duplicate")
    );

    let mut duplicate_signal = observation("signals");
    duplicate_signal.observed_signals = vec!["receipt".to_owned(), " RECEIPT ".to_owned()];
    assert!(
        assess_receiver_clock_verification(&[duplicate_signal], 100)
            .unwrap_err()
            .contains("duplicate")
    );

    let mut future_completion = observation("future");
    future_completion.completed_minute = Some(101);
    assert!(
        assess_receiver_clock_verification(&[future_completion], 100)
            .unwrap_err()
            .contains("completion")
    );
}
