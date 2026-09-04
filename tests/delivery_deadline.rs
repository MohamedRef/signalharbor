use signalharbor::{
    DeliveryDeadlineInput, DeliveryUrgency, HarborError, assess_delivery_deadlines,
};

fn input(id: &str, next: u64, expires: u64, remaining: u16) -> DeliveryDeadlineInput {
    DeliveryDeadlineInput {
        tenant: "tenant".into(),
        delivery_id: id.into(),
        next_attempt_at: next,
        expires_at: expires,
        attempts_remaining: remaining,
    }
}

#[test]
fn prioritizes_expired_due_and_constrained_deliveries_stably() {
    let rows = assess_delivery_deadlines(
        &[
            input("safe", 140, 400, 3),
            input("risk", 130, 160, 2),
            input("due", 100, 180, 2),
            input("expired", 90, 99, 1),
            input("budget", 120, 400, 0),
        ],
        100,
        60,
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.delivery_id.as_str())
            .collect::<Vec<_>>(),
        ["expired", "due", "risk", "budget", "safe"]
    );
    assert_eq!(rows[0].urgency, DeliveryUrgency::Expired);
    assert_eq!(rows[1].urgency, DeliveryUrgency::Due);
    assert_eq!(rows[2].urgency, DeliveryUrgency::AtRisk);
    assert_eq!(rows[4].urgency, DeliveryUrgency::OnTrack);
    assert_eq!(rows[2].seconds_until_attempt, 30);
    assert_eq!(rows[2].retry_window_seconds, 30);
}

#[test]
fn rejects_invalid_or_ambiguous_deadline_inputs() {
    assert!(matches!(
        assess_delivery_deadlines(&[input("one", 10, 20, 1)], 0, 0),
        Err(HarborError::Validation(_))
    ));
    for rows in [
        vec![input(" ", 10, 20, 1)],
        vec![input("one", 20, 19, 1)],
        vec![input("one", 10, 20, 1), input("one", 11, 21, 1)],
    ] {
        assert!(matches!(
            assess_delivery_deadlines(&rows, 0, 5),
            Err(HarborError::Validation(_))
        ));
    }
}

#[test]
fn duplicate_detection_is_tenant_case_insensitive_but_ids_are_exact() {
    let mut first = input("delivery", 10, 20, 1);
    first.tenant = "Tenant".into();
    let mut second = input("delivery", 10, 20, 1);
    second.tenant = " tenant ".into();
    assert!(matches!(
        assess_delivery_deadlines(&[first, second], 0, 5),
        Err(HarborError::Validation(_))
    ));
}
