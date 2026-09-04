use signalharbor::{
    HarborError, ReceiverCapacityInput, ReceiverCapacityState, assess_receiver_capacity,
};

fn input(
    route: &str,
    active: u32,
    pending: u64,
    millis: u64,
    objective: u64,
) -> ReceiverCapacityInput {
    ReceiverCapacityInput {
        route_id: route.into(),
        endpoint: format!("https://{route}.example.test/hook"),
        max_concurrency: 4,
        active_leases: active,
        pending_due: pending,
        average_attempt_millis: millis,
        drain_objective_seconds: objective,
    }
}

#[test]
fn ranks_receiver_pressure_and_rounds_drain_waves_up() {
    let rows = assess_receiver_capacity(&[
        input("idle", 0, 0, 500, 10),
        input("healthy", 1, 5, 1_200, 10),
        input("constrained", 2, 17, 2_500, 10),
        input("saturated", 4, 1, 100, 10),
    ])
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.route_id.as_str())
            .collect::<Vec<_>>(),
        ["saturated", "constrained", "healthy", "idle"]
    );
    assert_eq!(rows[0].state, ReceiverCapacityState::Saturated);
    assert_eq!(rows[1].state, ReceiverCapacityState::Constrained);
    assert_eq!(rows[1].estimated_drain_seconds, 13);
    assert_eq!(rows[1].objective_overrun_seconds, 3);
    assert_eq!(rows[2].available_slots, 3);
}

#[test]
fn validates_capacity_identity_and_arithmetic_without_partial_results() {
    let mut invalid = input("route", 0, 1, 100, 10);
    for mutate in 0..6 {
        invalid.route_id = "route".into();
        invalid.endpoint = "endpoint".into();
        invalid.max_concurrency = 4;
        invalid.active_leases = 0;
        invalid.average_attempt_millis = 100;
        invalid.drain_objective_seconds = 10;
        match mutate {
            0 => invalid.route_id = " ".into(),
            1 => invalid.endpoint = " ".into(),
            2 => invalid.max_concurrency = 0,
            3 => invalid.active_leases = 5,
            4 => invalid.average_attempt_millis = 0,
            _ => invalid.drain_objective_seconds = 0,
        }
        assert!(matches!(
            assess_receiver_capacity(&[invalid.clone()]),
            Err(HarborError::Validation(_))
        ));
    }
    assert!(matches!(
        assess_receiver_capacity(&[
            input("Route", 0, 1, 100, 10),
            input(" route ", 0, 1, 100, 10)
        ]),
        Err(HarborError::Validation(_))
    ));
}

#[test]
fn rejects_drain_estimate_overflow() {
    let mut row = input("overflow", 0, u64::MAX, u64::MAX, 1);
    row.max_concurrency = 1;
    assert!(matches!(
        assess_receiver_capacity(&[row]),
        Err(HarborError::Validation(_))
    ));
}
