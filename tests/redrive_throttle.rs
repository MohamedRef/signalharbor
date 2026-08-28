use signalharbor::{HarborError, RedriveCapacity, RedriveReadiness, calculate_redrive_throttle};

fn capacity(live: u64, requested: u64) -> RedriveCapacity {
    RedriveCapacity {
        endpoint: "https://hooks.example.test/orders".into(),
        capacity_per_minute: 1_000,
        live_traffic_per_minute: live,
        requested_redrive_per_minute: requested,
        reserve_percent: 10,
    }
}

#[test]
fn preserves_endpoint_reserve_while_approving_redrive_traffic() {
    let open = calculate_redrive_throttle(&capacity(500, 300)).unwrap();
    assert_eq!(open.reserved_per_minute, 100);
    assert_eq!(open.available_per_minute, 400);
    assert_eq!(open.approved_redrive_per_minute, 300);
    assert_eq!(open.readiness, RedriveReadiness::Available);

    let constrained = calculate_redrive_throttle(&capacity(750, 300)).unwrap();
    assert_eq!(constrained.approved_redrive_per_minute, 150);
    assert_eq!(constrained.readiness, RedriveReadiness::Constrained);
}

#[test]
fn saturates_redrive_when_live_traffic_consumes_safe_capacity() {
    let result = calculate_redrive_throttle(&capacity(950, 100)).unwrap();
    assert_eq!(result.approved_redrive_per_minute, 0);
    assert_eq!(result.readiness, RedriveReadiness::Saturated);
}

#[test]
fn rejects_invalid_redrive_capacity_controls() {
    let mut invalid = capacity(1, 1);
    invalid.capacity_per_minute = 0;
    assert!(matches!(
        calculate_redrive_throttle(&invalid),
        Err(HarborError::Validation(_))
    ));
    invalid.capacity_per_minute = u64::MAX;
    invalid.reserve_percent = 100;
    assert!(matches!(
        calculate_redrive_throttle(&invalid),
        Err(HarborError::Validation(_))
    ));
}
