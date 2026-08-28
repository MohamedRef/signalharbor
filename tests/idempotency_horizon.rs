use signalharbor::{HarborError, HorizonReadiness, IdempotencyHorizon, assess_idempotency_horizon};

#[test]
fn confirms_retention_covers_delivery_and_redrive_paths() {
    let result = assess_idempotency_horizon(&IdempotencyHorizon {
        retention_seconds: 100_000,
        maximum_delivery_lifetime_seconds: 86_400,
        maximum_redrive_delay_seconds: 3_600,
        clock_skew_allowance_seconds: 60,
    })
    .unwrap();
    assert_eq!(result.required_seconds, 90_060);
    assert_eq!(result.shortfall_seconds, 0);
    assert_eq!(result.readiness, HorizonReadiness::Sufficient);
}

#[test]
fn reports_the_exact_idempotency_retention_shortfall() {
    let result = assess_idempotency_horizon(&IdempotencyHorizon {
        retention_seconds: 80_000,
        maximum_delivery_lifetime_seconds: 86_400,
        maximum_redrive_delay_seconds: 3_600,
        clock_skew_allowance_seconds: 60,
    })
    .unwrap();
    assert_eq!(result.shortfall_seconds, 10_060);
    assert_eq!(result.readiness, HorizonReadiness::Insufficient);
}

#[test]
fn rejects_zero_and_overflowing_horizon_controls() {
    let invalid = IdempotencyHorizon {
        retention_seconds: 0,
        maximum_delivery_lifetime_seconds: 1,
        maximum_redrive_delay_seconds: 0,
        clock_skew_allowance_seconds: 0,
    };
    assert!(matches!(
        assess_idempotency_horizon(&invalid),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        assess_idempotency_horizon(&IdempotencyHorizon {
            retention_seconds: 1,
            maximum_delivery_lifetime_seconds: u64::MAX,
            maximum_redrive_delay_seconds: 1,
            clock_skew_allowance_seconds: 0,
        }),
        Err(HarborError::Validation(_))
    ));
}
