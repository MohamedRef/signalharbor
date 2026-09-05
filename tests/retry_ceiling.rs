use signalharbor::{HarborError, RetryCeilingPolicy, build_retry_ceiling};

#[test]
fn caps_exponential_backoff_without_overflow() {
    let rows = build_retry_ceiling(&RetryCeilingPolicy {
        route_id: "r".into(),
        base_delay_seconds: 10,
        multiplier: 3,
        maximum_delay_seconds: 50,
        maximum_attempts: 4,
    })
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.delay_seconds).collect::<Vec<_>>(),
        vec![10, 30, 50, 50]
    );
    assert!(rows[3].capped);
}

#[test]
fn rejects_non_growing_multiplier() {
    assert!(
        build_retry_ceiling(&RetryCeilingPolicy {
            route_id: "r".into(),
            base_delay_seconds: 1,
            multiplier: 1,
            maximum_delay_seconds: 2,
            maximum_attempts: 1
        })
        .is_err()
    );
}

#[test]
fn bounds_retry_schedules_before_allocating_steps() {
    let result = build_retry_ceiling(&RetryCeilingPolicy {
        route_id: "route".into(),
        base_delay_seconds: 1,
        multiplier: 2,
        maximum_delay_seconds: 60,
        maximum_attempts: 10_001,
    });
    assert!(matches!(
        result,
        Err(HarborError::Validation(message)) if message.contains("10000")
    ));
}
