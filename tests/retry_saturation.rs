use signalharbor::{RetryQueueSample, detect_retry_saturation};

#[test]
fn detects_routes_that_cannot_drain_on_time() {
    let result = detect_retry_saturation(&[
        RetryQueueSample {
            route_id: "slow".into(),
            queued: 1_000,
            workers: 2,
            attempts_per_worker_per_minute: 10,
            maximum_drain_minutes: 20,
        },
        RetryQueueSample {
            route_id: "healthy".into(),
            queued: 100,
            workers: 5,
            attempts_per_worker_per_minute: 10,
            maximum_drain_minutes: 5,
        },
    ])
    .expect("assessment");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].route_id, "slow");
    assert_eq!(result[0].drain_minutes, 50);
    assert_eq!(result[0].excess_minutes, 30);
}

#[test]
fn rejects_zero_worker_capacity() {
    assert!(
        detect_retry_saturation(&[RetryQueueSample {
            route_id: "route".into(),
            queued: 1,
            workers: 0,
            attempts_per_worker_per_minute: 10,
            maximum_drain_minutes: 1,
        }])
        .is_err()
    );
}
