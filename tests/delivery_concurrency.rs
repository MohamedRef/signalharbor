use signalharbor::{RouteDeliveryDemand, WorkerPoolCapacity, plan_delivery_concurrency};

#[test]
fn caps_route_parallelism_and_defers_excess() {
    let rows = plan_delivery_concurrency(
        &[RouteDeliveryDemand {
            route_id: "route".into(),
            queued_deliveries: 10,
            maximum_parallel: 3,
        }],
        &[WorkerPoolCapacity {
            pool_id: "pool".into(),
            route_ids: vec!["route".into()],
            available_workers: 8,
        }],
    )
    .unwrap();
    assert_eq!(rows[0].parallel_deliveries, 3);
    assert_eq!(rows[0].deferred_deliveries, 7);
}

#[test]
fn rejects_zero_parallel_ceiling() {
    assert!(
        plan_delivery_concurrency(
            &[RouteDeliveryDemand {
                route_id: "r".into(),
                queued_deliveries: 1,
                maximum_parallel: 0
            }],
            &[]
        )
        .is_err()
    );
}
