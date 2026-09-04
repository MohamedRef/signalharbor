use signalharbor::{FanoutPolicy, RouteFanoutSample, assess_route_fanout};

#[test]
fn rejects_route_and_concurrency_excesses() {
    let rows = assess_route_fanout(
        &[FanoutPolicy {
            tenant_id: "tenant".into(),
            maximum_routes_per_event: 3,
            maximum_concurrent_deliveries: 5,
        }],
        &[RouteFanoutSample {
            event_id: "event".into(),
            tenant_id: "tenant".into(),
            matched_routes: 4,
            active_deliveries: 3,
        }],
    )
    .unwrap();
    assert_eq!(rows[0].excess_routes, 1);
    assert_eq!(rows[0].excess_concurrency, 2);
    assert!(!rows[0].admitted);
}

#[test]
fn accepts_bounded_fanout_and_rejects_unknown_tenants() {
    let policy = FanoutPolicy {
        tenant_id: "tenant".into(),
        maximum_routes_per_event: 3,
        maximum_concurrent_deliveries: 5,
    };
    let sample = RouteFanoutSample {
        event_id: "event".into(),
        tenant_id: "tenant".into(),
        matched_routes: 2,
        active_deliveries: 2,
    };
    assert!(
        assess_route_fanout(std::slice::from_ref(&policy), std::slice::from_ref(&sample)).unwrap()
            [0]
        .admitted
    );
    assert!(
        assess_route_fanout(
            &[policy],
            &[RouteFanoutSample {
                tenant_id: "missing".into(),
                ..sample
            }]
        )
        .is_err()
    );
}
