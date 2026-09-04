use signalharbor::{DrainCapacity, DrainDemand, plan_route_drain_capacity};

#[test]
fn assigns_fast_workers_to_critical_route_drains() {
    let plans = plan_route_drain_capacity(
        &[
            DrainDemand {
                route_id: "routine".into(),
                queued_events: 100,
                seconds_remaining: 20,
                critical: false,
            },
            DrainDemand {
                route_id: "critical".into(),
                queued_events: 1000,
                seconds_remaining: 5,
                critical: true,
            },
            DrainDemand {
                route_id: "waiting".into(),
                queued_events: 20,
                seconds_remaining: 30,
                critical: false,
            },
        ],
        &[
            DrainCapacity {
                worker_id: "fast".into(),
                events_per_second: 100,
            },
            DrainCapacity {
                worker_id: "slow".into(),
                events_per_second: 10,
            },
        ],
    )
    .unwrap();
    assert_eq!(plans[0].route_id, "critical");
    assert_eq!(plans[0].worker_id.as_deref(), Some("fast"));
    assert_eq!(plans[0].state, "deadline_risk");
    assert_eq!(plans[1].state, "ready");
    assert_eq!(plans[2].state, "unassigned");
}

#[test]
fn rejects_duplicate_routes_and_unusable_workers() {
    let demand = DrainDemand {
        route_id: "route".into(),
        queued_events: 1,
        seconds_remaining: 1,
        critical: false,
    };
    assert!(
        plan_route_drain_capacity(
            &[
                demand.clone(),
                DrainDemand {
                    route_id: "ROUTE".into(),
                    ..demand
                }
            ],
            &[]
        )
        .is_err()
    );
    assert!(
        plan_route_drain_capacity(
            &[],
            &[DrainCapacity {
                worker_id: "worker".into(),
                events_per_second: 0
            }]
        )
        .is_err()
    );
}
