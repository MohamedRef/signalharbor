use signalharbor::{FailoverEndpoint, HarborError, plan_route_failover};

#[test]
fn allocates_failover_demand_by_priority_and_health() {
    let plan = plan_route_failover(
        &[
            FailoverEndpoint {
                endpoint: "backup-b".into(),
                priority: 2,
                healthy: true,
                available_slots: 4,
            },
            FailoverEndpoint {
                endpoint: "backup-a".into(),
                priority: 1,
                healthy: true,
                available_slots: 3,
            },
            FailoverEndpoint {
                endpoint: "offline".into(),
                priority: 1,
                healthy: false,
                available_slots: 10,
            },
        ],
        9,
    )
    .unwrap();
    assert_eq!(plan.allocations[0].endpoint, "backup-a");
    assert_eq!(plan.allocations[0].allocated, 3);
    assert_eq!(plan.allocations[1].allocated, 4);
    assert_eq!(plan.unallocated, 2);
}

#[test]
fn rejects_duplicate_failover_endpoints_and_zero_demand() {
    let endpoint = FailoverEndpoint {
        endpoint: "backup".into(),
        priority: 1,
        healthy: true,
        available_slots: 1,
    };
    assert!(matches!(
        plan_route_failover(&[endpoint.clone(), endpoint], 1),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        plan_route_failover(&[], 0),
        Err(HarborError::Validation(_))
    ));
}

#[test]
fn rejects_endpoint_aliases_that_only_differ_by_case_or_whitespace() {
    let endpoints = [
        FailoverEndpoint {
            endpoint: " Backup-A ".into(),
            priority: 1,
            healthy: true,
            available_slots: 2,
        },
        FailoverEndpoint {
            endpoint: "backup-a".into(),
            priority: 2,
            healthy: true,
            available_slots: 2,
        },
    ];
    assert!(matches!(
        plan_route_failover(&endpoints, 2),
        Err(HarborError::Validation(message)) if message.contains("duplicate")
    ));
}
