use signalharbor::retry_drain_capacity::{
    RetryDrainCapacityAllocation, RetryDrainCapacityDemand, RetryDrainCapacityResource,
    allocate_retry_drain_capacity,
};

#[test]
fn protects_priority_and_group_boundaries() {
    let resources = vec![
        RetryDrainCapacityResource {
            id: "east-2".into(),
            group: "east".into(),
            available_units: 3,
        },
        RetryDrainCapacityResource {
            id: "east-1".into(),
            group: " EAST ".into(),
            available_units: 4,
        },
        RetryDrainCapacityResource {
            id: "west-1".into(),
            group: "west".into(),
            available_units: 8,
        },
    ];
    let demands = vec![
        RetryDrainCapacityDemand {
            id: "routine".into(),
            group: "east".into(),
            required_units: 5,
            priority: 1,
        },
        RetryDrainCapacityDemand {
            id: "urgent".into(),
            group: "east".into(),
            required_units: 6,
            priority: 9,
        },
        RetryDrainCapacityDemand {
            id: "western".into(),
            group: "west".into(),
            required_units: 2,
            priority: 3,
        },
    ];

    let decisions = allocate_retry_drain_capacity(&resources, &demands).expect("valid allocation");
    assert_eq!(
        decisions
            .iter()
            .map(|item| item.demand_id.as_str())
            .collect::<Vec<_>>(),
        ["urgent", "western", "routine"]
    );
    assert_eq!(
        decisions[0].allocations,
        [
            RetryDrainCapacityAllocation {
                resource_id: "east-1".into(),
                units: 4
            },
            RetryDrainCapacityAllocation {
                resource_id: "east-2".into(),
                units: 2
            },
        ]
    );
    assert_eq!(decisions[0].shortfall_units, 0);
    assert_eq!(decisions[2].shortfall_units, 4);
}

#[test]
fn is_deterministic_across_input_order() {
    let resources = vec![
        RetryDrainCapacityResource {
            id: "b".into(),
            group: "shared".into(),
            available_units: 2,
        },
        RetryDrainCapacityResource {
            id: "a".into(),
            group: "shared".into(),
            available_units: 2,
        },
    ];
    let demands = vec![
        RetryDrainCapacityDemand {
            id: "second".into(),
            group: "shared".into(),
            required_units: 2,
            priority: 1,
        },
        RetryDrainCapacityDemand {
            id: "first".into(),
            group: "shared".into(),
            required_units: 3,
            priority: 2,
        },
    ];
    let mut reversed_resources = resources.clone();
    reversed_resources.reverse();
    let mut reversed_demands = demands.clone();
    reversed_demands.reverse();
    assert_eq!(
        allocate_retry_drain_capacity(&resources, &demands).unwrap(),
        allocate_retry_drain_capacity(&reversed_resources, &reversed_demands).unwrap()
    );
}

#[test]
fn rejects_unsafe_capacity_declarations() {
    let duplicates = vec![
        RetryDrainCapacityResource {
            id: "same".into(),
            group: "a".into(),
            available_units: 1,
        },
        RetryDrainCapacityResource {
            id: " SAME ".into(),
            group: "a".into(),
            available_units: 2,
        },
    ];
    assert!(
        allocate_retry_drain_capacity(&duplicates, &[])
            .unwrap_err()
            .contains("duplicate")
    );
    let zero = vec![RetryDrainCapacityResource {
        id: "resource".into(),
        group: "a".into(),
        available_units: 0,
    }];
    assert!(
        allocate_retry_drain_capacity(&zero, &[])
            .unwrap_err()
            .contains("positive")
    );
    let demand = vec![RetryDrainCapacityDemand {
        id: "demand".into(),
        group: "a".into(),
        required_units: 1,
        priority: 0,
    }];
    assert!(
        allocate_retry_drain_capacity(&[], &demand)
            .unwrap_err()
            .contains("positive")
    );
}
