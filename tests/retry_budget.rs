use signalharbor::{HarborError, RetryDemand, allocate_retry_budget};

fn demand(tenant: &str, pending: u32, weight: u32) -> RetryDemand {
    RetryDemand {
        tenant: tenant.into(),
        pending,
        weight,
    }
}

#[test]
fn shares_capacity_in_weighted_rounds() {
    let result =
        allocate_retry_budget(&[demand("bulk", 20, 2), demand("critical", 20, 4)], 9).unwrap();

    assert_eq!(result[0].tenant, "bulk");
    assert_eq!((result[0].granted, result[0].deferred), (4, 16));
    assert_eq!((result[1].granted, result[1].deferred), (5, 15));
}

#[test]
fn unused_share_flows_to_tenants_that_still_have_demand() {
    let result =
        allocate_retry_budget(&[demand("small", 1, 3), demand("large", 10, 1)], 5).unwrap();

    assert_eq!(result[0].tenant, "large");
    assert_eq!((result[0].granted, result[0].deferred), (4, 6));
    assert_eq!(result[1].tenant, "small");
    assert_eq!((result[1].granted, result[1].deferred), (1, 0));
}

#[test]
fn rejects_ambiguous_or_invalid_demands() {
    assert!(matches!(
        allocate_retry_budget(&[], 5),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        allocate_retry_budget(&[demand("tenant", 2, 0)], 5),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        allocate_retry_budget(&[demand("Tenant", 1, 1), demand("tenant", 2, 1)], 5),
        Err(HarborError::Conflict(_))
    ));
}
