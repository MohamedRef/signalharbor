use signalharbor::{TenantQueueDepth, assess_tenant_fairness};

#[test]
fn detects_tenants_over_shared_queue_limits() {
    let result = assess_tenant_fairness(&[
        TenantQueueDepth {
            tenant_id: "large".into(),
            pending: 800,
            maximum_share_per_mille: 600,
        },
        TenantQueueDepth {
            tenant_id: "small".into(),
            pending: 200,
            maximum_share_per_mille: 400,
        },
    ])
    .expect("fairness");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].tenant_id, "large");
    assert_eq!(result[0].actual_share_per_mille, 800);
    assert_eq!(result[0].excess_share_per_mille, 200);
}

#[test]
fn empty_queue_has_no_fairness_breaches() {
    let result = assess_tenant_fairness(&[
        TenantQueueDepth {
            tenant_id: "a".into(),
            pending: 0,
            maximum_share_per_mille: 500,
        },
        TenantQueueDepth {
            tenant_id: "b".into(),
            pending: 0,
            maximum_share_per_mille: 500,
        },
    ])
    .expect("fairness");
    assert!(result.is_empty());
}

#[test]
fn measures_large_queue_shares_without_saturating() {
    let lower_half = u64::MAX / 2;
    let upper_half = u64::MAX - lower_half;
    let breaches = assess_tenant_fairness(&[
        TenantQueueDepth {
            tenant_id: "lower".into(),
            pending: lower_half,
            maximum_share_per_mille: 100,
        },
        TenantQueueDepth {
            tenant_id: "upper".into(),
            pending: upper_half,
            maximum_share_per_mille: 100,
        },
    ])
    .expect("large queue assessment");
    assert_eq!(breaches.len(), 2);
    assert_eq!(breaches[0].actual_share_per_mille, 501);
    assert_eq!(breaches[1].actual_share_per_mille, 500);
}
