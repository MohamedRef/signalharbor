use signalharbor::{
    BurstReadiness, HarborError, TenantBurstLimit, TenantBurstSample, assess_tenant_bursts,
};

#[test]
fn separates_rejected_throttled_and_healthy_tenants() {
    let limits = [TenantBurstLimit {
        tenant_id: "tenant".into(),
        sustained_per_second: 10,
        burst_capacity: 20,
    }];
    for (accepted, queued, expected) in [
        (121, 0, BurstReadiness::Rejected),
        (100, 30, BurstReadiness::Throttled),
        (100, 20, BurstReadiness::Healthy),
    ] {
        let rows = assess_tenant_bursts(
            &limits,
            &[TenantBurstSample {
                tenant_id: "TENANT".into(),
                window_seconds: 10,
                accepted_events: accepted,
                queued_events: queued,
            }],
        )
        .unwrap();
        assert_eq!(rows[0].readiness, expected);
    }
}

#[test]
fn rejects_duplicate_limits_unknown_tenants_and_overflow() {
    let limit = TenantBurstLimit {
        tenant_id: "tenant".into(),
        sustained_per_second: 1,
        burst_capacity: 1,
    };
    assert!(matches!(
        assess_tenant_bursts(
            &[
                limit.clone(),
                TenantBurstLimit {
                    tenant_id: "TENANT".into(),
                    ..limit.clone()
                }
            ],
            &[]
        ),
        Err(HarborError::Validation(_))
    ));
    assert!(
        assess_tenant_bursts(
            &[],
            &[TenantBurstSample {
                tenant_id: "missing".into(),
                window_seconds: 1,
                accepted_events: 0,
                queued_events: 0
            }]
        )
        .is_err()
    );
    assert!(
        assess_tenant_bursts(
            &[TenantBurstLimit {
                sustained_per_second: u64::MAX,
                ..limit
            }],
            &[TenantBurstSample {
                tenant_id: "tenant".into(),
                window_seconds: 2,
                accepted_events: 0,
                queued_events: 0
            }]
        )
        .is_err()
    );
}
