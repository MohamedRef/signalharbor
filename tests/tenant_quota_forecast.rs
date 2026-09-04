use signalharbor::{TenantQuotaSample, forecast_tenant_quotas};

#[test]
fn ranks_exhausted_and_at_risk_tenant_quotas() {
    let rows = forecast_tenant_quotas(
        &[
            TenantQuotaSample {
                tenant_id: "healthy".into(),
                quota_events: 1000,
                used_events: 100,
                recent_events_per_minute: 10,
            },
            TenantQuotaSample {
                tenant_id: "idle".into(),
                quota_events: 1000,
                used_events: 100,
                recent_events_per_minute: 0,
            },
            TenantQuotaSample {
                tenant_id: "risk".into(),
                quota_events: 1000,
                used_events: 900,
                recent_events_per_minute: 20,
            },
            TenantQuotaSample {
                tenant_id: "exhausted".into(),
                quota_events: 1000,
                used_events: 1000,
                recent_events_per_minute: 20,
            },
        ],
        10,
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.state.as_str())
            .collect::<Vec<_>>(),
        vec!["exhausted", "at_risk", "healthy", "idle"]
    );
    assert_eq!(rows[1].minutes_until_exhausted, Some(5));
}

#[test]
fn rejects_invalid_tenant_quota_samples() {
    let valid = TenantQuotaSample {
        tenant_id: "tenant".into(),
        quota_events: 100,
        used_events: 10,
        recent_events_per_minute: 1,
    };
    assert!(
        forecast_tenant_quotas(
            &[
                valid.clone(),
                TenantQuotaSample {
                    tenant_id: "TENANT".into(),
                    ..valid.clone()
                }
            ],
            10
        )
        .is_err()
    );
    assert!(
        forecast_tenant_quotas(
            &[TenantQuotaSample {
                used_events: 101,
                ..valid
            }],
            10
        )
        .is_err()
    );
    assert!(forecast_tenant_quotas(&[], 0).is_err());
}
