use signalharbor::{
    HarborError, PayloadSizePolicy, PayloadSizeReadiness, WebhookPayloadSample,
    assess_payload_sizes,
};

#[test]
fn classifies_payloads_at_exact_warning_and_rejection_boundaries() {
    let policy = PayloadSizePolicy {
        route_id: "orders".into(),
        maximum_bytes: 1_000,
        warning_percent: 80,
    };
    let rows = assess_payload_sizes(
        &[policy],
        &[
            WebhookPayloadSample {
                route_id: "ORDERS".into(),
                event_id: "rejected".into(),
                payload_bytes: 1_001,
            },
            WebhookPayloadSample {
                route_id: "orders".into(),
                event_id: "warning".into(),
                payload_bytes: 800,
            },
            WebhookPayloadSample {
                route_id: "orders".into(),
                event_id: "accepted".into(),
                payload_bytes: 799,
            },
        ],
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.readiness).collect::<Vec<_>>(),
        [
            PayloadSizeReadiness::Rejected,
            PayloadSizeReadiness::Warning,
            PayloadSizeReadiness::Accepted
        ]
    );
    assert_eq!(rows[0].remaining_bytes, 0);
    assert_eq!(rows[2].remaining_bytes, 201);
}

#[test]
fn rejects_ambiguous_policies_and_unknown_routes() {
    let policy = PayloadSizePolicy {
        route_id: "route".into(),
        maximum_bytes: 10,
        warning_percent: 80,
    };
    assert!(matches!(
        assess_payload_sizes(
            &[
                policy.clone(),
                PayloadSizePolicy {
                    route_id: "ROUTE".into(),
                    ..policy.clone()
                }
            ],
            &[]
        ),
        Err(HarborError::Validation(_))
    ));
    assert!(
        assess_payload_sizes(
            &[policy],
            &[WebhookPayloadSample {
                route_id: "missing".into(),
                event_id: "e".into(),
                payload_bytes: 1
            }]
        )
        .is_err()
    );
}
