use signalharbor::{
    CompressionPolicy, CompressionReadiness, CompressionSample, HarborError,
    assess_compression_readiness,
};

fn sample(id: &str, original: u64, encoded: u64) -> CompressionSample {
    CompressionSample {
        route_id: "orders".into(),
        event_id: id.into(),
        encoding: "gzip".into(),
        original_bytes: original,
        encoded_bytes: encoded,
    }
}

#[test]
fn distinguishes_expansion_weak_savings_ready_and_small_payloads() {
    let policy = CompressionPolicy {
        route_id: "orders".into(),
        supported_encodings: vec!["gzip".into()],
        minimum_payload_bytes: 1_000,
        minimum_savings_basis_points: 2_000,
    };
    let rows = assess_compression_readiness(
        &[policy],
        &[
            sample("expand", 2_000, 2_001),
            sample("weak", 2_000, 1_700),
            sample("ready", 2_000, 1_000),
            sample("small", 999, 400),
        ],
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.readiness).collect::<Vec<_>>(),
        [
            CompressionReadiness::InvalidExpansion,
            CompressionReadiness::InsufficientSavings,
            CompressionReadiness::Ready,
            CompressionReadiness::BelowThreshold
        ]
    );
    assert_eq!(rows[2].savings_basis_points, 5_000);
}

#[test]
fn rejects_unsupported_encodings_and_aliased_routes() {
    let policy = CompressionPolicy {
        route_id: "orders".into(),
        supported_encodings: vec!["gzip".into()],
        minimum_payload_bytes: 1,
        minimum_savings_basis_points: 1,
    };
    let mut unsupported = sample("event", 10, 5);
    unsupported.encoding = "br".into();
    assert!(assess_compression_readiness(&[policy.clone()], &[unsupported]).is_err());
    assert!(matches!(
        assess_compression_readiness(
            &[
                policy.clone(),
                CompressionPolicy {
                    route_id: "ORDERS".into(),
                    ..policy
                }
            ],
            &[]
        ),
        Err(HarborError::Validation(_))
    ));
}
