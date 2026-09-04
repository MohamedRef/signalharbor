use signalharbor::{
    HarborError, SigningKeyWindow, SigningOverlapPolicy, SigningOverlapReadiness,
    assess_signing_overlap,
};

#[test]
fn classifies_safe_short_and_missing_key_overlap() {
    let policy = [SigningOverlapPolicy {
        route_id: "orders".into(),
        minimum_overlap_seconds: 60,
    }];
    for (start, expected) in [
        (940, SigningOverlapReadiness::Ready),
        (980, SigningOverlapReadiness::Insufficient),
        (1_001, SigningOverlapReadiness::Gap),
    ] {
        let rows = assess_signing_overlap(
            &policy,
            &[
                SigningKeyWindow {
                    route_id: "orders".into(),
                    key_id: "old".into(),
                    active_from: 0,
                    active_until: 1_000,
                },
                SigningKeyWindow {
                    route_id: "ORDERS".into(),
                    key_id: "new".into(),
                    active_from: start,
                    active_until: 2_000,
                },
            ],
        )
        .unwrap();
        assert_eq!(rows[0].readiness, expected);
    }
}

#[test]
fn rejects_unknown_routes_duplicate_keys_and_inverted_windows() {
    let policy = [SigningOverlapPolicy {
        route_id: "orders".into(),
        minimum_overlap_seconds: 60,
    }];
    let key = SigningKeyWindow {
        route_id: "orders".into(),
        key_id: "key".into(),
        active_from: 0,
        active_until: 10,
    };
    assert!(matches!(
        assess_signing_overlap(
            &policy,
            &[
                key.clone(),
                SigningKeyWindow {
                    key_id: "KEY".into(),
                    ..key.clone()
                }
            ]
        ),
        Err(HarborError::Validation(_))
    ));
    assert!(assess_signing_overlap(&[], &[key]).is_err());
}
