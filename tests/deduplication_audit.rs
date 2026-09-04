use signalharbor::{DedupeObservation, HarborError, audit_deduplication};

fn digest(byte: char) -> String {
    byte.to_string().repeat(64)
}

fn observation(tenant: &str, key: &str, payload: char, at: u64) -> DedupeObservation {
    DedupeObservation {
        tenant: tenant.into(),
        key: key.into(),
        payload_digest: digest(payload),
        received_at: at,
    }
}

#[test]
fn separates_exact_replays_from_conflicting_payloads_independent_of_input_order() {
    let rows = audit_deduplication(&[
        observation("tenant", "safe", 'c', 30),
        observation("Tenant", "conflict", 'a', 20),
        observation("tenant", "conflict", 'b', 30),
        observation("tenant", "safe", 'c', 10),
        observation("tenant", "conflict", 'a', 10),
    ])
    .unwrap();
    assert_eq!(rows[0].key, "conflict");
    assert_eq!(rows[0].canonical_payload_digest, digest('a'));
    assert_eq!(rows[0].observations, 3);
    assert_eq!(rows[0].exact_replays, 1);
    assert_eq!(rows[0].conflicting_payloads, 1);
    assert!(!rows[0].healthy);
    assert_eq!(rows[0].first_received_at, 10);
    assert_eq!(rows[0].last_received_at, 30);
    assert_eq!(rows[1].exact_replays, 1);
    assert!(rows[1].healthy);
}

#[test]
fn keeps_same_keys_isolated_between_tenants() {
    let rows = audit_deduplication(&[
        observation("north", "shared", 'a', 1),
        observation("south", "shared", 'b', 1),
    ])
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.healthy));
}

#[test]
fn rejects_malformed_observations() {
    let mut row = observation("tenant", "key", 'a', 1);
    for scenario in 0..4 {
        row.tenant = "tenant".into();
        row.key = "key".into();
        row.payload_digest = digest('a');
        row.received_at = 1;
        match scenario {
            0 => row.tenant = " ".into(),
            1 => row.key = " ".into(),
            2 => row.payload_digest = "not-a-digest".into(),
            _ => row.received_at = 0,
        }
        assert!(matches!(
            audit_deduplication(&[row.clone()]),
            Err(HarborError::Validation(_))
        ));
    }
}
