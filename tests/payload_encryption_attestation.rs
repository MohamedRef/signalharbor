use signalharbor::payload_encryption_attestation::{
    EncryptionAttestationState, PayloadEncryptionReceipt, attest_payload_encryption,
};

fn receipt(event_id: &str, nonce_id: &str) -> PayloadEncryptionReceipt {
    PayloadEncryptionReceipt {
        event_id: event_id.into(),
        route_id: "orders".into(),
        received_at_ms: 900,
        encrypted: true,
        key_id: Some("key-7".into()),
        nonce_id: Some(nonce_id.into()),
    }
}

#[test]
fn blocks_plaintext_and_nonce_reuse_before_attested_events() {
    let mut plaintext = receipt("plain", "n-1");
    plaintext.encrypted = false;
    plaintext.key_id = None;
    plaintext.nonce_id = None;
    let findings = attest_payload_encryption(
        &[
            receipt("first", "shared"),
            receipt("reused", "shared"),
            plaintext,
            receipt("ok", "n-2"),
        ],
        1_000,
        200,
    )
    .expect("valid evidence");
    assert_eq!(findings[0].state, EncryptionAttestationState::Blocked);
    assert_eq!(findings[1].state, EncryptionAttestationState::Blocked);
    assert_eq!(findings[2].state, EncryptionAttestationState::Attested);
    assert_eq!(findings[3].state, EncryptionAttestationState::Attested);
}

#[test]
fn sends_stale_and_incomplete_encryption_evidence_to_review() {
    let mut stale = receipt("stale", "n-1");
    stale.received_at_ms = 100;
    let mut incomplete = receipt("incomplete", "n-2");
    incomplete.key_id = None;
    let findings =
        attest_payload_encryption(&[stale, incomplete], 1_000, 200).expect("valid evidence");
    assert!(
        findings
            .iter()
            .all(|item| item.state == EncryptionAttestationState::Review)
    );
    assert!(
        findings
            .iter()
            .any(|item| item.reasons.contains(&"encryption receipt is stale"))
    );
}

#[test]
fn rejects_duplicate_events_and_future_receipts() {
    let error = attest_payload_encryption(
        &[receipt("Event", "a"), receipt(" event ", "b")],
        1_000,
        100,
    )
    .expect_err("duplicate event should fail");
    assert!(error.contains("duplicate"));
    let mut future = receipt("future", "a");
    future.received_at_ms = 1_001;
    let error =
        attest_payload_encryption(&[future], 1_000, 100).expect_err("future receipt should fail");
    assert!(error.contains("receipt time"));
}
