use signalharbor::{EndpointCredential, HarborError, RotationReadiness, plan_credential_rotations};

#[test]
fn identifies_expired_and_unprepared_endpoint_credentials() {
    let got = plan_credential_rotations(
        &[
            EndpointCredential {
                endpoint: "expired".into(),
                expires_at_epoch: 99,
                replacement_staged: true,
            },
            EndpointCredential {
                endpoint: "unstaged".into(),
                expires_at_epoch: 120,
                replacement_staged: false,
            },
            EndpointCredential {
                endpoint: "ready".into(),
                expires_at_epoch: 130,
                replacement_staged: true,
            },
            EndpointCredential {
                endpoint: "later".into(),
                expires_at_epoch: 200,
                replacement_staged: false,
            },
        ],
        100,
        30,
    )
    .unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].readiness, RotationReadiness::Expired);
    assert_eq!(got[1].readiness, RotationReadiness::ReplacementMissing);
    assert_eq!(got[2].readiness, RotationReadiness::Ready);
}

#[test]
fn rejects_duplicate_endpoints() {
    let row = EndpointCredential {
        endpoint: "same".into(),
        expires_at_epoch: 120,
        replacement_staged: true,
    };
    assert!(matches!(
        plan_credential_rotations(&[row.clone(), row], 100, 30),
        Err(HarborError::Validation(_))
    ));
}

#[test]
fn rejects_a_rotation_horizon_that_overflows() {
    assert!(matches!(
        plan_credential_rotations(&[], u64::MAX - 10, 11),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
}
