use signalharbor::{EndpointCertificate, plan_certificate_renewals};

#[test]
fn plans_due_and_expired_certificates() {
    let renewals = plan_certificate_renewals(
        &[
            EndpointCertificate {
                endpoint_id: "expired".into(),
                fingerprint: "aa".into(),
                expires_at_epoch: 900,
                renewal_lead_seconds: 100,
            },
            EndpointCertificate {
                endpoint_id: "due".into(),
                fingerprint: "bb".into(),
                expires_at_epoch: 1_050,
                renewal_lead_seconds: 100,
            },
            EndpointCertificate {
                endpoint_id: "fresh".into(),
                fingerprint: "cc".into(),
                expires_at_epoch: 2_000,
                renewal_lead_seconds: 100,
            },
        ],
        1_000,
    )
    .expect("renewals");
    assert_eq!(renewals.len(), 2);
    assert_eq!(renewals[0].endpoint_id, "expired");
    assert!(renewals[0].expired);
    assert_eq!(renewals[1].endpoint_id, "due");
}

#[test]
fn rejects_duplicate_endpoints() {
    let row = EndpointCertificate {
        endpoint_id: "endpoint".into(),
        fingerprint: "aa".into(),
        expires_at_epoch: 2_000,
        renewal_lead_seconds: 100,
    };
    assert!(plan_certificate_renewals(&[row.clone(), row], 1_000).is_err());
}
