use signalharbor::{ReceiverTlsPolicy, ReceiverTlsProbe, TlsReadiness, assess_receiver_tls};

#[test]
fn distinguishes_blocked_rotation_due_and_ready_receivers() {
    let policy = ReceiverTlsPolicy {
        receiver_id: "r".into(),
        minimum_protocol: 13,
        required_issuer: "ca".into(),
        minimum_days_remaining: 30,
    };
    let blocked = ReceiverTlsProbe {
        receiver_id: "r".into(),
        protocol: 12,
        issuer: "other".into(),
        days_remaining: 90,
        hostname_verified: false,
    };
    assert_eq!(
        assess_receiver_tls(std::slice::from_ref(&policy), &[blocked]).unwrap()[0].readiness,
        TlsReadiness::Blocked
    );
    let due = ReceiverTlsProbe {
        receiver_id: "r".into(),
        protocol: 13,
        issuer: "CA".into(),
        days_remaining: 10,
        hostname_verified: true,
    };
    assert_eq!(
        assess_receiver_tls(&[policy], &[due]).unwrap()[0].readiness,
        TlsReadiness::RotationDue
    );
}

#[test]
fn rejects_unsupported_minimum_protocols() {
    let policy = ReceiverTlsPolicy {
        receiver_id: "r".into(),
        minimum_protocol: 11,
        required_issuer: "ca".into(),
        minimum_days_remaining: 1,
    };
    assert!(assess_receiver_tls(&[policy], &[]).is_err());
}
