use signalharbor::{HarborError, SignatureFreshness, SignatureWindow, assess_signature_window};

fn window(signed: i64, received: i64) -> SignatureWindow {
    SignatureWindow {
        signed_at_epoch_seconds: signed,
        received_at_epoch_seconds: received,
        maximum_age_seconds: 300,
        maximum_future_skew_seconds: 20,
    }
}

#[test]
fn accepts_fresh_signatures_and_bounded_clock_skew() {
    let fresh = assess_signature_window(&window(1_000, 1_250)).unwrap();
    assert_eq!(fresh.freshness, SignatureFreshness::Fresh);
    assert!(fresh.acceptable);

    let future = assess_signature_window(&window(1_015, 1_000)).unwrap();
    assert_eq!(future.freshness, SignatureFreshness::Future);
    assert_eq!(future.age_seconds, -15);
    assert!(future.acceptable);
}

#[test]
fn rejects_expired_or_excessively_future_signatures() {
    let expired = assess_signature_window(&window(1_000, 1_301)).unwrap();
    assert_eq!(expired.freshness, SignatureFreshness::Expired);
    assert!(!expired.acceptable);
    let future = assess_signature_window(&window(1_100, 1_000)).unwrap();
    assert!(!future.acceptable);
}

#[test]
fn rejects_invalid_signature_controls() {
    let mut invalid = window(1, 2);
    invalid.maximum_age_seconds = 0;
    assert!(matches!(
        assess_signature_window(&invalid),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        assess_signature_window(&window(-1, 2)),
        Err(HarborError::Validation(_))
    ));
}
