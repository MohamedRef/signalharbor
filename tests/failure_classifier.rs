use signalharbor::{
    DeliveryDisposition, DeliveryResponse, HarborError, classify_delivery_response,
    parse_retry_after_delta,
};

#[test]
fn success_is_delivered_and_permanent_client_errors_drop() {
    assert_eq!(
        classify_delivery_response(DeliveryResponse {
            status_code: 204,
            retry_after_seconds: None,
        }),
        Ok(DeliveryDisposition::Delivered)
    );
    assert_eq!(
        classify_delivery_response(DeliveryResponse {
            status_code: 422,
            retry_after_seconds: None,
        }),
        Ok(DeliveryDisposition::Drop)
    );
}

#[test]
fn throttling_and_server_failures_retry_with_optional_override() {
    assert_eq!(
        classify_delivery_response(DeliveryResponse {
            status_code: 429,
            retry_after_seconds: Some(90),
        }),
        Ok(DeliveryDisposition::Retry {
            delay_override_seconds: Some(90),
        })
    );
    assert_eq!(
        classify_delivery_response(DeliveryResponse {
            status_code: 503,
            retry_after_seconds: None,
        }),
        Ok(DeliveryDisposition::Retry {
            delay_override_seconds: None,
        })
    );
}

#[test]
fn malformed_status_and_retry_after_are_rejected() {
    for response in [
        DeliveryResponse {
            status_code: 99,
            retry_after_seconds: None,
        },
        DeliveryResponse {
            status_code: 429,
            retry_after_seconds: Some(86_401),
        },
        DeliveryResponse {
            status_code: 404,
            retry_after_seconds: Some(1),
        },
        DeliveryResponse {
            status_code: 204,
            retry_after_seconds: Some(1),
        },
    ] {
        assert!(matches!(
            classify_delivery_response(response),
            Err(HarborError::Validation(_))
        ));
    }
}

#[test]
fn retry_after_delta_parser_accepts_bounded_decimal_fields() {
    for (field, expected) in [("0", 0), (" 90 ", 90), ("000120", 120), ("86400", 86_400)] {
        assert_eq!(parse_retry_after_delta(field), Ok(expected));
    }
}

#[test]
fn retry_after_delta_parser_rejects_ambiguous_or_excessive_fields() {
    for field in [
        "",
        " ",
        "+10",
        "-1",
        "1.5",
        "10, 20",
        "Wed, 21 Oct 2015 07:28:00 GMT",
        "86401",
        "18446744073709551616",
    ] {
        assert!(matches!(
            parse_retry_after_delta(field),
            Err(HarborError::Validation(_))
        ));
    }
}
