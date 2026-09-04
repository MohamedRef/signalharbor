use signalharbor::{ContentTypePolicy, WebhookContentObservation, assess_content_types};

#[test]
fn rejects_media_and_charset_mismatches_before_accepted_events() {
    let policy = ContentTypePolicy {
        route_id: "orders".into(),
        allowed_media_types: vec!["application/json".into()],
        require_utf8: true,
    };
    let rows = assess_content_types(
        &[policy],
        &[
            WebhookContentObservation {
                event_id: "accepted".into(),
                route_id: "orders".into(),
                media_type: "APPLICATION/JSON".into(),
                charset: Some("utf-8".into()),
            },
            WebhookContentObservation {
                event_id: "charset".into(),
                route_id: "orders".into(),
                media_type: "application/json".into(),
                charset: None,
            },
            WebhookContentObservation {
                event_id: "media".into(),
                route_id: "orders".into(),
                media_type: "text/plain".into(),
                charset: Some("utf-8".into()),
            },
        ],
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.state.as_str())
            .collect::<Vec<_>>(),
        vec!["unsupported_media_type", "charset_rejected", "accepted"]
    );
}

#[test]
fn rejects_duplicate_and_unknown_content_evidence() {
    let policy = ContentTypePolicy {
        route_id: "orders".into(),
        allowed_media_types: vec!["application/json".into()],
        require_utf8: false,
    };
    let event = WebhookContentObservation {
        event_id: "event".into(),
        route_id: "missing".into(),
        media_type: "application/json".into(),
        charset: None,
    };
    assert!(assess_content_types(&[policy.clone(), policy], &[]).is_err());
    assert!(
        assess_content_types(
            &[ContentTypePolicy {
                route_id: "orders".into(),
                allowed_media_types: vec!["application/json".into()],
                require_utf8: false
            }],
            &[event]
        )
        .is_err()
    );
}
