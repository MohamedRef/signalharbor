use signalharbor::{RouteSchemaPolicy, SchemaObservation, detect_schema_drift};

#[test]
fn reports_missing_and_unexpected_payload_fields() {
    let rows = detect_schema_drift(
        &[RouteSchemaPolicy {
            route_id: "orders".into(),
            event_type: "created".into(),
            required_fields: vec!["id".into(), "total".into()],
            allowed_fields: vec!["note".into()],
        }],
        &[SchemaObservation {
            route_id: "orders".into(),
            event_type: "created".into(),
            observed_fields: vec!["id".into(), "debug".into()],
        }],
    )
    .unwrap();
    assert_eq!(rows[0].missing_fields, vec!["total"]);
    assert_eq!(rows[0].unexpected_fields, vec!["debug"]);
}

#[test]
fn rejects_observations_without_a_policy() {
    assert!(
        detect_schema_drift(
            &[],
            &[SchemaObservation {
                route_id: "r".into(),
                event_type: "e".into(),
                observed_fields: vec![]
            }]
        )
        .is_err()
    );
}
