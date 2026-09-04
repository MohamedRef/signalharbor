use signalharbor::{EndpointMaintenanceWindow, assess_maintenance_windows};

#[test]
fn distinguishes_scheduled_drain_and_maintenance_states() {
    let window = EndpointMaintenanceWindow {
        endpoint_id: "e".into(),
        starts_at_epoch: 1_000,
        ends_at_epoch: 1_100,
        drain_lead_seconds: 60,
    };
    assert_eq!(
        assess_maintenance_windows(std::slice::from_ref(&window), 900).unwrap()[0].state,
        "scheduled"
    );
    assert_eq!(
        assess_maintenance_windows(std::slice::from_ref(&window), 950).unwrap()[0].state,
        "draining"
    );
    assert_eq!(
        assess_maintenance_windows(&[window], 1_050).unwrap()[0].state,
        "maintenance"
    );
}

#[test]
fn rejects_inverted_windows() {
    assert!(
        assess_maintenance_windows(
            &[EndpointMaintenanceWindow {
                endpoint_id: "e".into(),
                starts_at_epoch: 2,
                ends_at_epoch: 1,
                drain_lead_seconds: 0
            }],
            0
        )
        .is_err()
    );
}
