use signalharbor::{
    EndpointMigrationInput, EndpointMigrationReadiness, HarborError, assess_endpoint_migrations,
};

fn input(route: &str, traffic: u8, success: u16, samples: u64) -> EndpointMigrationInput {
    EndpointMigrationInput {
        route_id: route.into(),
        current_endpoint: "https://old.test".into(),
        candidate_endpoint: "https://new.test".into(),
        candidate_traffic_percent: traffic,
        candidate_success_basis_points: success,
        minimum_success_basis_points: 9_900,
        candidate_samples: samples,
        minimum_samples: 100,
        rollback_ready: true,
    }
}

#[test]
fn advances_healthy_migrations_and_rolls_back_bad_candidates() {
    let rows = assess_endpoint_migrations(
        &[
            input("advance", 40, 9_950, 100),
            input("bad", 20, 9_000, 100),
            input("hold", 10, 10_000, 99),
        ],
        25,
    )
    .unwrap();
    assert_eq!(
        rows[0].readiness,
        EndpointMigrationReadiness::RollbackRequired
    );
    assert_eq!(rows[0].next_candidate_traffic_percent, 0);
    assert_eq!(rows[1].readiness, EndpointMigrationReadiness::Hold);
    assert_eq!(
        rows[2].readiness,
        EndpointMigrationReadiness::ReadyToAdvance
    );
    assert_eq!(rows[2].next_candidate_traffic_percent, 65);
}

#[test]
fn rejects_aliased_routes_and_identical_endpoints() {
    assert!(matches!(
        assess_endpoint_migrations(&[input("A", 0, 10_000, 0), input("a", 0, 10_000, 0)], 10),
        Err(HarborError::Validation(_))
    ));
    let mut invalid = input("route", 0, 10_000, 0);
    invalid.candidate_endpoint = invalid.current_endpoint.clone();
    assert!(assess_endpoint_migrations(&[invalid], 10).is_err());
}
