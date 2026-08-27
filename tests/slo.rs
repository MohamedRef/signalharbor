use signalharbor::{DeliveryObservation, DeliverySloObjective, HarborError, evaluate_delivery_slo};

fn objective() -> DeliverySloObjective {
    DeliverySloObjective {
        minimum_samples: 4,
        minimum_success_basis_points: 7_500,
        maximum_p95_latency_ms: 500,
        maximum_consecutive_failures: 2,
    }
}

fn observation(
    id: &str,
    completed_at: u64,
    latency_ms: u64,
    successful: bool,
) -> DeliveryObservation {
    DeliveryObservation {
        delivery_id: id.into(),
        route_id: "billing".into(),
        completed_at,
        latency_ms,
        successful,
    }
}

#[test]
fn healthy_sample_reports_rate_latency_and_streak() {
    let report = evaluate_delivery_slo(
        &[
            observation("d-3", 3, 300, true),
            observation("d-1", 1, 100, true),
            observation("d-4", 4, 400, false),
            observation("d-2", 2, 200, true),
        ],
        objective(),
    )
    .unwrap();
    assert_eq!(report.success_basis_points, 7_500);
    assert_eq!(report.p95_latency_ms, 400);
    assert_eq!(report.longest_failure_streak, 1);
    assert!(!report.breached);
}

#[test]
fn report_names_each_independent_slo_breach() {
    let report = evaluate_delivery_slo(
        &[
            observation("d-1", 1, 100, true),
            observation("d-2", 2, 700, false),
            observation("d-3", 3, 800, false),
            observation("d-4", 4, 900, false),
        ],
        objective(),
    )
    .unwrap();
    assert!(report.breached);
    assert_eq!(
        report.breached_reasons,
        ["success_rate", "p95_latency", "failure_streak"]
    );
}

#[test]
fn sparse_sample_defers_rate_and_latency_but_honors_failure_streak() {
    let report = evaluate_delivery_slo(
        &[
            observation("d-1", 1, 900, false),
            observation("d-2", 2, 900, false),
            observation("d-3", 3, 900, false),
        ],
        objective(),
    )
    .unwrap();
    assert!(!report.enough_samples);
    assert_eq!(report.breached_reasons, ["failure_streak"]);
}

#[test]
fn rejects_duplicate_cross_route_and_invalid_observations() {
    let first = observation("d-1", 1, 10, true);
    assert!(matches!(
        evaluate_delivery_slo(&[first.clone(), first.clone()], objective()),
        Err(HarborError::Conflict(_))
    ));
    let mut other = observation("d-2", 2, 10, true);
    other.route_id = "orders".into();
    assert!(matches!(
        evaluate_delivery_slo(&[first.clone(), other], objective()),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        evaluate_delivery_slo(&[observation("d-3", 3, 0, true)], objective()),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        evaluate_delivery_slo(&[], objective()),
        Err(HarborError::Validation(_))
    ));
}
