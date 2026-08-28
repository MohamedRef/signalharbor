use signalharbor::{EndpointHealthMonitor, EndpointStatus, HarborError};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn journal() -> PathBuf {
    std::env::temp_dir().join(format!(
        "signalharbor-health-{}-{}.log",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn successful_probe_enables_routing_and_survives_replay() {
    let path = journal();
    let mut monitor = EndpointHealthMonitor::open(&path).unwrap();
    let health = monitor
        .register("billing", "https://hooks.example/billing", 30, 500, 3, 100)
        .unwrap();
    assert!(!monitor.routing_allowed("billing", 100));
    let outcome = monitor
        .record_probe("billing", &health.endpoint, Some(204), 50, None, 100, 1)
        .unwrap();
    assert!(outcome.successful && outcome.routing_allowed);
    assert_eq!(outcome.health.status, EndpointStatus::Healthy);
    drop(monitor);
    let restored = EndpointHealthMonitor::open(&path).unwrap();
    assert_eq!(restored.health("billing").unwrap().version, 2);
    assert!(restored.routing_allowed("billing", 159));
    assert!(!restored.routing_allowed("billing", 161));
}

#[test]
fn failures_degrade_then_stop_routing_at_threshold() {
    let path = journal();
    let mut monitor = EndpointHealthMonitor::open(path).unwrap();
    let health = monitor
        .register("orders", "https://hooks.example/orders", 10, 200, 2, 10)
        .unwrap();
    let first = monitor
        .record_probe("orders", &health.endpoint, Some(503), 20, None, 10, 1)
        .unwrap();
    assert_eq!(first.health.status, EndpointStatus::Degraded);
    assert!(first.routing_allowed);
    let second = monitor
        .record_probe(
            "orders",
            &health.endpoint,
            None,
            250,
            Some("connection reset"),
            20,
            2,
        )
        .unwrap();
    assert_eq!(second.health.status, EndpointStatus::Unavailable);
    assert!(!second.routing_allowed);
    assert!(!monitor.routing_allowed("orders", 20));
}

#[test]
fn recovery_resets_failure_counter() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    let health = monitor
        .register("route", "https://example.test/health", 5, 100, 2, 0)
        .unwrap();
    monitor
        .record_probe("route", &health.endpoint, Some(500), 10, None, 0, 1)
        .unwrap();
    let recovered = monitor
        .record_probe("route", &health.endpoint, Some(200), 90, None, 5, 2)
        .unwrap();
    assert_eq!(recovered.health.status, EndpointStatus::Healthy);
    assert_eq!(recovered.health.consecutive_failures, 0);
    assert_eq!(recovered.health.last_error, None);
}

#[test]
fn timeout_without_transport_error_has_deterministic_reason() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    let health = monitor
        .register("route", "https://example.test/health", 5, 100, 1, 0)
        .unwrap();
    let outcome = monitor
        .record_probe("route", &health.endpoint, Some(200), 101, None, 0, 1)
        .unwrap();
    assert_eq!(
        outcome.health.last_error.as_deref(),
        Some("probe timed out after 101ms")
    );
}

#[test]
fn due_queue_orders_deadline_then_route() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    monitor
        .register("z-route", "https://z.test/health", 10, 100, 2, 20)
        .unwrap();
    monitor
        .register("a-route", "https://a.test/health", 10, 100, 2, 20)
        .unwrap();
    monitor
        .register("early", "https://early.test/health", 10, 100, 2, 10)
        .unwrap();
    assert_eq!(monitor.due_routes(20), ["early", "a-route", "z-route"]);
}

#[test]
fn probe_validates_due_endpoint_latency_and_version() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    let health = monitor
        .register("route", "https://example.test/health", 30, 100, 2, 100)
        .unwrap();
    assert!(matches!(
        monitor.record_probe("route", &health.endpoint, Some(200), 10, None, 99, 1),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        monitor.record_probe(
            "route",
            "https://other.test/health",
            Some(200),
            10,
            None,
            100,
            1
        ),
        Err(HarborError::Conflict(_))
    ));
    assert!(matches!(
        monitor.record_probe("route", &health.endpoint, Some(200), 0, None, 100, 1),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        monitor.record_probe("route", &health.endpoint, Some(600), 10, None, 100, 1),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        monitor.record_probe("route", &health.endpoint, Some(200), 10, None, 100, 99),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn probe_rejects_a_next_schedule_that_overflows() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    let health = monitor
        .register("route", "https://example.test/health", 30, 100, 2, u64::MAX)
        .unwrap();
    assert!(matches!(
        monitor.record_probe(
            "route",
            &health.endpoint,
            Some(200),
            10,
            None,
            u64::MAX,
            1
        ),
        Err(HarborError::Validation(message)) if message.contains("timestamp range")
    ));
    assert_eq!(monitor.health("route").unwrap().version, 1);
}

#[test]
fn registration_rejects_invalid_policy_and_duplicates() {
    let mut monitor = EndpointHealthMonitor::open(journal()).unwrap();
    assert!(matches!(
        monitor.register("route", "http://insecure.test", 30, 100, 2, 0),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        monitor.register("route", "https://example.test", 1, 100, 2, 0),
        Err(HarborError::Validation(_))
    ));
    monitor
        .register("route", "https://example.test", 30, 100, 2, 0)
        .unwrap();
    assert!(matches!(
        monitor.register("route", "https://example.test", 30, 100, 2, 0),
        Err(HarborError::Conflict(_))
    ));
}

#[test]
fn malformed_and_non_contiguous_journals_fail_closed() {
    let path = journal();
    fs::write(&path, "bad\n").unwrap();
    assert!(matches!(
        EndpointHealthMonitor::open(&path),
        Err(HarborError::Journal(_))
    ));
    let second_path = journal();
    let mut monitor = EndpointHealthMonitor::open(&second_path).unwrap();
    let health = monitor
        .register("route", "https://example.test", 30, 100, 2, 0)
        .unwrap();
    monitor
        .record_probe("route", &health.endpoint, Some(200), 10, None, 0, 1)
        .unwrap();
    drop(monitor);
    let valid = fs::read_to_string(&second_path).unwrap();
    let mut lines: Vec<String> = valid.lines().map(str::to_owned).collect();
    let last = lines.last_mut().unwrap();
    let split = last.rfind('\t').unwrap();
    last.replace_range(split + 1.., "3");
    fs::write(&second_path, format!("{}\n", lines.join("\n"))).unwrap();
    assert!(matches!(
        EndpointHealthMonitor::open(&second_path),
        Err(HarborError::Journal(_))
    ));
}
