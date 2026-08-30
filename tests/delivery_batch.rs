use signalharbor::{BatchEvent, HarborError, build_delivery_batches};

fn event(id: &str, endpoint: &str, payload_bytes: u64) -> BatchEvent {
    BatchEvent {
        id: id.into(),
        endpoint: endpoint.into(),
        payload_bytes,
    }
}

#[test]
fn batches_events_by_endpoint_count_and_payload() {
    let batches = build_delivery_batches(
        &[
            event("c", "orders", 40),
            event("a", "orders", 60),
            event("b", "orders", 50),
            event("d", "billing", 10),
        ],
        2,
        100,
    )
    .unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].endpoint, "billing");
    assert_eq!(batches[1].event_ids, vec!["a"]);
    assert_eq!(batches[2].event_ids, vec!["b", "c"]);
    assert_eq!(batches[2].payload_bytes, 90);
}

#[test]
fn rejects_duplicate_and_oversized_batch_events() {
    let duplicate = vec![event("a", "orders", 10), event("a", "orders", 10)];
    assert!(matches!(
        build_delivery_batches(&duplicate, 2, 100),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        build_delivery_batches(&[event("a", "orders", 101)], 2, 100),
        Err(HarborError::Validation(_))
    ));
}
