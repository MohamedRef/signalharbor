use signalharbor::{DeadLetterPartition, ExportCapacity, plan_dead_letter_exports};

#[test]
fn preserves_holds_and_allocates_oldest_exportable_partition() {
    let rows = plan_dead_letter_exports(
        &[
            DeadLetterPartition {
                partition_id: "held".into(),
                tenant_id: "t".into(),
                queued_events: 4,
                oldest_age_minutes: 100,
                legal_hold: true,
            },
            DeadLetterPartition {
                partition_id: "open".into(),
                tenant_id: "t".into(),
                queued_events: 5,
                oldest_age_minutes: 90,
                legal_hold: false,
            },
        ],
        &[ExportCapacity {
            exporter_id: "export".into(),
            permitted_tenants: vec!["t".into()],
            event_capacity: 3,
        }],
    )
    .unwrap();
    assert_eq!(rows[0].decision, "legal_hold");
    assert_eq!(rows[1].decision, "partial");
    assert_eq!(rows[1].exported_events, 3);
}

#[test]
fn rejects_duplicate_partitions() {
    let partition = DeadLetterPartition {
        partition_id: "p".into(),
        tenant_id: "t".into(),
        queued_events: 1,
        oldest_age_minutes: 1,
        legal_hold: false,
    };
    assert!(
        plan_dead_letter_exports(
            &[
                partition.clone(),
                DeadLetterPartition {
                    partition_id: " P ".into(),
                    ..partition
                }
            ],
            &[]
        )
        .is_err()
    );
}
