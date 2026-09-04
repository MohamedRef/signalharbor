use signalharbor::{DeliverySignature, assess_delivery_signature_freshness};

#[test]
fn ranks_stale_pending_signatures_before_delivered_evidence() {
    let rows = assess_delivery_signature_freshness(
        &[
            DeliverySignature {
                delivery_id: "pending-stale".into(),
                signed_at_epoch: 100,
                delivered_at_epoch: None,
                maximum_age_seconds: 20,
            },
            DeliverySignature {
                delivery_id: "delivered-stale".into(),
                signed_at_epoch: 100,
                delivered_at_epoch: Some(140),
                maximum_age_seconds: 20,
            },
            DeliverySignature {
                delivery_id: "pending-fresh".into(),
                signed_at_epoch: 140,
                delivered_at_epoch: None,
                maximum_age_seconds: 20,
            },
            DeliverySignature {
                delivery_id: "delivered-fresh".into(),
                signed_at_epoch: 100,
                delivered_at_epoch: Some(110),
                maximum_age_seconds: 20,
            },
        ],
        150,
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.state.as_str())
            .collect::<Vec<_>>(),
        vec![
            "pending_stale",
            "delivered_stale",
            "pending_fresh",
            "delivered_fresh"
        ]
    );
    assert_eq!(rows[0].age_seconds, 50);
}

#[test]
fn rejects_duplicate_signatures_and_impossible_chronology() {
    let valid = DeliverySignature {
        delivery_id: "delivery".into(),
        signed_at_epoch: 100,
        delivered_at_epoch: Some(110),
        maximum_age_seconds: 20,
    };
    assert!(
        assess_delivery_signature_freshness(
            &[
                valid.clone(),
                DeliverySignature {
                    delivery_id: "DELIVERY".into(),
                    ..valid.clone()
                }
            ],
            120
        )
        .is_err()
    );
    assert!(
        assess_delivery_signature_freshness(
            &[DeliverySignature {
                delivered_at_epoch: Some(90),
                ..valid.clone()
            }],
            120
        )
        .is_err()
    );
    assert!(
        assess_delivery_signature_freshness(
            &[DeliverySignature {
                maximum_age_seconds: 0,
                ..valid
            }],
            120
        )
        .is_err()
    );
}
