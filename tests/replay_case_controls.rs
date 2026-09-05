use signalharbor::{
    ReplayApproval, ReplayArtifact, ReplayEventRow, ReplayEvidence, ReplayHeaderSet,
    assess_replay_evidence, assess_replay_retention, authorize_replay, build_replay_scope,
    page_replay_events, redact_replay_headers,
};

#[test]
fn scopes_replay_and_requires_independent_roles() {
    let scope = build_replay_scope(
        "tenant-a",
        &["route-a".into()],
        &["event-b".into(), "event-a".into()],
    )
    .unwrap();
    assert_eq!(scope.event_ids, vec!["event-a", "event-b"]);
    let authorization = authorize_replay(
        "replay-1",
        "requester",
        &["operations".into(), "security".into()],
        &[
            ReplayApproval {
                approval_id: "approval-1".into(),
                replay_id: "replay-1".into(),
                approver_id: "operator".into(),
                role: "operations".into(),
                expires_at_epoch: 200,
            },
            ReplayApproval {
                approval_id: "approval-2".into(),
                replay_id: "replay-1".into(),
                approver_id: "security".into(),
                role: "security".into(),
                expires_at_epoch: 200,
            },
        ],
        100,
    )
    .unwrap();
    assert!(authorization.authorized);
}

#[test]
fn redacts_headers_and_pages_replay_events_stably() {
    let headers = redact_replay_headers(
        &ReplayHeaderSet {
            event_id: "event-a".into(),
            headers: [
                ("Authorization".into(), "secret".into()),
                ("X-Trace".into(), "t".into()),
            ]
            .into(),
        },
        &["authorization".into()],
    )
    .unwrap();
    assert_eq!(headers.headers["authorization"], "[redacted]");
    let page = page_replay_events(
        &[
            ReplayEventRow {
                event_id: "b".into(),
                received_at_epoch: 2,
            },
            ReplayEventRow {
                event_id: "a".into(),
                received_at_epoch: 1,
            },
        ],
        None,
        1,
    )
    .unwrap();
    assert_eq!(page.rows[0].event_id, "a");
    assert_eq!(page.next_cursor.as_deref(), Some("a"));
}

#[test]
fn preserves_legal_holds_during_replay_retention() {
    let decisions = assess_replay_retention(
        &[
            ReplayArtifact {
                artifact_id: "expired".into(),
                created_at_epoch: 0,
                retention_days: 1,
                legal_hold: false,
                active_replay: false,
            },
            ReplayArtifact {
                artifact_id: "held".into(),
                created_at_epoch: 0,
                retention_days: 1,
                legal_hold: true,
                active_replay: false,
            },
        ],
        200_000,
    )
    .unwrap();
    assert!(
        decisions
            .iter()
            .any(|item| item.artifact_id == "expired" && item.purge_eligible)
    );
    assert!(
        decisions
            .iter()
            .any(|item| item.artifact_id == "held" && !item.purge_eligible)
    );
}

#[test]
fn blocks_missing_or_quarantined_replay_evidence() {
    let report = assess_replay_evidence(
        &["event-a".into(), "event-b".into()],
        &[ReplayEvidence {
            evidence_id: "evidence-a".into(),
            event_id: "event-a".into(),
            payload_sha256: "a".repeat(64),
            captured_at_epoch: 90,
            quarantined: true,
        }],
        100,
    )
    .unwrap();
    assert!(!report.complete);
    assert_eq!(report.missing_event_ids, vec!["event-b"]);
    assert_eq!(report.quarantined_event_ids, vec!["event-a"]);
}
