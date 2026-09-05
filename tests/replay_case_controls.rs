use signalharbor::{
    ReplayApproval, ReplayEvidence, assess_replay_evidence, authorize_replay, build_replay_scope,
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
