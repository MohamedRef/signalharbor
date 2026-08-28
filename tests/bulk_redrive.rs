use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use signalharbor::{AttemptOutcome, DeliveryState, HarborError, Route, SignalHarbor, WebhookInput};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct Journal(PathBuf);

impl Journal {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "signalharbor-bulk-redrive-{label}-{}-{}.journal",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )))
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        fs::remove_file(&self.0).ok();
        fs::remove_dir_all(&self.0).ok();
    }
}

fn route(id: &str) -> Route {
    Route::new(
        id,
        "tenant-a",
        &format!("{id}.*"),
        &format!("https://example.test/{id}"),
        1,
        10,
    )
    .unwrap()
}

fn dead_letters(label: &str, count: usize) -> (SignalHarbor, Journal, Vec<String>) {
    let journal = Journal::new(label);
    let mut harbor = SignalHarbor::open(&journal.0).unwrap();
    harbor.register_route(route("orders")).unwrap();
    let mut ids = Vec::new();
    for index in 0..count {
        let input = WebhookInput::new(
            "tenant-a",
            "orders.created",
            &format!("{label}-{index}"),
            format!("{{\"index\":{index}}}").into_bytes(),
            100 + index as u64,
        )
        .unwrap();
        let id = harbor.ingest(input).unwrap().delivery_ids[0].clone();
        harbor
            .record_attempt(&id, 100 + index as u64, AttemptOutcome::Http(400))
            .unwrap();
        ids.push(id);
    }
    (harbor, journal, ids)
}

#[test]
fn redrives_a_stable_bounded_route_batch() {
    let (mut harbor, _journal, ids) = dead_letters("bounded", 3);
    let result = harbor.redrive_route_dead_letters("orders", 500, 2).unwrap();
    assert_eq!(result.redriven_delivery_ids, ids[..2]);
    assert_eq!(result.remaining_dead_letters, 1);
    assert!(result.truncated);
    for id in &ids[..2] {
        let delivery = harbor.delivery(id).unwrap();
        assert_eq!(delivery.state, DeliveryState::Pending);
        assert_eq!(delivery.attempts, 0);
        assert_eq!(delivery.next_attempt_at, 500);
        assert!(delivery.last_error.is_none());
        assert_eq!(harbor.attempt_history(id).unwrap().len(), 1);
    }
    assert_eq!(
        harbor.delivery(&ids[2]).unwrap().state,
        DeliveryState::DeadLetter
    );
}

#[test]
fn followup_batch_drains_remaining_dead_letters_idempotently() {
    let (mut harbor, _journal, ids) = dead_letters("drain", 3);
    harbor.redrive_route_dead_letters("orders", 500, 2).unwrap();
    let followup = harbor.redrive_route_dead_letters("orders", 600, 2).unwrap();
    assert_eq!(followup.redriven_delivery_ids, vec![ids[2].clone()]);
    assert_eq!(followup.remaining_dead_letters, 0);
    assert!(!followup.truncated);
    let empty = harbor
        .redrive_route_dead_letters("orders", 700, 10)
        .unwrap();
    assert!(empty.redriven_delivery_ids.is_empty());
}

#[test]
fn route_batch_and_attempt_history_survive_restart() {
    let (mut harbor, journal, ids) = dead_letters("restart", 2);
    harbor
        .redrive_route_dead_letters("orders", 500, 10)
        .unwrap();
    drop(harbor);
    let reopened = SignalHarbor::open(&journal.0).unwrap();
    assert_eq!(reopened.summary().pending, 2);
    for id in ids {
        assert_eq!(reopened.delivery(&id).unwrap().next_attempt_at, 500);
        assert_eq!(reopened.attempt_history(&id).unwrap().len(), 1);
    }
}

#[test]
fn validates_limit_route_and_active_state_without_mutation() {
    let (mut harbor, _journal, ids) = dead_letters("validation", 1);
    assert!(matches!(
        harbor.redrive_route_dead_letters("orders", 500, 0),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.redrive_route_dead_letters("orders", 500, 1_001),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        harbor.redrive_route_dead_letters("missing", 500, 1),
        Err(HarborError::NotFound(_))
    ));
    harbor.set_route_active("orders", false).unwrap();
    assert!(matches!(
        harbor.redrive_route_dead_letters("orders", 500, 1),
        Err(HarborError::Conflict(_))
    ));
    assert_eq!(
        harbor.delivery(&ids[0]).unwrap().state,
        DeliveryState::DeadLetter
    );
}

#[test]
fn bulk_redrive_rejects_a_due_time_before_attempt_history() {
    let (mut harbor, _journal, ids) = dead_letters("chronology", 2);
    assert!(matches!(
        harbor.redrive_route_dead_letters("orders", 99, 2),
        Err(HarborError::Validation(message)) if message.contains("attempt history")
    ));
    assert!(
        ids.iter()
            .all(|id| harbor.delivery(id).unwrap().state == DeliveryState::DeadLetter)
    );
}

#[test]
fn journal_failure_exposes_no_partial_redrive() {
    let (mut harbor, journal, ids) = dead_letters("journal", 2);
    fs::remove_file(&journal.0).unwrap();
    fs::create_dir(&journal.0).unwrap();
    assert!(matches!(
        harbor.redrive_route_dead_letters("orders", 500, 2),
        Err(HarborError::Journal(_))
    ));
    assert!(
        ids.iter()
            .all(|id| harbor.delivery(id).unwrap().state == DeliveryState::DeadLetter)
    );
}
