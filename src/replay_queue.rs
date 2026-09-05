//! Replay queue prioritization with tenant fairness.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayQueueEntry {
    pub replay_id: String,
    pub tenant_id: String,
    pub event_count: usize,
    pub oldest_age_minutes: u64,
    pub priority: u32,
    pub authorized: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayQueueDecision {
    pub replay_id: String,
    pub tenant_id: String,
    pub admitted_events: usize,
    pub deferred_events: usize,
    pub reason: String,
}

/// Allocates a replay event budget fairly across tenants and priority levels.
///
/// # Errors
/// Returns an error for blank identities, duplicates, zero demands, or a zero budget.
pub fn allocate_replay_queue(
    entries: &[ReplayQueueEntry],
    event_budget: usize,
    per_tenant_limit: usize,
) -> Result<Vec<ReplayQueueDecision>, String> {
    if event_budget == 0 || per_tenant_limit == 0 {
        return Err("replay event budget and tenant limit must be positive".to_owned());
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut ordered = entries.to_vec();
    for entry in &ordered {
        if entry.replay_id.trim().is_empty()
            || entry.tenant_id.trim().is_empty()
            || entry.event_count == 0
        {
            return Err("replay queue identities and event count are required".to_owned());
        }
        if !seen.insert(entry.replay_id.trim().to_lowercase()) {
            return Err(format!("duplicate replay queue entry: {}", entry.replay_id));
        }
    }
    ordered.sort_by(|left, right| {
        right
            .authorized
            .cmp(&left.authorized)
            .then_with(|| right.priority.cmp(&left.priority))
            .then_with(|| right.oldest_age_minutes.cmp(&left.oldest_age_minutes))
            .then_with(|| left.replay_id.cmp(&right.replay_id))
    });
    let mut budget = event_budget;
    let mut tenant_usage = BTreeMap::<String, usize>::new();
    let mut decisions = Vec::with_capacity(ordered.len());
    for entry in ordered {
        let tenant = entry.tenant_id.trim().to_lowercase();
        if !entry.authorized {
            decisions.push(ReplayQueueDecision {
                replay_id: entry.replay_id.trim().to_owned(),
                tenant_id: tenant,
                admitted_events: 0,
                deferred_events: entry.event_count,
                reason: "authorization_required".to_owned(),
            });
            continue;
        }
        let used = tenant_usage.entry(tenant.clone()).or_default();
        let tenant_available = per_tenant_limit.saturating_sub(*used);
        let admitted = entry.event_count.min(tenant_available).min(budget);
        *used += admitted;
        budget -= admitted;
        let reason = if admitted == entry.event_count {
            "admitted"
        } else if tenant_available == 0 {
            "tenant_limit"
        } else if budget == 0 {
            "budget_exhausted"
        } else {
            "partially_admitted"
        };
        decisions.push(ReplayQueueDecision {
            replay_id: entry.replay_id.trim().to_owned(),
            tenant_id: tenant,
            admitted_events: admitted,
            deferred_events: entry.event_count - admitted,
            reason: reason.to_owned(),
        });
    }
    Ok(decisions)
}
