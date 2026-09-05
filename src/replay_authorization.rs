//! Independent authorization gates for replay execution.

use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayApproval {
    pub approval_id: String,
    pub replay_id: String,
    pub approver_id: String,
    pub role: String,
    pub expires_at_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayAuthorization {
    pub replay_id: String,
    pub authorized: bool,
    pub missing_roles: Vec<String>,
    pub expired_approvals: Vec<String>,
}

/// Evaluates independent, unexpired approvals for one replay.
///
/// # Errors
/// Returns an error for blank identities or duplicate approvals and approvers.
pub fn authorize_replay(
    replay_id: &str,
    requested_by: &str,
    required_roles: &[String],
    approvals: &[ReplayApproval],
    as_of_epoch: u64,
) -> Result<ReplayAuthorization, String> {
    let replay_id = replay_id.trim();
    let requested_by = requested_by.trim();
    if replay_id.is_empty() || requested_by.is_empty() {
        return Err("replay and requester identities are required".to_owned());
    }
    let required = required_roles
        .iter()
        .map(|role| role.trim().to_lowercase())
        .collect::<BTreeSet<_>>();
    if required.is_empty() || required.contains("") {
        return Err("replay authorization roles are required".to_owned());
    }
    let mut ids = BTreeSet::new();
    let mut approvers = BTreeSet::new();
    let mut provided = BTreeSet::new();
    let mut expired = Vec::new();
    for approval in approvals {
        if approval.replay_id.trim() != replay_id {
            continue;
        }
        let approval_id = approval.approval_id.trim();
        let approver = approval.approver_id.trim().to_lowercase();
        if approval_id.is_empty() || approver.is_empty() || approval.role.trim().is_empty() {
            return Err("replay approval identities and role are required".to_owned());
        }
        if !ids.insert(approval_id.to_lowercase()) {
            return Err(format!("duplicate replay approval: {approval_id}"));
        }
        if approver == requested_by.to_lowercase() {
            return Err("replay requester cannot approve their own replay".to_owned());
        }
        if !approvers.insert(approver) {
            return Err("replay approvals require distinct approvers".to_owned());
        }
        if approval.expires_at_epoch < as_of_epoch {
            expired.push(approval_id.to_owned());
        } else {
            provided.insert(approval.role.trim().to_lowercase());
        }
    }
    let missing_roles = required.difference(&provided).cloned().collect::<Vec<_>>();
    expired.sort();
    Ok(ReplayAuthorization {
        replay_id: replay_id.to_owned(),
        authorized: missing_roles.is_empty() && expired.is_empty(),
        missing_roles,
        expired_approvals: expired,
    })
}
