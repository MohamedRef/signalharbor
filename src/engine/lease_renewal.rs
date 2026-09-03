use super::SignalHarbor;
use crate::journal::Record;
use crate::model::required;
use crate::{Delivery, DeliveryState, HarborError};

impl SignalHarbor {
    /// Extends a live worker claim without recording a delivery attempt.
    ///
    /// The worker and expected expiry must match the current claim. Supplying
    /// the previous expiry acts as a compare-and-set guard against a delayed
    /// renewal overwriting a newer lease. The requested duration is measured
    /// from `renewed_at`, must be 1..=3,600 seconds and must extend the existing
    /// expiry. A claim cannot be resurrected at or after its expiry.
    ///
    /// Route maintenance and ingress controls do not revoke an already live
    /// claim. Renewal retains its captured endpoint, retry deadline and attempt
    /// count. The replacement delivery snapshot is durable before it is returned.
    ///
    /// # Errors
    ///
    /// Returns validation for an invalid worker, duration or deadline; conflict
    /// for terminal, expired, differently owned or stale claims; not-found for
    /// an unknown delivery; and journal errors without changing memory.
    pub fn renew_claim(
        &mut self,
        delivery_id: &str,
        worker: &str,
        expected_expires_at: u64,
        renewed_at: u64,
        lease_seconds: u64,
    ) -> Result<Delivery, HarborError> {
        let worker = required(worker, "worker", 120)?;
        if !(1..=3_600).contains(&lease_seconds) {
            return Err(HarborError::Validation(
                "lease_seconds must be from 1 through 3600".into(),
            ));
        }
        let expires_at = renewed_at.checked_add(lease_seconds).ok_or_else(|| {
            HarborError::Validation("lease expiry exceeds timestamp range".into())
        })?;
        let current = self.delivery(delivery_id)?;
        if current.state != DeliveryState::Pending
            || current.lease_owner.as_deref() != Some(worker.as_str())
            || current.lease_expires_at != Some(expected_expires_at)
            || expected_expires_at <= renewed_at
        {
            return Err(HarborError::Conflict(format!(
                "worker {worker} does not own the expected live lease for delivery {delivery_id}"
            )));
        }
        if renewed_at < current.next_attempt_at || expires_at <= expected_expires_at {
            return Err(HarborError::Validation(
                "renewal must be after the due time and extend the current expiry".into(),
            ));
        }
        let updated = Delivery {
            lease_expires_at: Some(expires_at),
            ..current
        };
        self.journal.append(&Record::Delivery(updated.clone()))?;
        self.deliveries.insert(updated.id.clone(), updated.clone());
        Ok(updated)
    }
}
