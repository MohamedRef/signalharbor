use super::SignalHarbor;
use crate::model::required;
use crate::{AttemptOutcome, AttemptResult, DeliveryResponse, HarborError};

impl SignalHarbor {
    /// Records an HTTP response including the receiver's bounded retry delay.
    ///
    /// `retry_after_seconds` is a minimum waiting period, not permission to retry
    /// earlier than the route's exponential backoff. It must be at most one day
    /// and is accepted only on retryable statuses. The route attempt budget and
    /// circuit policy still apply; exhausted attempts become dead letters.
    ///
    /// The actual next deadline is journaled with the HTTP outcome and delivery
    /// snapshot in one transaction. No new deadline is invented for terminal
    /// outcomes. HTTP adapters must parse header values before constructing the
    /// response; this API does not parse HTTP-date or delta-seconds syntax.
    ///
    /// # Errors
    ///
    /// Returns validation for an invalid response or overflowing retry deadline,
    /// the usual delivery admission errors for held, leased, not-due or terminal
    /// work, and journal errors without changing in-memory state.
    pub fn record_response(
        &mut self,
        delivery_id: &str,
        attempted_at: u64,
        response: DeliveryResponse,
    ) -> Result<AttemptResult, HarborError> {
        self.record_attempt_internal(
            delivery_id,
            attempted_at,
            AttemptOutcome::Http(response.status_code),
            None,
            response.retry_after_seconds,
        )
    }

    /// Records an HTTP response only for the current owner of a live claim.
    ///
    /// Retry delays follow [`Self::record_response`]. A successful append clears
    /// the claim and records its worker in attempt history; rejected responses
    /// or persistence failures leave ownership and the retry schedule unchanged.
    ///
    /// # Errors
    ///
    /// Returns validation for malformed worker or response values, conflict for
    /// an expired or differently owned lease, and the same admission and journal
    /// errors as [`Self::record_response`].
    pub fn record_claimed_response(
        &mut self,
        delivery_id: &str,
        worker: &str,
        attempted_at: u64,
        response: DeliveryResponse,
    ) -> Result<AttemptResult, HarborError> {
        let worker = required(worker, "worker", 120)?;
        self.record_attempt_internal(
            delivery_id,
            attempted_at,
            AttemptOutcome::Http(response.status_code),
            Some(&worker),
            response.retry_after_seconds,
        )
    }
}
