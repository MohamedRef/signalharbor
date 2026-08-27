use crate::HarborError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryResponse {
    pub status_code: u16,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryDisposition {
    Delivered,
    Retry { delay_override_seconds: Option<u64> },
    Drop,
}

/// Classifies an HTTP delivery response without retrying permanent failures.
///
/// # Errors
///
/// Returns validation when the status or retry-after value is outside supported bounds.
pub fn classify_delivery_response(
    response: DeliveryResponse,
) -> Result<DeliveryDisposition, HarborError> {
    if !(100..=599).contains(&response.status_code) {
        return Err(HarborError::Validation(
            "delivery status must be from 100 through 599".into(),
        ));
    }
    if response
        .retry_after_seconds
        .is_some_and(|value| value > 86_400)
    {
        return Err(HarborError::Validation(
            "retry-after cannot exceed one day".into(),
        ));
    }
    if (200..=299).contains(&response.status_code) {
        return Ok(DeliveryDisposition::Delivered);
    }
    let retryable = matches!(response.status_code, 408 | 425 | 429 | 500..=599);
    if retryable {
        return Ok(DeliveryDisposition::Retry {
            delay_override_seconds: response.retry_after_seconds,
        });
    }
    if response.retry_after_seconds.is_some() {
        return Err(HarborError::Validation(
            "retry-after is only valid on retryable responses".into(),
        ));
    }
    Ok(DeliveryDisposition::Drop)
}
