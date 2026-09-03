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

/// Parses the delta-seconds form of an HTTP `Retry-After` field.
///
/// Optional whitespace is accepted, while HTTP-date values, signs, fractions,
/// and delays beyond the one-day delivery-policy limit are rejected.
///
/// # Errors
///
/// Returns validation when the field is empty, is not unsigned decimal
/// delta-seconds, overflows, or exceeds 86,400 seconds.
pub fn parse_retry_after_delta(value: &str) -> Result<u64, HarborError> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HarborError::Validation(
            "retry-after must be unsigned delta-seconds".into(),
        ));
    }
    let seconds = value.parse::<u64>().map_err(|_| {
        HarborError::Validation("retry-after delta-seconds exceeds numeric range".into())
    })?;
    if seconds > 86_400 {
        return Err(HarborError::Validation(
            "retry-after cannot exceed one day".into(),
        ));
    }
    Ok(seconds)
}

impl DeliveryResponse {
    /// Builds a validated response from an optional raw `Retry-After` field.
    ///
    /// This accepts the portable delta-seconds form and rejects HTTP-date
    /// values so callers cannot accidentally apply a delay using an unknown
    /// clock. Status and retryability rules are validated before returning.
    ///
    /// # Errors
    ///
    /// Returns validation for an invalid HTTP status, malformed or excessive
    /// delta, or a retry field attached to a non-retryable response.
    pub fn from_retry_after(
        status_code: u16,
        retry_after: Option<&str>,
    ) -> Result<Self, HarborError> {
        let response = Self {
            status_code,
            retry_after_seconds: retry_after.map(parse_retry_after_delta).transpose()?,
        };
        classify_delivery_response(response)?;
        Ok(response)
    }
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
    let retryable = matches!(response.status_code, 408 | 425 | 429 | 500..=599);
    if response.retry_after_seconds.is_some() && !retryable {
        return Err(HarborError::Validation(
            "retry-after is only valid on retryable responses".into(),
        ));
    }
    if (200..=299).contains(&response.status_code) {
        return Ok(DeliveryDisposition::Delivered);
    }
    if retryable {
        return Ok(DeliveryDisposition::Retry {
            delay_override_seconds: response.retry_after_seconds,
        });
    }
    Ok(DeliveryDisposition::Drop)
}
