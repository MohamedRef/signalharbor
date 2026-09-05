use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadEncryptionReceipt {
    pub event_id: String,
    pub route_id: String,
    pub received_at_ms: u64,
    pub encrypted: bool,
    pub key_id: Option<String>,
    pub nonce_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EncryptionAttestationState {
    Blocked,
    Review,
    Attested,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionAttestation {
    pub event_id: String,
    pub route_id: String,
    pub state: EncryptionAttestationState,
    pub reasons: Vec<&'static str>,
    pub receipt_age_ms: u64,
}

fn label(value: &str, field: &str) -> Result<String, String> {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return Err(format!("payload encryption {field} is required"));
    }
    Ok(normalized)
}

/// Verifies encryption evidence before a retained webhook payload is routed.
///
/// # Errors
///
/// Returns an error for invalid controls, duplicate events, malformed receipts,
/// missing identities, or receipts dated after the review time.
pub fn attest_payload_encryption(
    receipts: &[PayloadEncryptionReceipt],
    review_at_ms: u64,
    maximum_receipt_age_ms: u64,
) -> Result<Vec<EncryptionAttestation>, String> {
    if review_at_ms == 0 || maximum_receipt_age_ms == 0 {
        return Err("payload encryption review controls must be positive".into());
    }
    let mut events = BTreeSet::new();
    let mut nonces = BTreeSet::new();
    let mut attestations = Vec::with_capacity(receipts.len());

    for receipt in receipts {
        let event_id = label(&receipt.event_id, "event")?;
        let route_id = label(&receipt.route_id, "route")?;
        if !events.insert(event_id.to_lowercase()) {
            return Err(format!("duplicate payload encryption event: {event_id}"));
        }
        if receipt.received_at_ms == 0 || receipt.received_at_ms > review_at_ms {
            return Err(format!(
                "invalid payload encryption receipt time for {event_id}"
            ));
        }
        let age = review_at_ms - receipt.received_at_ms;
        let mut reasons = Vec::new();
        if !receipt.encrypted {
            reasons.push("payload is not encrypted");
        }
        let key_id = receipt
            .key_id
            .as_deref()
            .map(|value| label(value, "key"))
            .transpose()?;
        if receipt.encrypted && key_id.is_none() {
            reasons.push("encryption key identity is missing");
        }
        let nonce_id = receipt
            .nonce_id
            .as_deref()
            .map(|value| label(value, "nonce"))
            .transpose()?;
        if receipt.encrypted && nonce_id.is_none() {
            reasons.push("encryption nonce is missing");
        }
        if let Some(nonce) = nonce_id
            && !nonces.insert(nonce.to_lowercase())
        {
            reasons.push("encryption nonce was reused");
        }

        let state = if reasons.iter().any(|reason| {
            matches!(
                *reason,
                "payload is not encrypted" | "encryption nonce was reused"
            )
        }) {
            EncryptionAttestationState::Blocked
        } else if !reasons.is_empty() || age > maximum_receipt_age_ms {
            if age > maximum_receipt_age_ms {
                reasons.push("encryption receipt is stale");
            }
            EncryptionAttestationState::Review
        } else {
            EncryptionAttestationState::Attested
        };
        attestations.push(EncryptionAttestation {
            event_id,
            route_id,
            state,
            reasons,
            receipt_age_ms: age,
        });
    }

    attestations.sort_by(|left, right| {
        left.state
            .cmp(&right.state)
            .then_with(|| right.receipt_age_ms.cmp(&left.receipt_age_ms))
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    Ok(attestations)
}
