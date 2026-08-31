use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointCertificate {
    pub endpoint_id: String,
    pub fingerprint: String,
    pub expires_at_epoch: u64,
    pub renewal_lead_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateRenewal {
    pub endpoint_id: String,
    pub fingerprint: String,
    pub renew_by_epoch: u64,
    pub expired: bool,
}

/// Plans certificate renewals that are due at the supplied observation time.
///
/// # Errors
///
/// Returns validation for blank identities, zero expiry, zero lead time, or duplicate endpoints.
pub fn plan_certificate_renewals(
    rows: &[EndpointCertificate],
    as_of_epoch: u64,
) -> Result<Vec<CertificateRenewal>, HarborError> {
    if as_of_epoch == 0 {
        return Err(HarborError::Validation(
            "certificate assessment time is required".into(),
        ));
    }
    let mut endpoints = std::collections::BTreeSet::new();
    let mut renewals = Vec::new();
    for row in rows {
        let endpoint_id = row.endpoint_id.trim();
        let fingerprint = row.fingerprint.trim();
        if endpoint_id.is_empty()
            || fingerprint.is_empty()
            || row.expires_at_epoch == 0
            || row.renewal_lead_seconds == 0
        {
            return Err(HarborError::Validation(
                "endpoint, fingerprint, expiry, and renewal lead are required".into(),
            ));
        }
        if !endpoints.insert(endpoint_id.to_owned()) {
            return Err(HarborError::Validation(format!(
                "duplicate certificate endpoint {endpoint_id}"
            )));
        }
        let renew_by_epoch = row
            .expires_at_epoch
            .saturating_sub(row.renewal_lead_seconds);
        if renew_by_epoch <= as_of_epoch {
            renewals.push(CertificateRenewal {
                endpoint_id: endpoint_id.to_owned(),
                fingerprint: fingerprint.to_owned(),
                renew_by_epoch,
                expired: row.expires_at_epoch < as_of_epoch,
            });
        }
    }
    renewals.sort_by(|left, right| {
        left.renew_by_epoch
            .cmp(&right.renew_by_epoch)
            .then_with(|| left.endpoint_id.cmp(&right.endpoint_id))
    });
    Ok(renewals)
}
