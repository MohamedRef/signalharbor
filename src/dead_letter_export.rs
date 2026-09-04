use std::collections::BTreeSet;

use crate::HarborError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterPartition {
    pub partition_id: String,
    pub tenant_id: String,
    pub queued_events: u64,
    pub oldest_age_minutes: u64,
    pub legal_hold: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportCapacity {
    pub exporter_id: String,
    pub permitted_tenants: Vec<String>,
    pub event_capacity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterExportPlan {
    pub partition_id: String,
    pub exporter_id: Option<String>,
    pub exported_events: u64,
    pub remaining_events: u64,
    pub decision: String,
}

/// Allocates dead-letter exports oldest-first while preserving legal holds.
///
/// # Errors
///
/// Returns validation errors for invalid partitions, exporters, or tenant coverage.
pub fn plan_dead_letter_exports(
    partitions: &[DeadLetterPartition],
    capacities: &[ExportCapacity],
) -> Result<Vec<DeadLetterExportPlan>, HarborError> {
    let mut remaining = Vec::with_capacity(capacities.len());
    let mut coverage = Vec::with_capacity(capacities.len());
    let mut exporters = BTreeSet::new();
    for capacity in capacities {
        let id = required(&capacity.exporter_id, "dead-letter exporter")?;
        if !exporters.insert(id.to_ascii_lowercase()) || capacity.event_capacity == 0 {
            return Err(HarborError::Validation(
                "invalid or duplicate dead-letter exporter".into(),
            ));
        }
        let tenants = capacity
            .permitted_tenants
            .iter()
            .map(|tenant| required(tenant, "export tenant").map(|value| value.to_ascii_lowercase()))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if tenants.is_empty() {
            return Err(HarborError::Validation(format!(
                "exporter {id} needs tenant coverage"
            )));
        }
        remaining.push(capacity.event_capacity);
        coverage.push(tenants);
    }
    let mut ordered = partitions.to_vec();
    ordered.sort_by(|a, b| {
        b.oldest_age_minutes
            .cmp(&a.oldest_age_minutes)
            .then_with(|| a.partition_id.cmp(&b.partition_id))
    });
    let mut seen = BTreeSet::new();
    let mut plans = Vec::with_capacity(ordered.len());
    for partition in ordered {
        let id = required(&partition.partition_id, "dead-letter partition")?;
        let tenant = required(&partition.tenant_id, "dead-letter tenant")?;
        if !seen.insert(id.to_ascii_lowercase()) || partition.queued_events == 0 {
            return Err(HarborError::Validation(
                "invalid or duplicate dead-letter partition".into(),
            ));
        }
        if partition.legal_hold {
            plans.push(DeadLetterExportPlan {
                partition_id: id,
                exporter_id: None,
                exported_events: 0,
                remaining_events: partition.queued_events,
                decision: "legal_hold".into(),
            });
            continue;
        }
        let choice = coverage
            .iter()
            .enumerate()
            .filter(|(index, tenants)| {
                remaining[*index] > 0 && tenants.contains(&tenant.to_ascii_lowercase())
            })
            .max_by_key(|(index, _)| remaining[*index])
            .map(|(index, _)| index);
        let Some(index) = choice else {
            plans.push(DeadLetterExportPlan {
                partition_id: id,
                exporter_id: None,
                exported_events: 0,
                remaining_events: partition.queued_events,
                decision: "capacity_required".into(),
            });
            continue;
        };
        let exported = remaining[index].min(partition.queued_events);
        remaining[index] -= exported;
        plans.push(DeadLetterExportPlan {
            partition_id: id,
            exporter_id: Some(capacities[index].exporter_id.trim().into()),
            exported_events: exported,
            remaining_events: partition.queued_events - exported,
            decision: if exported == partition.queued_events {
                "exported".into()
            } else {
                "partial".into()
            },
        });
    }
    Ok(plans)
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarborError::Validation(format!("{field} is required")))
    } else {
        Ok(value.into())
    }
}
