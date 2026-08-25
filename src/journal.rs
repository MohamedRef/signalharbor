use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::model::{
    AttemptOutcome, Delivery, DeliveryAttempt, DeliveryCancellation, DeliveryHold, DeliveryState,
    HarborError, Route, RouteMaintenance, TenantIngressFreeze, WebhookEvent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    Route(Route),
    Event(WebhookEvent),
    Delivery(Delivery),
    Attempt(DeliveryAttempt),
    Cancellation(DeliveryCancellation),
    Hold(DeliveryHold),
    Maintenance(RouteMaintenance),
    IngressFreeze(TenantIngressFreeze),
}

#[derive(Debug, Clone)]
pub(crate) struct FileJournal {
    path: PathBuf,
}

impl FileJournal {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub(crate) fn replay(&self) -> Result<Vec<Record>, HarborError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path).map_err(|error| {
            HarborError::Journal(format!("cannot open {}: {error}", self.path.display()))
        })?;
        let mut records = Vec::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|error| {
                HarborError::Journal(format!("cannot read line {}: {error}", index + 1))
            })?;
            if line.is_empty() {
                continue;
            }
            records.extend(decode_records(&line).map_err(|error| {
                HarborError::Journal(format!("invalid line {}: {error}", index + 1))
            })?);
        }
        Ok(records)
    }

    pub(crate) fn append(&self, record: &Record) -> Result<(), HarborError> {
        self.append_batch(std::slice::from_ref(record))
    }

    pub(crate) fn append_batch(&self, records: &[Record]) -> Result<(), HarborError> {
        if records.is_empty() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                HarborError::Journal(format!("cannot create {}: {error}", parent.display()))
            })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| {
                HarborError::Journal(format!("cannot append {}: {error}", self.path.display()))
            })?;
        let encoded = if records.len() == 1 {
            encode_record(&records[0])
        } else {
            format!(
                "T|{}",
                records
                    .iter()
                    .map(encode_record)
                    .collect::<Vec<_>>()
                    .join(";")
            )
        };
        writeln!(file, "{encoded}").map_err(|error| {
            HarborError::Journal(format!("cannot write {}: {error}", self.path.display()))
        })?;
        file.flush().map_err(|error| {
            HarborError::Journal(format!("cannot flush {}: {error}", self.path.display()))
        })?;
        file.sync_data().map_err(|error| {
            HarborError::Journal(format!("cannot sync {}: {error}", self.path.display()))
        })
    }
}

fn decode_records(line: &str) -> Result<Vec<Record>, HarborError> {
    if let Some(batch) = line.strip_prefix("T|") {
        if batch.is_empty() {
            return Err(HarborError::Journal("empty transaction".into()));
        }
        batch.split(';').map(decode_record).collect()
    } else {
        Ok(vec![decode_record(line)?])
    }
}

fn encode_record(record: &Record) -> String {
    match record {
        Record::Route(route) => [
            "R".to_owned(),
            encode_text(&route.id),
            encode_text(&route.tenant),
            encode_text(&route.topic_pattern),
            encode_text(&route.endpoint),
            route.max_attempts.to_string(),
            route.base_delay_seconds.to_string(),
            u8::from(route.active).to_string(),
            route.failure_threshold.to_string(),
            route.cooldown_seconds.to_string(),
            route.consecutive_failures.to_string(),
            route
                .circuit_open_until
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            route.max_in_flight.to_string(),
        ]
        .join("|"),
        Record::Event(event) => [
            "E".to_owned(),
            encode_text(&event.id),
            encode_text(&event.tenant),
            encode_text(&event.topic),
            encode_text(&event.dedupe_key),
            encode_bytes(&event.body),
            event.received_at.to_string(),
        ]
        .join("|"),
        Record::Delivery(delivery) => [
            "D".to_owned(),
            encode_text(&delivery.id),
            encode_text(&delivery.event_id),
            encode_text(&delivery.route_id),
            encode_text(&delivery.endpoint),
            delivery.state.code().to_owned(),
            delivery.attempts.to_string(),
            delivery.next_attempt_at.to_string(),
            delivery
                .last_error
                .as_deref()
                .map_or_else(|| "-".to_owned(), encode_text),
            delivery
                .lease_owner
                .as_deref()
                .map_or_else(|| "-".to_owned(), encode_text),
            delivery
                .lease_expires_at
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        ]
        .join("|"),
        Record::Attempt(attempt) => {
            let (outcome_kind, outcome_value) = match &attempt.outcome {
                AttemptOutcome::Http(status) => ("http", status.to_string()),
                AttemptOutcome::Transport(message) => ("transport", encode_text(message)),
            };
            [
                "A".to_owned(),
                encode_text(&attempt.id),
                attempt.sequence.to_string(),
                encode_text(&attempt.delivery_id),
                attempt.cycle_attempt.to_string(),
                attempt.attempted_at.to_string(),
                attempt
                    .worker
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), encode_text),
                outcome_kind.to_owned(),
                outcome_value,
                attempt.resulting_state.code().to_owned(),
                u8::from(attempt.retried).to_string(),
                attempt
                    .next_attempt_at
                    .map_or_else(|| "-".to_owned(), |value| value.to_string()),
                attempt
                    .error
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), encode_text),
            ]
            .join("|")
        }
        Record::Cancellation(cancellation) => [
            "C".to_owned(),
            encode_text(&cancellation.id),
            encode_text(&cancellation.delivery_id),
            encode_text(&cancellation.actor),
            encode_text(&cancellation.reason),
            cancellation.cancelled_at.to_string(),
        ]
        .join("|"),
        Record::Hold(hold) => encode_hold(hold),
        Record::Maintenance(maintenance) => encode_maintenance(maintenance),
        Record::IngressFreeze(freeze) => encode_ingress_freeze(freeze),
    }
}

fn decode_record(line: &str) -> Result<Record, HarborError> {
    let parts = line.split('|').collect::<Vec<_>>();
    match parts.first().copied() {
        Some("R") if parts.len() == 8 || parts.len() == 12 || parts.len() == 13 => {
            decode_route(&parts).map(Record::Route)
        }
        Some("E") if parts.len() == 7 => Ok(Record::Event(WebhookEvent {
            id: decode_text(parts[1])?,
            tenant: decode_text(parts[2])?,
            topic: decode_text(parts[3])?,
            dedupe_key: decode_text(parts[4])?,
            body: decode_bytes(parts[5])?,
            received_at: number(parts[6], "received_at")?,
        })),
        Some("D") if parts.len() == 9 || parts.len() == 11 => Ok(Record::Delivery(Delivery {
            id: decode_text(parts[1])?,
            event_id: decode_text(parts[2])?,
            route_id: decode_text(parts[3])?,
            endpoint: decode_text(parts[4])?,
            state: DeliveryState::from_code(parts[5])?,
            attempts: number(parts[6], "attempts")?,
            next_attempt_at: number(parts[7], "next_attempt_at")?,
            last_error: if parts[8] == "-" {
                None
            } else {
                Some(decode_text(parts[8])?)
            },
            lease_owner: if parts.len() == 9 || parts[9] == "-" {
                None
            } else {
                Some(decode_text(parts[9])?)
            },
            lease_expires_at: if parts.len() == 9 || parts[10] == "-" {
                None
            } else {
                Some(number(parts[10], "lease expiry")?)
            },
        })),
        Some("A") if parts.len() == 13 => Ok(Record::Attempt(DeliveryAttempt {
            id: decode_text(parts[1])?,
            sequence: number(parts[2], "attempt sequence")?,
            delivery_id: decode_text(parts[3])?,
            cycle_attempt: number(parts[4], "cycle attempt")?,
            attempted_at: number(parts[5], "attempted_at")?,
            worker: if parts[6] == "-" {
                None
            } else {
                Some(decode_text(parts[6])?)
            },
            outcome: match parts[7] {
                "http" => AttemptOutcome::Http(number(parts[8], "HTTP status")?),
                "transport" => AttemptOutcome::Transport(decode_text(parts[8])?),
                value => {
                    return Err(HarborError::Journal(format!(
                        "unknown attempt outcome: {value}"
                    )));
                }
            },
            resulting_state: DeliveryState::from_code(parts[9])?,
            retried: match parts[10] {
                "1" => true,
                "0" => false,
                value => {
                    return Err(HarborError::Journal(format!(
                        "invalid attempt retried flag: {value}"
                    )));
                }
            },
            next_attempt_at: if parts[11] == "-" {
                None
            } else {
                Some(number(parts[11], "attempt next due")?)
            },
            error: if parts[12] == "-" {
                None
            } else {
                Some(decode_text(parts[12])?)
            },
        })),
        Some("C") if parts.len() == 6 => Ok(Record::Cancellation(DeliveryCancellation {
            id: decode_text(parts[1])?,
            delivery_id: decode_text(parts[2])?,
            actor: decode_text(parts[3])?,
            reason: decode_text(parts[4])?,
            cancelled_at: number(parts[5], "cancelled_at")?,
        })),
        Some("H") if parts.len() == 10 => decode_hold(&parts).map(Record::Hold),
        Some("M") if parts.len() == 12 => decode_maintenance(&parts).map(Record::Maintenance),
        Some("F") if parts.len() == 12 => decode_ingress_freeze(&parts).map(Record::IngressFreeze),
        Some(kind) => Err(HarborError::Journal(format!(
            "unknown record or field count: {kind}"
        ))),
        None => Err(HarborError::Journal("empty record".into())),
    }
}

fn encode_ingress_freeze(freeze: &TenantIngressFreeze) -> String {
    [
        "F".to_owned(),
        encode_text(&freeze.id),
        freeze.sequence.to_string(),
        encode_text(&freeze.tenant),
        freeze.starts_at.to_string(),
        freeze.expires_at.to_string(),
        encode_text(&freeze.imposed_by),
        encode_text(&freeze.reason),
        freeze.imposed_at.to_string(),
        freeze
            .lifted_at
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        freeze
            .lifted_by
            .as_deref()
            .map_or_else(|| "-".to_owned(), encode_text),
        freeze
            .lift_reason
            .as_deref()
            .map_or_else(|| "-".to_owned(), encode_text),
    ]
    .join("|")
}

fn decode_ingress_freeze(parts: &[&str]) -> Result<TenantIngressFreeze, HarborError> {
    Ok(TenantIngressFreeze {
        id: decode_text(parts[1])?,
        sequence: number(parts[2], "ingress freeze sequence")?,
        tenant: decode_text(parts[3])?,
        starts_at: number(parts[4], "ingress freeze start")?,
        expires_at: number(parts[5], "ingress freeze expiry")?,
        imposed_by: decode_text(parts[6])?,
        reason: decode_text(parts[7])?,
        imposed_at: number(parts[8], "ingress freeze imposed_at")?,
        lifted_at: if parts[9] == "-" {
            None
        } else {
            Some(number(parts[9], "ingress freeze lifted_at")?)
        },
        lifted_by: if parts[10] == "-" {
            None
        } else {
            Some(decode_text(parts[10])?)
        },
        lift_reason: if parts[11] == "-" {
            None
        } else {
            Some(decode_text(parts[11])?)
        },
    })
}

fn encode_hold(hold: &DeliveryHold) -> String {
    [
        "H".to_owned(),
        encode_text(&hold.id),
        hold.sequence.to_string(),
        encode_text(&hold.delivery_id),
        encode_text(&hold.actor),
        encode_text(&hold.reason),
        hold.held_at.to_string(),
        hold.release_at.to_string(),
        hold.released_at
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        hold.released_by
            .as_deref()
            .map_or_else(|| "-".to_owned(), encode_text),
    ]
    .join("|")
}

fn decode_hold(parts: &[&str]) -> Result<DeliveryHold, HarborError> {
    Ok(DeliveryHold {
        id: decode_text(parts[1])?,
        sequence: number(parts[2], "hold sequence")?,
        delivery_id: decode_text(parts[3])?,
        actor: decode_text(parts[4])?,
        reason: decode_text(parts[5])?,
        held_at: number(parts[6], "held_at")?,
        release_at: number(parts[7], "hold release_at")?,
        released_at: if parts[8] == "-" {
            None
        } else {
            Some(number(parts[8], "hold released_at")?)
        },
        released_by: if parts[9] == "-" {
            None
        } else {
            Some(decode_text(parts[9])?)
        },
    })
}

fn encode_maintenance(maintenance: &RouteMaintenance) -> String {
    [
        "M".to_owned(),
        encode_text(&maintenance.id),
        maintenance.sequence.to_string(),
        encode_text(&maintenance.route_id),
        maintenance.starts_at.to_string(),
        maintenance.ends_at.to_string(),
        encode_text(&maintenance.scheduled_by),
        encode_text(&maintenance.reason),
        maintenance.scheduled_at.to_string(),
        maintenance
            .cancelled_at
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        maintenance
            .cancelled_by
            .as_deref()
            .map_or_else(|| "-".to_owned(), encode_text),
        maintenance
            .cancellation_reason
            .as_deref()
            .map_or_else(|| "-".to_owned(), encode_text),
    ]
    .join("|")
}

fn decode_maintenance(parts: &[&str]) -> Result<RouteMaintenance, HarborError> {
    Ok(RouteMaintenance {
        id: decode_text(parts[1])?,
        sequence: number(parts[2], "maintenance sequence")?,
        route_id: decode_text(parts[3])?,
        starts_at: number(parts[4], "maintenance start")?,
        ends_at: number(parts[5], "maintenance end")?,
        scheduled_by: decode_text(parts[6])?,
        reason: decode_text(parts[7])?,
        scheduled_at: number(parts[8], "maintenance scheduled_at")?,
        cancelled_at: if parts[9] == "-" {
            None
        } else {
            Some(number(parts[9], "maintenance cancelled_at")?)
        },
        cancelled_by: if parts[10] == "-" {
            None
        } else {
            Some(decode_text(parts[10])?)
        },
        cancellation_reason: if parts[11] == "-" {
            None
        } else {
            Some(decode_text(parts[11])?)
        },
    })
}

fn decode_route(parts: &[&str]) -> Result<Route, HarborError> {
    Ok(Route {
        id: decode_text(parts[1])?,
        tenant: decode_text(parts[2])?,
        topic_pattern: decode_text(parts[3])?,
        endpoint: decode_text(parts[4])?,
        max_attempts: number(parts[5], "max attempts")?,
        base_delay_seconds: number(parts[6], "base delay")?,
        active: match parts[7] {
            "1" => true,
            "0" => false,
            value => {
                return Err(HarborError::Journal(format!(
                    "invalid route active flag: {value}"
                )));
            }
        },
        failure_threshold: if parts.len() == 8 {
            3
        } else {
            number(parts[8], "failure threshold")?
        },
        cooldown_seconds: if parts.len() == 8 {
            60
        } else {
            number(parts[9], "cooldown seconds")?
        },
        consecutive_failures: if parts.len() == 8 {
            0
        } else {
            number(parts[10], "consecutive failures")?
        },
        circuit_open_until: if parts.len() == 8 || parts[11] == "-" {
            None
        } else {
            Some(number(parts[11], "circuit open until")?)
        },
        max_in_flight: if parts.len() < 13 {
            1_000
        } else {
            number(parts[12], "max in flight")?
        },
    })
}

fn number<T>(value: &str, name: &str) -> Result<T, HarborError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| HarborError::Journal(format!("invalid {name}: {value}")))
}

fn encode_text(value: &str) -> String {
    encode_bytes(value.as_bytes())
}

fn decode_text(value: &str) -> Result<String, HarborError> {
    String::from_utf8(decode_bytes(value)?)
        .map_err(|error| HarborError::Journal(format!("invalid UTF-8: {error}")))
}

fn encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, HarborError> {
    if !value.len().is_multiple_of(2) {
        return Err(HarborError::Journal("hex value has odd length".into()));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8, HarborError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(HarborError::Journal(format!(
            "invalid hex digit: {}",
            char::from(value)
        ))),
    }
}
