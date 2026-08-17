use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::model::{Delivery, DeliveryState, HarborError, Route, WebhookEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    Route(Route),
    Event(WebhookEvent),
    Delivery(Delivery),
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
        ]
        .join("|"),
    }
}

fn decode_record(line: &str) -> Result<Record, HarborError> {
    let parts = line.split('|').collect::<Vec<_>>();
    match parts.first().copied() {
        Some("R") if parts.len() == 8 => Ok(Record::Route(Route {
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
        })),
        Some("E") if parts.len() == 7 => Ok(Record::Event(WebhookEvent {
            id: decode_text(parts[1])?,
            tenant: decode_text(parts[2])?,
            topic: decode_text(parts[3])?,
            dedupe_key: decode_text(parts[4])?,
            body: decode_bytes(parts[5])?,
            received_at: number(parts[6], "received_at")?,
        })),
        Some("D") if parts.len() == 9 => Ok(Record::Delivery(Delivery {
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
        })),
        Some(kind) => Err(HarborError::Journal(format!(
            "unknown record or field count: {kind}"
        ))),
        None => Err(HarborError::Journal("empty record".into())),
    }
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
