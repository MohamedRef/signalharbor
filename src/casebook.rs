use crate::HarborError;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaseStatus {
    Open,
    Investigating,
    Decided,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaseDisposition {
    Redrive,
    Quarantine,
    Discard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterCase {
    pub id: String,
    pub delivery_id: String,
    pub reason: String,
    pub opened_by: String,
    pub owner: Option<String>,
    pub status: CaseStatus,
    pub disposition: Option<CaseDisposition>,
    pub decision_note: Option<String>,
    pub decided_by: Option<String>,
    pub closure_evidence: Option<String>,
    pub version: u64,
}

pub struct DeadLetterCasebook {
    path: PathBuf,
    cases: BTreeMap<String, DeadLetterCase>,
    sequence: u64,
}

impl DeadLetterCasebook {
    /// Replays the append-only case journal, keeping the last durable version of each case.
    ///
    /// # Errors
    /// Returns a journal error when records cannot be read or fail integrity validation.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HarborError> {
        let path = path.as_ref().to_path_buf();
        let mut cases: BTreeMap<String, DeadLetterCase> = BTreeMap::new();
        let mut sequence = 0_u64;
        match fs::File::open(&path) {
            Ok(file) => {
                for (line_number, line) in BufReader::new(file).lines().enumerate() {
                    let line = line.map_err(|error| HarborError::Journal(error.to_string()))?;
                    let item = decode_case(&line).map_err(|message| {
                        HarborError::Journal(format!(
                            "case journal line {}: {message}",
                            line_number + 1
                        ))
                    })?;
                    sequence = sequence.max(case_sequence(&item.id)?);
                    if let Some(previous) = cases.get(&item.id) {
                        if item.version != previous.version + 1 {
                            return Err(HarborError::Journal(format!(
                                "case {} has non-contiguous version {}",
                                item.id, item.version
                            )));
                        }
                    } else if item.version != 1 {
                        return Err(HarborError::Journal(format!(
                            "case {} starts at version {}",
                            item.id, item.version
                        )));
                    }
                    cases.insert(item.id.clone(), item);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(HarborError::Journal(error.to_string())),
        }
        Ok(Self {
            path,
            cases,
            sequence,
        })
    }

    /// Opens one active investigation for a dead-letter delivery.
    ///
    /// # Errors
    /// Returns validation, conflict, or journal errors without exposing an unpersisted case.
    pub fn open_case(
        &mut self,
        delivery_id: &str,
        reason: &str,
        opened_by: &str,
    ) -> Result<DeadLetterCase, HarborError> {
        let delivery_id = required(delivery_id, "delivery id")?;
        let reason = required(reason, "case reason")?;
        let opened_by = required(opened_by, "case opener")?;
        if self
            .cases
            .values()
            .any(|item| item.delivery_id == delivery_id && item.status != CaseStatus::Closed)
        {
            return Err(HarborError::Conflict(format!(
                "delivery {delivery_id} already has an active dead-letter case"
            )));
        }
        self.sequence += 1;
        let item = DeadLetterCase {
            id: format!("dlc_{:08}", self.sequence),
            delivery_id,
            reason,
            opened_by,
            owner: None,
            status: CaseStatus::Open,
            disposition: None,
            decision_note: None,
            decided_by: None,
            closure_evidence: None,
            version: 1,
        };
        self.append(&item)?;
        self.cases.insert(item.id.clone(), item.clone());
        Ok(item)
    }

    /// Assigns an active case to an investigator.
    ///
    /// # Errors
    /// Returns not-found, version, lifecycle, validation, or journal errors.
    pub fn assign(
        &mut self,
        id: &str,
        owner: &str,
        expected_version: u64,
    ) -> Result<DeadLetterCase, HarborError> {
        let mut item = self.current(id, expected_version)?;
        if item.status != CaseStatus::Open && item.status != CaseStatus::Investigating {
            return Err(HarborError::Conflict(
                "only active cases can be assigned".into(),
            ));
        }
        item.owner = Some(required(owner, "case owner")?);
        item.status = CaseStatus::Investigating;
        item.version += 1;
        self.replace(item)
    }

    /// Records an independently approved disposition.
    ///
    /// # Errors
    /// Returns lifecycle, separation-of-duty, version, validation, or journal errors.
    pub fn decide(
        &mut self,
        id: &str,
        disposition: CaseDisposition,
        note: &str,
        actor: &str,
        expected_version: u64,
    ) -> Result<DeadLetterCase, HarborError> {
        let mut item = self.current(id, expected_version)?;
        if item.status != CaseStatus::Investigating {
            return Err(HarborError::Conflict(
                "case must be assigned before decision".into(),
            ));
        }
        let actor = required(actor, "decision actor")?;
        if item
            .owner
            .as_deref()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(&actor))
        {
            return Err(HarborError::Conflict(
                "case owner cannot approve their own disposition".into(),
            ));
        }
        item.disposition = Some(disposition);
        item.decision_note = Some(required(note, "decision note")?);
        item.decided_by = Some(actor);
        item.status = CaseStatus::Decided;
        item.version += 1;
        self.replace(item)
    }

    /// Closes a decided case with execution evidence.
    ///
    /// # Errors
    /// Returns lifecycle, version, validation, or journal errors.
    pub fn close(
        &mut self,
        id: &str,
        evidence: &str,
        expected_version: u64,
    ) -> Result<DeadLetterCase, HarborError> {
        let mut item = self.current(id, expected_version)?;
        if item.status != CaseStatus::Decided {
            return Err(HarborError::Conflict(
                "case needs an approved disposition before closure".into(),
            ));
        }
        item.closure_evidence = Some(required(evidence, "closure evidence")?);
        item.status = CaseStatus::Closed;
        item.version += 1;
        self.replace(item)
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<DeadLetterCase> {
        self.cases.get(id).cloned()
    }

    #[must_use]
    pub fn active_queue(&self) -> Vec<DeadLetterCase> {
        self.cases
            .values()
            .filter(|item| item.status != CaseStatus::Closed)
            .cloned()
            .collect()
    }

    fn current(&self, id: &str, expected_version: u64) -> Result<DeadLetterCase, HarborError> {
        let item = self
            .cases
            .get(id)
            .cloned()
            .ok_or_else(|| HarborError::NotFound(format!("dead-letter case {id}")))?;
        if item.version != expected_version {
            return Err(HarborError::Conflict(format!(
                "case version is {}, expected {expected_version}",
                item.version
            )));
        }
        Ok(item)
    }

    fn replace(&mut self, item: DeadLetterCase) -> Result<DeadLetterCase, HarborError> {
        self.append(&item)?;
        self.cases.insert(item.id.clone(), item.clone());
        Ok(item)
    }

    fn append(&self, item: &DeadLetterCase) -> Result<(), HarborError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| HarborError::Journal(error.to_string()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| HarborError::Journal(error.to_string()))?;
        writeln!(file, "{}", encode_case(item))
            .map_err(|error| HarborError::Journal(error.to_string()))?;
        file.sync_data()
            .map_err(|error| HarborError::Journal(error.to_string()))
    }
}

fn required(value: &str, field: &str) -> Result<String, HarborError> {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.is_empty() {
        return Err(HarborError::Validation(format!("{field} is required")));
    }
    Ok(value)
}

fn encode_case(item: &DeadLetterCase) -> String {
    [
        item.id.clone(),
        hex(&item.delivery_id),
        hex(&item.reason),
        hex(&item.opened_by),
        item.owner.as_deref().map(hex).unwrap_or_default(),
        status(item.status).into(),
        item.disposition.map_or("-", disposition).into(),
        item.decision_note.as_deref().map(hex).unwrap_or_default(),
        item.decided_by.as_deref().map(hex).unwrap_or_default(),
        item.closure_evidence
            .as_deref()
            .map(hex)
            .unwrap_or_default(),
        item.version.to_string(),
    ]
    .join("|")
}

fn decode_case(line: &str) -> Result<DeadLetterCase, String> {
    let parts = line.split('|').collect::<Vec<_>>();
    if parts.len() != 11 {
        return Err("expected 11 fields".into());
    }
    Ok(DeadLetterCase {
        id: parts[0].into(),
        delivery_id: unhex(parts[1])?,
        reason: unhex(parts[2])?,
        opened_by: unhex(parts[3])?,
        owner: optional_unhex(parts[4])?,
        status: parse_status(parts[5])?,
        disposition: parse_disposition(parts[6])?,
        decision_note: optional_unhex(parts[7])?,
        decided_by: optional_unhex(parts[8])?,
        closure_evidence: optional_unhex(parts[9])?,
        version: parts[10].parse().map_err(|_| "invalid version")?,
    })
}

fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .fold(String::new(), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
            output
        })
}
fn unhex(value: &str) -> Result<String, String> {
    if !value.len().is_multiple_of(2) {
        return Err("invalid text encoding".into());
    }
    let bytes = (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| "invalid text encoding".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    String::from_utf8(bytes).map_err(|_| "invalid utf-8".into())
}
fn optional_unhex(value: &str) -> Result<Option<String>, String> {
    if value.is_empty() {
        Ok(None)
    } else {
        unhex(value).map(Some)
    }
}
fn status(value: CaseStatus) -> &'static str {
    match value {
        CaseStatus::Open => "open",
        CaseStatus::Investigating => "investigating",
        CaseStatus::Decided => "decided",
        CaseStatus::Closed => "closed",
    }
}
fn disposition(value: CaseDisposition) -> &'static str {
    match value {
        CaseDisposition::Redrive => "redrive",
        CaseDisposition::Quarantine => "quarantine",
        CaseDisposition::Discard => "discard",
    }
}
fn parse_status(value: &str) -> Result<CaseStatus, String> {
    match value {
        "open" => Ok(CaseStatus::Open),
        "investigating" => Ok(CaseStatus::Investigating),
        "decided" => Ok(CaseStatus::Decided),
        "closed" => Ok(CaseStatus::Closed),
        _ => Err("invalid status".into()),
    }
}
fn parse_disposition(value: &str) -> Result<Option<CaseDisposition>, String> {
    match value {
        "-" => Ok(None),
        "redrive" => Ok(Some(CaseDisposition::Redrive)),
        "quarantine" => Ok(Some(CaseDisposition::Quarantine)),
        "discard" => Ok(Some(CaseDisposition::Discard)),
        _ => Err("invalid disposition".into()),
    }
}
fn case_sequence(id: &str) -> Result<u64, HarborError> {
    id.strip_prefix("dlc_")
        .ok_or_else(|| HarborError::Journal("invalid case id".into()))?
        .parse()
        .map_err(|_| HarborError::Journal("invalid case sequence".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "signalharbor-{label}-{}-{}.journal",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    #[test]
    fn disposition_lifecycle_survives_replay() {
        let path = path("case");
        let mut cases = DeadLetterCasebook::open(&path).expect("open");
        let opened = cases
            .open_case("delivery-7", "signature rejected", "operator")
            .expect("case");
        let assigned = cases.assign(&opened.id, "investigator", 1).expect("assign");
        let decided = cases
            .decide(
                &opened.id,
                CaseDisposition::Redrive,
                "secret rotated",
                "lead",
                assigned.version,
            )
            .expect("decide");
        let closed = cases
            .close(&opened.id, "delivery attempt 12 succeeded", decided.version)
            .expect("close");
        assert_eq!(closed.status, CaseStatus::Closed);
        let replayed = DeadLetterCasebook::open(&path).expect("replay");
        assert_eq!(replayed.get(&opened.id), Some(closed));
        assert!(replayed.active_queue().is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn prevents_duplicate_active_cases_and_self_approval() {
        let path = path("guard");
        let mut cases = DeadLetterCasebook::open(&path).expect("open");
        let opened = cases
            .open_case("delivery-9", "timeout", "operator")
            .expect("case");
        assert!(matches!(
            cases.open_case("delivery-9", "again", "operator"),
            Err(HarborError::Conflict(_))
        ));
        let assigned = cases.assign(&opened.id, "Alice", 1).expect("assign");
        assert!(matches!(
            cases.decide(
                &opened.id,
                CaseDisposition::Discard,
                "bad",
                "alice",
                assigned.version
            ),
            Err(HarborError::Conflict(_))
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn guards_versions_states_and_required_evidence() {
        let path = path("version");
        let mut cases = DeadLetterCasebook::open(&path).expect("open");
        let opened = cases
            .open_case("delivery-1", "exhausted", "operator")
            .expect("case");
        assert!(matches!(
            cases.assign(&opened.id, "owner", 8),
            Err(HarborError::Conflict(_))
        ));
        assert!(matches!(
            cases.close(&opened.id, "proof", 1),
            Err(HarborError::Conflict(_))
        ));
        let assigned = cases.assign(&opened.id, "owner", 1).expect("assign");
        assert!(matches!(
            cases.decide(
                &opened.id,
                CaseDisposition::Quarantine,
                "",
                "lead",
                assigned.version
            ),
            Err(HarborError::Validation(_))
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn queue_is_ordered_and_closed_delivery_can_open_a_new_case() {
        let path = path("queue");
        let mut cases = DeadLetterCasebook::open(&path).expect("open");
        let first = cases.open_case("delivery-b", "one", "op").expect("case");
        let second = cases.open_case("delivery-a", "two", "op").expect("case");
        assert_eq!(
            cases
                .active_queue()
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec![first.id.as_str(), second.id.as_str()]
        );
        let assigned = cases.assign(&first.id, "owner", 1).expect("assign");
        let decided = cases
            .decide(
                &first.id,
                CaseDisposition::Discard,
                "test event",
                "lead",
                assigned.version,
            )
            .expect("decide");
        cases
            .close(&first.id, "payload deleted", decided.version)
            .expect("close");
        assert!(cases.open_case("delivery-b", "new failure", "op").is_ok());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_corrupt_or_noncontiguous_journal() {
        let path = path("corrupt");
        fs::write(&path, "not-a-record\n").expect("write");
        assert!(matches!(
            DeadLetterCasebook::open(&path),
            Err(HarborError::Journal(_))
        ));
        let _ = fs::remove_file(path);
    }
}
