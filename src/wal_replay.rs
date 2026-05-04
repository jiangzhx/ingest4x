use crate::ingest::processor::{ProcessorOutput, ProcessorRequestContext, ProcessorState};
use crate::projects::ProjectRegistryState;
use crate::rules::RuleRepository;
use crate::utils::events::{EventSinkState, EventStatus};
use crate::wal::{
    read_entries_after_limit, remove_segments_covered_by_checkpoint, WalPosition, WalRecord,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tracing::warn;

const CHECKPOINT_FILE: &str = "checkpoint.json";
const REPLAY_BATCH_SIZE: usize = 1024;

pub struct WalReplayContext<'a> {
    pub dir: &'a Path,
    pub event_sinks: &'a EventSinkState,
    pub project_registry: &'a ProjectRegistryState,
    pub rule_repository: &'a RuleRepository,
    pub processor: &'a ProcessorState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct WalCheckpoint {
    #[serde(default = "checkpoint_version")]
    version: u16,
    #[serde(default)]
    checkpoint_lsn: u64,
    #[serde(default, alias = "segment")]
    checkpoint_segment_id: u64,
    #[serde(default, alias = "offset")]
    checkpoint_segment_offset: u64,
}

impl From<WalPosition> for WalCheckpoint {
    fn from(position: WalPosition) -> Self {
        Self {
            version: checkpoint_version(),
            checkpoint_lsn: position.lsn,
            checkpoint_segment_id: position.segment,
            checkpoint_segment_offset: position.offset,
        }
    }
}

impl From<WalCheckpoint> for WalPosition {
    fn from(checkpoint: WalCheckpoint) -> Self {
        Self {
            lsn: checkpoint.checkpoint_lsn,
            segment: checkpoint.checkpoint_segment_id,
            offset: checkpoint.checkpoint_segment_offset,
        }
    }
}

const fn checkpoint_version() -> u16 {
    1
}

pub async fn replay_once(context: WalReplayContext<'_>) -> Result<usize> {
    let checkpoint = read_checkpoint(context.dir)?;
    let entries = read_entries_after_limit(context.dir, checkpoint, Some(REPLAY_BATCH_SIZE))?;
    let mut replayed = 0;

    for entry in entries {
        replay_record(&context, &entry.record).await?;
        write_checkpoint(context.dir, entry.next_position)?;
        remove_segments_covered_by_checkpoint(
            context.dir,
            entry.next_position.lsn,
            entry.next_position.segment,
        )?;
        replayed += 1;
    }

    Ok(replayed)
}

async fn replay_record(context: &WalReplayContext<'_>, record: &WalRecord) -> Result<()> {
    let json = match serde_json::from_slice::<Value>(&record.body) {
        Ok(json) => json,
        Err(error) => {
            warn!(
                record_id = record.record_id.as_str(),
                error = %error,
                "skip invalid wal record json body"
            );
            return Ok(());
        }
    };
    let appid = json
        .get("appid")
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
        .to_string();
    let xwhat = json
        .get("xwhat")
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_string();

    if appid == "<missing>" || !context.project_registry.contains(&appid) {
        context
            .event_sinks
            .send_json(EventStatus::Invalid, &appid, &xwhat, &json)
            .await?;
        return Ok(());
    }

    let rules = context
        .rule_repository
        .compile_project_rules(&appid)
        .await?;
    let output = context
        .processor
        .process(json.clone(), rules, request_context(record))?;

    match output {
        ProcessorOutput::Accepted(event) => {
            context
                .event_sinks
                .send_json(EventStatus::Valid, &appid, &xwhat, &event)
                .await?;
        }
        ProcessorOutput::Rejected { event, .. } => {
            context
                .event_sinks
                .send_json(EventStatus::Invalid, &appid, &xwhat, &event)
                .await?;
        }
        ProcessorOutput::Dropped { .. } => {}
    }

    Ok(())
}

fn request_context(record: &WalRecord) -> ProcessorRequestContext {
    ProcessorRequestContext::new(
        record.remote_addr.clone(),
        record.method.clone(),
        record.path.clone(),
        record
            .headers
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
            .collect::<HashMap<_, _>>(),
    )
    .with_request_id(record.record_id.clone())
}

fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join(CHECKPOINT_FILE)
}

fn read_checkpoint(dir: &Path) -> io::Result<Option<WalPosition>> {
    let path = checkpoint_path(dir);
    if !path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(path)?;
    let checkpoint = serde_json::from_slice::<WalCheckpoint>(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Some(checkpoint.into()))
}

fn write_checkpoint(dir: &Path, position: WalPosition) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = checkpoint_path(dir);
    let temp_path = dir.join(format!("{CHECKPOINT_FILE}.tmp"));
    let checkpoint = WalCheckpoint::from(position);
    let bytes = serde_json::to_vec(&checkpoint).map_err(io::Error::other)?;
    let mut temp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    temp_file.write_all(&bytes)?;
    temp_file.sync_data()?;
    drop(temp_file);
    fs::rename(&temp_path, &path)?;
    File::open(dir)?.sync_all()
}
