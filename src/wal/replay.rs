use crate::ingest::processor::{ProcessorRequestContext, ProcessorState};
use crate::repositories::RuleRepository;
use crate::services::ProjectRegistryState;
use crate::settings::CheckpointSettings;
use crate::utils::events::EventSinkState;
use crate::wal::{
    read_entries_after_limit, remove_segments_covered_by_checkpoint, WalEntry, WalPosition,
    WalRecord,
};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::warn;

const CHECKPOINT_DIR: &str = "checkpoints";
const CHECKPOINT_FILE: &str = "checkpoint.json";
const NODE_ID_FILE: &str = "node_id";
const QUARANTINE_FILE: &str = "quarantine.jsonl";
const REPLAY_BATCH_SIZE: usize = 1024;

pub struct WalReplayContext<'a> {
    pub dir: &'a Path,
    pub event_sinks: &'a EventSinkState,
    pub project_registry: &'a ProjectRegistryState,
    pub rule_repository: &'a RuleRepository,
    pub processor: &'a ProcessorState,
    pub checkpoint: CheckpointSettings,
}

struct CheckpointFlushPolicy {
    flush_interval: Duration,
    flush_records: usize,
    flush_bytes: u64,
}

struct SinkReplayState {
    checkpoint: Option<WalPosition>,
    blocked: bool,
    failure: Option<String>,
    pending_checkpoint: Option<WalPosition>,
    pending_records: usize,
    pending_bytes: u64,
    last_checkpoint_flush: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WalCheckpoint {
    #[serde(default = "checkpoint_version")]
    version: u16,
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    sink_id: Option<String>,
    #[serde(default)]
    checkpoint_lsn: u64,
    #[serde(default)]
    checkpoint_segment_id: u64,
    #[serde(default)]
    checkpoint_segment_offset: u64,
    #[serde(default)]
    updated_at: u64,
    #[serde(default)]
    checksum: u32,
}

#[derive(Serialize)]
struct WalCheckpointChecksum<'a> {
    version: u16,
    node_id: &'a str,
    sink_id: Option<&'a str>,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
    updated_at: u64,
}

impl WalCheckpoint {
    fn new(position: WalPosition, node_id: String, sink_id: Option<&str>) -> io::Result<Self> {
        let mut checkpoint = Self {
            version: checkpoint_version(),
            node_id,
            sink_id: sink_id.map(ToString::to_string),
            checkpoint_lsn: position.lsn,
            checkpoint_segment_id: position.segment,
            checkpoint_segment_offset: position.offset,
            updated_at: crate::current_timestamp_as_u64(),
            checksum: 0,
        };
        checkpoint.checksum = checkpoint.compute_checksum()?;
        Ok(checkpoint)
    }

    fn position(&self) -> WalPosition {
        WalPosition {
            lsn: self.checkpoint_lsn,
            segment: self.checkpoint_segment_id,
            offset: self.checkpoint_segment_offset,
        }
    }

    fn validate_checksum(&self, path: &Path) -> io::Result<()> {
        let expected = self.compute_checksum()?;
        if self.checksum == expected {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checkpoint checksum mismatch: {}", path.display()),
        ))
    }

    fn compute_checksum(&self) -> io::Result<u32> {
        let bytes = serde_json::to_vec(&WalCheckpointChecksum {
            version: self.version,
            node_id: self.node_id.as_str(),
            sink_id: self.sink_id.as_deref(),
            checkpoint_lsn: self.checkpoint_lsn,
            checkpoint_segment_id: self.checkpoint_segment_id,
            checkpoint_segment_offset: self.checkpoint_segment_offset,
            updated_at: self.updated_at,
        })
        .map_err(io::Error::other)?;
        Ok(crc32fast::hash(&bytes))
    }
}

const fn checkpoint_version() -> u16 {
    1
}

pub async fn replay_once(context: WalReplayContext<'_>) -> Result<usize> {
    let checkpoint_policy = checkpoint_flush_policy(&context.checkpoint)?;
    let sink_names = context.event_sinks.sink_names();
    if sink_names.is_empty() {
        return Ok(0);
    }

    let mut sink_states = read_sink_replay_states(context.dir, &sink_names)?;
    let replay_start = min_checkpoint(&sink_states);
    let entries = read_entries_after_limit(context.dir, replay_start, Some(REPLAY_BATCH_SIZE))?;
    let mut expected_lsn = replay_start.map(|position| position.lsn + 1);
    let mut replayed = 0;

    for entry in entries {
        let expected = expected_lsn.get_or_insert(entry.position.lsn);
        if entry.position.lsn != *expected {
            return Err(anyhow!(
                "non-contiguous wal lsn: expected {}, got {}",
                *expected,
                entry.position.lsn
            ));
        }
        let deliveries = match process_record(&context, &entry.record).await {
            Ok(deliveries) => deliveries,
            Err(error) => {
                warn!(
                    record_id = entry.record.record_id.as_str(),
                    lsn = entry.position.lsn,
                    error = %error,
                    "wal record quarantined; replay will continue"
                );
                quarantine_entry(context.dir, &entry, error.as_ref())?;
                mark_all_sink_checkpoints_pending(&mut sink_states, &entry);
                expected_lsn = Some(entry.position.lsn + 1);
                replayed += 1;
                continue;
            }
        };
        replay_entry_to_sinks(
            &context,
            &checkpoint_policy,
            &mut sink_states,
            &entry,
            deliveries,
        )
        .await?;
        expected_lsn = Some(entry.position.lsn + 1);
        replayed += 1;
    }

    flush_pending_checkpoints(context.dir, &mut sink_states)?;
    cleanup_covered_segments(context.dir, &sink_states)?;

    let failures = sink_failures(&sink_states);
    if !failures.is_empty() {
        return Err(anyhow!(
            "wal replay sink delivery failed: {}",
            failures.join("; ")
        ));
    }

    Ok(replayed)
}

#[derive(Serialize)]
struct QuarantinedWalRecord<'a> {
    version: u16,
    record_id: &'a str,
    position: WalPosition,
    next_position: WalPosition,
    node_id: &'a str,
    received_at_ms: u64,
    method: &'a str,
    path: &'a str,
    query: Option<&'a str>,
    error: &'a str,
}

fn quarantine_entry(dir: &Path, entry: &WalEntry, error: &dyn std::error::Error) -> io::Result<()> {
    let path = dir.join(QUARANTINE_FILE);
    let is_new_file = !path.exists();
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    let error = error.to_string();
    serde_json::to_writer(
        &mut file,
        &QuarantinedWalRecord {
            version: checkpoint_version(),
            record_id: entry.record.record_id.as_str(),
            position: entry.position,
            next_position: entry.next_position,
            node_id: entry.record.node_id.as_str(),
            received_at_ms: entry.record.received_at_ms,
            method: entry.record.method.as_str(),
            path: entry.record.path.as_str(),
            query: entry.record.query.as_deref(),
            error: error.as_str(),
        },
    )
    .map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    if is_new_file {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn read_sink_replay_states(
    dir: &Path,
    sink_names: &[String],
) -> io::Result<HashMap<String, SinkReplayState>> {
    let mut states = HashMap::new();
    for sink_name in sink_names {
        states.insert(
            sink_name.clone(),
            SinkReplayState {
                checkpoint: read_checkpoint(dir, sink_name)?,
                blocked: false,
                failure: None,
                pending_checkpoint: None,
                pending_records: 0,
                pending_bytes: 0,
                last_checkpoint_flush: Instant::now(),
            },
        );
    }
    Ok(states)
}

fn mark_all_sink_checkpoints_pending(
    sink_states: &mut HashMap<String, SinkReplayState>,
    entry: &WalEntry,
) {
    for state in sink_states.values_mut() {
        if state.blocked || checkpoint_covers_entry(state.checkpoint, entry) {
            continue;
        }
        mark_sink_checkpoint_pending(state, entry);
    }
}

fn min_checkpoint(states: &HashMap<String, SinkReplayState>) -> Option<WalPosition> {
    if states.values().any(|state| state.checkpoint.is_none()) {
        return None;
    }
    states
        .values()
        .filter_map(|state| state.checkpoint)
        .min_by_key(|position| (position.lsn, position.segment, position.offset))
}

fn checkpoint_flush_policy(settings: &CheckpointSettings) -> Result<CheckpointFlushPolicy> {
    let flush_interval = humantime::parse_duration(&settings.flush_interval)
        .map_err(|error| anyhow!("invalid checkpoint.flush_interval: {error}"))?;
    Ok(CheckpointFlushPolicy {
        flush_interval: flush_interval.max(Duration::from_millis(1)),
        flush_records: settings.flush_records.max(1),
        flush_bytes: settings.flush_bytes.max(1),
    })
}

fn should_flush_checkpoint(
    policy: &CheckpointFlushPolicy,
    records: usize,
    bytes: u64,
    last_flush: Instant,
) -> bool {
    records >= policy.flush_records
        || bytes >= policy.flush_bytes
        || last_flush.elapsed() >= policy.flush_interval
}

async fn process_record(
    context: &WalReplayContext<'_>,
    record: &WalRecord,
) -> Result<Vec<crate::rhai_ctx::ProcessorDelivery>> {
    let json = match serde_json::from_slice::<Value>(&record.body) {
        Ok(json) => json,
        Err(error) => {
            warn!(
                record_id = record.record_id.as_str(),
                error = %error,
                "invalid wal record json body"
            );
            return Err(anyhow!(
                "invalid wal record json body for {}: {}",
                record.record_id,
                error
            ));
        }
    };
    let appid = json
        .get("appid")
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
        .to_string();
    if appid == "<missing>" || !context.project_registry.contains(&appid) {
        return Err(anyhow!(
            "wal record {} references unknown appid `{appid}`",
            record.record_id
        ));
    }

    let rules = context
        .rule_repository
        .compile_project_rules(&appid)
        .await?;
    let output = context
        .processor
        .process(json.clone(), rules, request_context(record))?;

    Ok(output.deliveries)
}

async fn replay_entry_to_sinks(
    context: &WalReplayContext<'_>,
    checkpoint_policy: &CheckpointFlushPolicy,
    sink_states: &mut HashMap<String, SinkReplayState>,
    entry: &WalEntry,
    deliveries: Vec<crate::rhai_ctx::ProcessorDelivery>,
) -> Result<()> {
    let mut deliveries_by_sink: HashMap<String, Vec<crate::rhai_ctx::ProcessorDelivery>> =
        HashMap::new();
    for delivery in deliveries {
        if context.event_sinks.contains_sink(&delivery.target) {
            deliveries_by_sink
                .entry(delivery.target.clone())
                .or_default()
                .push(delivery);
        } else {
            warn!(
                target = delivery.target.as_str(),
                record_id = entry.record.record_id.as_str(),
                "processor delivery ignored unknown sink target"
            );
        }
    }

    let sink_names = sink_states.keys().cloned().collect::<Vec<_>>();
    for sink_name in sink_names {
        let Some(state) = sink_states.get_mut(&sink_name) else {
            continue;
        };
        if state.blocked || checkpoint_covers_entry(state.checkpoint, entry) {
            continue;
        }

        if let Some(sink_deliveries) = deliveries_by_sink.get(&sink_name) {
            for delivery in sink_deliveries {
                if let Err(error) = context.event_sinks.send_delivery(delivery).await {
                    state.blocked = true;
                    state.failure = Some(format!(
                        "sink `{sink_name}` failed at lsn {}: {error}",
                        entry.position.lsn
                    ));
                    warn!(
                        sink = sink_name.as_str(),
                        lsn = entry.position.lsn,
                        error = %error,
                        "wal replay sink delivery failed; sink checkpoint will not advance"
                    );
                    break;
                }
            }
            if state.blocked {
                continue;
            }
        }

        mark_sink_checkpoint_pending(state, entry);
        if should_flush_checkpoint(
            checkpoint_policy,
            state.pending_records,
            state.pending_bytes,
            state.last_checkpoint_flush,
        ) {
            flush_sink_checkpoint(context.dir, sink_name.as_str(), state)?;
            cleanup_covered_segments(context.dir, sink_states)?;
        }
    }

    Ok(())
}

fn checkpoint_covers_entry(checkpoint: Option<WalPosition>, entry: &WalEntry) -> bool {
    checkpoint.is_some_and(|checkpoint| checkpoint.lsn >= entry.position.lsn)
}

fn mark_sink_checkpoint_pending(state: &mut SinkReplayState, entry: &WalEntry) {
    state.pending_checkpoint = Some(entry.next_position);
    state.pending_records += 1;
    state.pending_bytes += entry
        .next_position
        .offset
        .saturating_sub(entry.position.offset);
}

fn flush_sink_checkpoint(
    dir: &Path,
    sink_name: &str,
    state: &mut SinkReplayState,
) -> io::Result<()> {
    let Some(position) = state.pending_checkpoint else {
        return Ok(());
    };
    write_checkpoint(dir, sink_name, position)?;
    state.checkpoint = Some(position);
    state.pending_checkpoint = None;
    state.pending_records = 0;
    state.pending_bytes = 0;
    state.last_checkpoint_flush = Instant::now();
    Ok(())
}

fn flush_pending_checkpoints(
    dir: &Path,
    sink_states: &mut HashMap<String, SinkReplayState>,
) -> io::Result<()> {
    let sink_names = sink_states.keys().cloned().collect::<Vec<_>>();
    for sink_name in sink_names {
        if let Some(state) = sink_states.get_mut(&sink_name) {
            flush_sink_checkpoint(dir, sink_name.as_str(), state)?;
        }
    }
    Ok(())
}

fn cleanup_covered_segments(
    dir: &Path,
    sink_states: &HashMap<String, SinkReplayState>,
) -> io::Result<()> {
    let Some(watermark) = min_checkpoint(sink_states) else {
        return Ok(());
    };
    write_global_checkpoint(dir, watermark)?;
    remove_segments_covered_by_checkpoint(dir, watermark.lsn, watermark.segment)
}

fn sink_failures(sink_states: &HashMap<String, SinkReplayState>) -> Vec<String> {
    let mut failures = sink_states
        .values()
        .filter_map(|state| state.failure.clone())
        .collect::<Vec<_>>();
    failures.sort();
    failures
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
    .with_received_at_ms(record.received_at_ms)
}

fn global_checkpoint_path(dir: &Path) -> PathBuf {
    dir.join(CHECKPOINT_FILE)
}

fn sink_checkpoint_path(dir: &Path, sink_name: &str) -> PathBuf {
    dir.join(CHECKPOINT_DIR)
        .join(format!("{}.json", checkpoint_file_stem(sink_name)))
}

fn read_checkpoint(dir: &Path, sink_name: &str) -> io::Result<Option<WalPosition>> {
    let path = sink_checkpoint_path(dir, sink_name);
    if !path.exists() {
        return read_global_checkpoint(dir);
    }

    let checkpoint = read_checkpoint_file(&path)?;
    validate_checkpoint(dir, &checkpoint, &path)?;
    if checkpoint.sink_id.as_deref() != Some(sink_name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint sink_id mismatch: checkpoint={:?} current={}",
                checkpoint.sink_id, sink_name
            ),
        ));
    }
    Ok(Some(checkpoint.position()))
}

fn read_global_checkpoint(dir: &Path) -> io::Result<Option<WalPosition>> {
    let path = global_checkpoint_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let checkpoint = read_checkpoint_file(&path)?;
    validate_checkpoint(dir, &checkpoint, &path)?;
    if checkpoint.sink_id.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint sink_id mismatch: checkpoint={:?} current=<global>",
                checkpoint.sink_id
            ),
        ));
    }
    Ok(Some(checkpoint.position()))
}

fn read_checkpoint_file(path: &Path) -> io::Result<WalCheckpoint> {
    let bytes = fs::read(path)?;
    let checkpoint = serde_json::from_slice::<WalCheckpoint>(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(checkpoint)
}

fn validate_checkpoint(dir: &Path, checkpoint: &WalCheckpoint, path: &Path) -> io::Result<()> {
    checkpoint.validate_checksum(path)?;
    let node_id = read_node_id(dir)?;
    if checkpoint.node_id != node_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint node_id mismatch: checkpoint={} current={}",
                checkpoint.node_id, node_id
            ),
        ));
    }
    Ok(())
}

fn write_checkpoint(dir: &Path, sink_name: &str, position: WalPosition) -> io::Result<()> {
    let checkpoint_dir = dir.join(CHECKPOINT_DIR);
    fs::create_dir_all(&checkpoint_dir)?;
    let path = sink_checkpoint_path(dir, sink_name);
    let temp_path = checkpoint_dir.join(format!("{}.json.tmp", checkpoint_file_stem(sink_name)));
    let checkpoint = WalCheckpoint::new(position, read_node_id(dir)?, Some(sink_name))?;
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
    File::open(&checkpoint_dir)?.sync_all()
}

fn write_global_checkpoint(dir: &Path, position: WalPosition) -> io::Result<()> {
    let path = global_checkpoint_path(dir);
    let temp_path = dir.join(format!("{CHECKPOINT_FILE}.tmp"));
    let checkpoint = WalCheckpoint::new(position, read_node_id(dir)?, None)?;
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

fn read_node_id(dir: &Path) -> io::Result<String> {
    let node_id = fs::read_to_string(dir.join(NODE_ID_FILE))?;
    Ok(node_id.trim().to_string())
}

fn checkpoint_file_stem(sink_name: &str) -> String {
    let mut stem = String::new();
    for byte in sink_name.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            stem.push(char::from(*byte));
        } else {
            stem.push('_');
            stem.push_str(format!("{byte:02x}").as_str());
        }
    }
    if stem.is_empty() {
        "sink".to_string()
    } else {
        stem
    }
}
