use crate::ingest::processor::{ProcessorRequestContext, ProcessorRuntime};
use crate::repositories::RuleRepository;
use crate::services::ProjectRegistryState;
use crate::settings::{AutoOffsetReset, CheckpointSettings, ReplaySettings};
use crate::sinks::{EventSinkBatchConfig, EventSinkState};
use crate::wal::{
    error::{ReplayAction, ReplayIssue, QUARANTINE_LOG_TARGET},
    read_entries_after_limit, read_wal_bounds, remove_segments_covered_by_checkpoint, WalBounds,
    WalEntry, WalPosition, WalRecord,
};
use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::warn;

const CHECKPOINT_FILE: &str = "checkpoint.json";
const NODE_ID_FILE: &str = "node_id";
const QUARANTINE_SCHEMA: &str = "ingest4x.wal.quarantine.v1";
const REPLAY_BATCH_SIZE: usize = 1024;

pub struct WalReplayContext<'a> {
    pub dir: &'a Path,
    pub event_sinks: &'a EventSinkState,
    pub project_registry: &'a ProjectRegistryState,
    pub rule_repository: &'a RuleRepository,
    pub processor: &'a dyn ProcessorRuntime,
    pub checkpoint: CheckpointSettings,
    pub replay: ReplaySettings,
}

struct CheckpointFlushPolicy {
    flush_interval: Duration,
    flush_records: usize,
    flush_bytes: u64,
}

struct ReplayWindowPolicy {
    max_records: usize,
    max_bytes: u64,
}

struct SinkBatchPolicy {
    max_events: usize,
    max_bytes: u64,
}

struct ReplayCheckpointState {
    checkpoint: Option<WalPosition>,
    pending_checkpoint: Option<WalPosition>,
    pending_records: usize,
    pending_bytes: u64,
    last_checkpoint_flush: Instant,
}

struct ReplayEntryDeliveries {
    entry: WalEntry,
    deliveries_by_sink: HashMap<String, Vec<Value>>,
}

struct SinkDeliveryEvent {
    lsn: u64,
    event: Value,
    bytes: u64,
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

pub fn initialize_replay_checkpoint(dir: &Path, event_sinks: &EventSinkState) -> io::Result<()> {
    let _state = read_replay_checkpoint_state(dir, event_sinks)?;
    Ok(())
}

pub async fn replay_once(context: WalReplayContext<'_>) -> Result<usize> {
    let checkpoint_policy = checkpoint_flush_policy(&context.checkpoint)?;
    let replay_policy = replay_window_policy(&context.replay);
    if context.event_sinks.sink_names().is_empty() {
        return Ok(0);
    }

    let mut checkpoint_state = read_replay_checkpoint_state(context.dir, context.event_sinks)
        .map_err(ReplayIssue::checkpoint_corrupt)?;
    let replay_start = checkpoint_state.checkpoint;
    let entries = read_entries_after_limit(context.dir, replay_start, Some(REPLAY_BATCH_SIZE))
        .map_err(ReplayIssue::wal_read_failed)?;
    let mut expected_lsn = replay_start.map(|position| position.lsn + 1);
    let mut replay_batch = Vec::new();
    let mut replayed = 0;

    for entry in entries {
        let expected = expected_lsn.get_or_insert(entry.position.lsn);
        if entry.position.lsn != *expected {
            replay_batch_to_sinks(
                &context,
                &checkpoint_policy,
                &mut checkpoint_state,
                &replay_batch,
            )
            .await?;
            return Err(ReplayIssue::wal_lsn_gap(*expected, entry.position.lsn).into());
        }
        let deliveries = match process_record(&context, &entry.record).await {
            Ok(deliveries) => deliveries,
            Err(issue) if issue.action() == ReplayAction::QuarantineRecord => {
                replay_batch_to_sinks(
                    &context,
                    &checkpoint_policy,
                    &mut checkpoint_state,
                    &replay_batch,
                )
                .await?;
                replay_batch.clear();
                quarantine_replay_issue(&mut checkpoint_state, &entry, &issue);
                expected_lsn = Some(entry.position.lsn + 1);
                replayed += 1;
                continue;
            }
            Err(issue) => {
                replay_batch_to_sinks(
                    &context,
                    &checkpoint_policy,
                    &mut checkpoint_state,
                    &replay_batch,
                )
                .await?;
                return Err(issue.into());
            }
        };
        let deliveries_by_sink = match group_deliveries_by_sink(&context, deliveries) {
            Ok(deliveries_by_sink) => deliveries_by_sink,
            Err(issue) if issue.action() == ReplayAction::QuarantineRecord => {
                replay_batch_to_sinks(
                    &context,
                    &checkpoint_policy,
                    &mut checkpoint_state,
                    &replay_batch,
                )
                .await?;
                replay_batch.clear();
                quarantine_replay_issue(&mut checkpoint_state, &entry, &issue);
                expected_lsn = Some(entry.position.lsn + 1);
                replayed += 1;
                continue;
            }
            Err(issue) => return Err(issue.into()),
        };
        expected_lsn = Some(entry.position.lsn + 1);
        replayed += 1;
        replay_batch.push(ReplayEntryDeliveries {
            entry,
            deliveries_by_sink,
        });
        if replay_batch_reaches_window_policy(&replay_batch, &replay_policy) {
            replay_batch_to_sinks(
                &context,
                &checkpoint_policy,
                &mut checkpoint_state,
                &replay_batch,
            )
            .await?;
            replay_batch.clear();
        }
    }

    replay_batch_to_sinks(
        &context,
        &checkpoint_policy,
        &mut checkpoint_state,
        &replay_batch,
    )
    .await?;
    flush_pending_checkpoint(context.dir, &mut checkpoint_state)
        .map_err(ReplayIssue::checkpoint_write_failed)?;
    cleanup_covered_segments(context.dir, &checkpoint_state)
        .map_err(ReplayIssue::checkpoint_write_failed)?;

    Ok(replayed)
}

#[derive(Serialize)]
struct QuarantinedWalRecord<'a> {
    schema: &'a str,
    code: &'a str,
    action: &'a str,
    record_id: &'a str,
    position: WalPosition,
    next_position: WalPosition,
    node_id: &'a str,
    received_at_ms: u64,
    method: &'a str,
    path: &'a str,
    query: Option<&'a str>,
    xwhat: Option<String>,
    target: Option<String>,
    message: &'a str,
    error: String,
    body_base64: String,
}

fn quarantine_replay_issue(
    checkpoint_state: &mut ReplayCheckpointState,
    entry: &WalEntry,
    issue: &ReplayIssue,
) {
    let record = quarantine_record(entry, issue);
    let record_json =
        serde_json::to_string(&record).expect("quarantine record should serialize to json");
    warn!(
        target: QUARANTINE_LOG_TARGET,
        record = %record_json,
        record_id = entry.record.record_id.as_str(),
        lsn = entry.position.lsn,
        code = issue.code(),
        action = issue.action().as_str(),
        error = %issue,
        "wal record quarantined; replay will continue"
    );
    mark_checkpoint_pending(checkpoint_state, entry);
}

fn quarantine_record<'a>(entry: &'a WalEntry, issue: &'a ReplayIssue) -> QuarantinedWalRecord<'a> {
    let error = issue.to_string();
    let http = entry.record.http();
    let body_xwhat = quarantine_event_xwhat(&entry.record.payload);
    QuarantinedWalRecord {
        schema: QUARANTINE_SCHEMA,
        code: issue.code(),
        action: issue.action().as_str(),
        record_id: entry.record.record_id.as_str(),
        position: entry.position,
        next_position: entry.next_position,
        node_id: entry.record.node_id.as_str(),
        received_at_ms: entry.record.received_at_ms(),
        method: http.method.as_str(),
        path: http.path.as_str(),
        query: http.query.as_deref(),
        xwhat: body_xwhat.or_else(|| issue.xwhat().map(str::to_string)),
        target: issue.target().map(str::to_string),
        message: issue.message(),
        error,
        body_base64: STANDARD.encode(&entry.record.payload),
    }
}

fn quarantine_event_xwhat(body: &[u8]) -> Option<String> {
    let Ok(json) = serde_json::from_slice::<Value>(body) else {
        return None;
    };

    json.get("xwhat")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn read_replay_checkpoint_state(
    dir: &Path,
    event_sinks: &EventSinkState,
) -> io::Result<ReplayCheckpointState> {
    let bounds = read_wal_bounds(dir)?;
    let reset = pipeline_auto_offset_reset(event_sinks);
    Ok(ReplayCheckpointState {
        checkpoint: read_checkpoint(dir, reset, &bounds)?,
        pending_checkpoint: None,
        pending_records: 0,
        pending_bytes: 0,
        last_checkpoint_flush: Instant::now(),
    })
}

fn pipeline_auto_offset_reset(event_sinks: &EventSinkState) -> AutoOffsetReset {
    for sink_name in event_sinks.sink_names() {
        if event_sinks.auto_offset_reset(&sink_name) == Some(AutoOffsetReset::Earliest) {
            return AutoOffsetReset::Earliest;
        }
    }
    AutoOffsetReset::Latest
}

fn checkpoint_flush_policy(
    settings: &CheckpointSettings,
) -> std::result::Result<CheckpointFlushPolicy, ReplayIssue> {
    let flush_interval = humantime::parse_duration(&settings.flush_interval).map_err(|error| {
        ReplayIssue::checkpoint_config_invalid(format!(
            "invalid checkpoint.flush_interval: {error}"
        ))
    })?;
    Ok(CheckpointFlushPolicy {
        flush_interval: flush_interval.max(Duration::from_millis(1)),
        flush_records: settings.flush_records.max(1),
        flush_bytes: settings.flush_bytes.max(1),
    })
}

fn replay_window_policy(settings: &ReplaySettings) -> ReplayWindowPolicy {
    ReplayWindowPolicy {
        max_records: settings.max_records.max(1),
        max_bytes: settings.max_bytes.max(1),
    }
}

fn sink_batch_policy(
    settings: &ReplaySettings,
    override_config: Option<&EventSinkBatchConfig>,
) -> SinkBatchPolicy {
    SinkBatchPolicy {
        max_events: override_config
            .and_then(|config| config.max_events)
            .unwrap_or(settings.sink_batch.max_events)
            .max(1),
        max_bytes: override_config
            .and_then(|config| config.max_bytes)
            .unwrap_or(settings.sink_batch.max_bytes)
            .max(1),
    }
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

fn replay_batch_reaches_window_policy(
    batch: &[ReplayEntryDeliveries],
    policy: &ReplayWindowPolicy,
) -> bool {
    batch.len() >= policy.max_records || replay_batch_bytes(batch) >= policy.max_bytes
}

fn replay_batch_bytes(batch: &[ReplayEntryDeliveries]) -> u64 {
    batch.iter().fold(0, |bytes, item| {
        bytes.saturating_add(
            item.entry
                .next_position
                .offset
                .saturating_sub(item.entry.position.offset),
        )
    })
}

async fn process_record(
    context: &WalReplayContext<'_>,
    record: &WalRecord,
) -> std::result::Result<Vec<crate::rhai_ctx::ProcessorDelivery>, ReplayIssue> {
    let json = match serde_json::from_slice::<Value>(&record.payload) {
        Ok(json) => json,
        Err(error) => {
            warn!(
                record_id = record.record_id.as_str(),
                error = %error,
                "invalid wal record json body"
            );
            return Err(ReplayIssue::invalid_json_body(error));
        }
    };
    let xwhat = json
        .get("xwhat")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    if !context
        .project_registry
        .contains_project_id(record.project_id())
    {
        return Err(ReplayIssue::unknown_project_id(record.project_id(), xwhat));
    }

    let rules = context
        .rule_repository
        .compile_project_rules(record.project_id())
        .await
        .map_err(|error| ReplayIssue::from_rule_repository(record.project_id(), error))?;
    let output = context
        .processor
        .process_event(
            record.project_id(),
            json.clone(),
            rules,
            request_context(record),
        )
        .map_err(ReplayIssue::processor_runtime_failed)?;

    Ok(output.deliveries)
}

fn group_deliveries_by_sink(
    context: &WalReplayContext<'_>,
    deliveries: Vec<crate::rhai_ctx::ProcessorDelivery>,
) -> std::result::Result<HashMap<String, Vec<Value>>, ReplayIssue> {
    let mut deliveries_by_sink: HashMap<String, Vec<Value>> = HashMap::new();
    for delivery in deliveries {
        if delivery.target.trim().is_empty() {
            return Err(ReplayIssue::empty_sink_target());
        }
        if context.event_sinks.contains_sink(&delivery.target) {
            deliveries_by_sink
                .entry(delivery.target.clone())
                .or_default()
                .push(delivery.event);
        } else {
            return Err(ReplayIssue::unknown_sink_target(delivery.target));
        }
    }
    Ok(deliveries_by_sink)
}

async fn replay_batch_to_sinks(
    context: &WalReplayContext<'_>,
    checkpoint_policy: &CheckpointFlushPolicy,
    checkpoint_state: &mut ReplayCheckpointState,
    batch: &[ReplayEntryDeliveries],
) -> std::result::Result<(), ReplayIssue> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut sink_events_by_name: HashMap<String, Vec<SinkDeliveryEvent>> = HashMap::new();
    for item in batch {
        if checkpoint_covers_entry(checkpoint_state.checkpoint, &item.entry) {
            continue;
        }
        for (sink_name, events) in &item.deliveries_by_sink {
            if events.is_empty() {
                continue;
            }
            let sink_events = sink_events_by_name.entry(sink_name.clone()).or_default();
            for event in events {
                sink_events.push(SinkDeliveryEvent {
                    lsn: item.entry.position.lsn,
                    bytes: json_event_bytes(event),
                    event: event.clone(),
                });
            }
        }
    }

    let mut sink_names = sink_events_by_name.keys().cloned().collect::<Vec<_>>();
    sink_names.sort();
    for sink_name in sink_names {
        let sink_events = sink_events_by_name
            .get(&sink_name)
            .expect("sink events should exist for sink name");
        let sink_batch_override = context.event_sinks.batch_config(&sink_name);
        let sink_batch_policy = sink_batch_policy(&context.replay, sink_batch_override.as_ref());
        if let Err((failed_lsn, error)) =
            send_sink_events_in_batches(context, &sink_name, sink_events, &sink_batch_policy).await
        {
            let issue = ReplayIssue::sink_send_failed(sink_name.clone(), failed_lsn, error);
            warn!(
                sink = sink_name.as_str(),
                lsn = failed_lsn,
                code = issue.code(),
                action = issue.action().as_str(),
                error = %issue,
                "wal replay sink delivery failed; pipeline checkpoint will not advance"
            );
            return Err(issue);
        }
    }

    for item in batch {
        if checkpoint_covers_entry(checkpoint_state.checkpoint, &item.entry) {
            continue;
        }
        mark_checkpoint_pending(checkpoint_state, &item.entry);
    }
    if should_flush_checkpoint(
        checkpoint_policy,
        checkpoint_state.pending_records,
        checkpoint_state.pending_bytes,
        checkpoint_state.last_checkpoint_flush,
    ) {
        flush_pending_checkpoint(context.dir, checkpoint_state)
            .map_err(ReplayIssue::checkpoint_write_failed)?;
        cleanup_covered_segments(context.dir, checkpoint_state)
            .map_err(ReplayIssue::checkpoint_write_failed)?;
    }

    Ok(())
}

async fn send_sink_events_in_batches(
    context: &WalReplayContext<'_>,
    sink_name: &str,
    events: &[SinkDeliveryEvent],
    policy: &SinkBatchPolicy,
) -> std::result::Result<(), (u64, anyhow::Error)> {
    let mut batch = Vec::new();
    let mut batch_bytes = 0_u64;
    let mut batch_first_lsn = None;

    for delivery in events {
        let would_exceed_events = batch.len() >= policy.max_events;
        let would_exceed_bytes =
            !batch.is_empty() && batch_bytes.saturating_add(delivery.bytes) > policy.max_bytes;
        if would_exceed_events || would_exceed_bytes {
            let failed_lsn = batch_first_lsn.expect("non-empty sink batch should have first lsn");
            send_sink_batch(context, sink_name, &batch, failed_lsn).await?;
            batch.clear();
            batch_bytes = 0;
            batch_first_lsn = None;
        }

        batch_first_lsn.get_or_insert(delivery.lsn);
        batch_bytes = batch_bytes.saturating_add(delivery.bytes);
        batch.push(delivery.event.clone());
    }

    if !batch.is_empty() {
        let failed_lsn = batch_first_lsn.expect("non-empty sink batch should have first lsn");
        send_sink_batch(context, sink_name, &batch, failed_lsn).await?;
    }

    Ok(())
}

async fn send_sink_batch(
    context: &WalReplayContext<'_>,
    sink_name: &str,
    events: &[Value],
    failed_lsn: u64,
) -> std::result::Result<(), (u64, anyhow::Error)> {
    context
        .event_sinks
        .send_events_to_sink(sink_name, events)
        .await
        .map_err(|error| (failed_lsn, error))
}

fn json_event_bytes(event: &Value) -> u64 {
    serde_json::to_vec(event)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn checkpoint_covers_entry(checkpoint: Option<WalPosition>, entry: &WalEntry) -> bool {
    checkpoint.is_some_and(|checkpoint| checkpoint.lsn >= entry.position.lsn)
}

fn mark_checkpoint_pending(state: &mut ReplayCheckpointState, entry: &WalEntry) {
    state.pending_checkpoint = Some(entry.next_position);
    state.pending_records += 1;
    state.pending_bytes += entry
        .next_position
        .offset
        .saturating_sub(entry.position.offset);
}

fn flush_pending_checkpoint(dir: &Path, state: &mut ReplayCheckpointState) -> io::Result<()> {
    let Some(position) = state.pending_checkpoint else {
        return Ok(());
    };
    write_checkpoint(dir, position)?;
    state.checkpoint = Some(position);
    state.pending_checkpoint = None;
    state.pending_records = 0;
    state.pending_bytes = 0;
    state.last_checkpoint_flush = Instant::now();
    Ok(())
}

fn cleanup_covered_segments(
    dir: &Path,
    checkpoint_state: &ReplayCheckpointState,
) -> io::Result<()> {
    let Some(watermark) = checkpoint_state.checkpoint else {
        return Ok(());
    };
    remove_segments_covered_by_checkpoint(dir, watermark.lsn, watermark.segment)
}

fn request_context(record: &WalRecord) -> ProcessorRequestContext {
    let http = record.http();
    ProcessorRequestContext::new(
        http.remote_addr.clone(),
        http.method.clone(),
        http.path.clone(),
        http.headers
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
            .collect::<HashMap<_, _>>(),
    )
    .with_request_id(record.record_id.clone())
    .with_received_at_ms(record.received_at_ms())
}

fn checkpoint_path(dir: &Path) -> std::path::PathBuf {
    dir.join(CHECKPOINT_FILE)
}

fn read_checkpoint(
    dir: &Path,
    reset: AutoOffsetReset,
    bounds: &WalBounds,
) -> io::Result<Option<WalPosition>> {
    let path = checkpoint_path(dir);
    if !path.exists() {
        return reset_checkpoint(dir, reset, bounds);
    }

    let checkpoint = read_checkpoint_file(&path)?;
    validate_checkpoint(dir, &checkpoint, &path)?;
    if checkpoint.sink_id.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint sink_id mismatch: checkpoint={:?} current=<pipeline>",
                checkpoint.sink_id
            ),
        ));
    }
    let position = checkpoint.position();
    if checkpoint_before_wal_floor(position, bounds) {
        return reset_checkpoint(dir, reset, bounds);
    }
    Ok(Some(position))
}

fn reset_checkpoint(
    dir: &Path,
    reset: AutoOffsetReset,
    bounds: &WalBounds,
) -> io::Result<Option<WalPosition>> {
    match reset {
        AutoOffsetReset::Earliest => Ok(None),
        AutoOffsetReset::Latest => {
            let Some(position) = bounds.tail else {
                return Ok(None);
            };
            write_checkpoint(dir, position)?;
            Ok(Some(position))
        }
    }
}

fn checkpoint_before_wal_floor(position: WalPosition, bounds: &WalBounds) -> bool {
    let Some(floor) = bounds.floor else {
        return false;
    };
    position.segment < floor.segment
        || (position.segment == floor.segment && position.offset < floor.offset)
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

fn write_checkpoint(dir: &Path, position: WalPosition) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = checkpoint_path(dir);
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
