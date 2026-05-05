use crate::settings::WalSettings;
use serde::{Deserialize, Serialize};

pub mod replay;

#[cfg(debug_assertions)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::error;
use uuid::Uuid;

const SEGMENT_MAGIC: &[u8; 8] = b"i4x.seg\0";
const SEGMENT_VERSION: u16 = 1;
const SEGMENT_HEADER_LEN: u64 = 512;
const SEGMENT_HEADER_CRC_OFFSET: usize = SEGMENT_HEADER_LEN as usize - 4;
const SEGMENT_NODE_ID_OFFSET: usize = 38;
const RECORD_MAGIC: &[u8; 8] = b"i4x.rec\0";
const RECORD_VERSION: u16 = 1;
const RECORD_TYPE_DATA: u8 = 1;
const RECORD_FLAGS_NONE: u8 = 0;
const RECORD_HEADER_FIXED_LEN: usize = 42;
const SEGMENT_EXTENSION: &str = "wal";
const FIRST_SEGMENT_ID: u64 = 1;
const NODE_ID_FILE: &str = "node_id";
const WAL_LOCK_FILE: &str = "wal.lock";
const CHECKPOINT_FILE: &str = "checkpoint.json";

static RECORD_SEQUENCE: AtomicU64 = AtomicU64::new(1);
#[cfg(debug_assertions)]
thread_local! {
    static FAIL_AFTER_TEST_WRITES: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub record_id: String,
    #[serde(default)]
    pub lsn: u64,
    #[serde(default)]
    pub node_id: String,
    pub received_at_ms: u64,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub remote_addr: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalPosition {
    pub lsn: u64,
    pub segment: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub position: WalPosition,
    pub next_position: WalPosition,
    pub record: WalRecord,
}

#[derive(Debug)]
pub struct WalWriter {
    inner: Arc<WalWriterInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSnapshot {
    pub node_id: String,
    pub ready: bool,
    pub no_sync: bool,
    pub available_bytes: u64,
    pub min_free_bytes: u64,
    pub active_segment_id: u64,
    pub active_segment_bytes: u64,
    pub max_lsn: u64,
    pub checkpoint_lsn: u64,
}

#[derive(Debug)]
struct WalWriterInner {
    dir: PathBuf,
    _lock_file: File,
    node_id: String,
    segment_max_bytes: u64,
    flush_max_interval: Duration,
    flush_max_records: usize,
    no_sync: bool,
    min_free_bytes: u64,
    state: Mutex<WalWriterState>,
}

#[derive(Debug)]
struct WalWriterState {
    segment_id: u64,
    offset: u64,
    next_lsn: u64,
    buffered: Vec<BufferedWalRecord>,
}

#[derive(Debug)]
struct BufferedWalRecord {
    record: WalRecord,
    response: Option<mpsc::Sender<io::Result<WalPosition>>>,
}

impl WalWriter {
    pub fn new(settings: &WalSettings) -> io::Result<Self> {
        let dir = PathBuf::from(&settings.dir);
        fs::create_dir_all(&dir)?;
        let lock_file = acquire_wal_lock(&dir)?;
        let node_id = resolve_node_id(settings.node_id.as_deref(), &dir)?;
        let checkpoint = read_checkpoint(&dir, &node_id)?;
        let segment_id = recover_active_segment_id(&dir, checkpoint.as_ref())?;
        let next_lsn = recover_next_lsn(&dir, checkpoint.as_ref())?;
        ensure_segment_file(&dir, segment_id, &node_id, next_lsn)?;
        let offset = repair_segment_tail(&segment_path(&dir, segment_id))?;
        ensure_wal_disk_space(&dir, settings.min_free_bytes, 0)?;
        let flush_max_interval =
            humantime::parse_duration(&settings.flush_max_interval).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid flush_max_interval: {error}"),
                )
            })?;
        let flush_max_interval = flush_max_interval.max(Duration::from_millis(1));

        let inner = Arc::new(WalWriterInner {
            dir,
            _lock_file: lock_file,
            node_id,
            segment_max_bytes: settings.wal_segment_max_bytes.max(SEGMENT_HEADER_LEN + 1),
            flush_max_interval,
            flush_max_records: settings.flush_max_records.max(1),
            no_sync: settings.no_sync,
            min_free_bytes: settings.min_free_bytes,
            state: Mutex::new(WalWriterState {
                segment_id,
                offset,
                next_lsn,
                buffered: Vec::new(),
            }),
        });

        spawn_flush_loop(Arc::downgrade(&inner));

        Ok(Self { inner })
    }

    pub fn append(&self, record: &WalRecord) -> io::Result<WalPosition> {
        self.inner.append(record)
    }

    pub fn check_ready(&self) -> io::Result<()> {
        self.inner.ensure_disk_space(0)
    }

    pub fn snapshot(&self) -> io::Result<WalSnapshot> {
        self.inner.snapshot()
    }

    #[allow(unused)]
    pub fn flush(&self) -> io::Result<()> {
        self.inner.flush_buffer()
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            error!(error = %error, "failed to flush wal buffer on drop");
        }
    }
}

impl WalWriterInner {
    fn append(&self, record: &WalRecord) -> io::Result<WalPosition> {
        if self.no_sync {
            self.append_buffered(record, None)
        } else {
            let (tx, rx) = mpsc::channel();
            self.append_buffered(record, Some(tx))?;
            rx.recv()
                .map_err(|_| io::Error::other("wal flush response channel closed"))?
        }
    }

    fn append_buffered(
        &self,
        record: &WalRecord,
        response: Option<mpsc::Sender<io::Result<WalPosition>>>,
    ) -> io::Result<WalPosition> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("wal writer mutex poisoned"))?;
        let position = WalPosition {
            lsn: state.next_lsn,
            segment: state.segment_id,
            offset: state.offset,
        };
        let mut record = record.clone();
        assign_wal_metadata(&mut record, state.next_lsn, &self.node_id);
        state.next_lsn += 1;
        state.buffered.push(BufferedWalRecord { record, response });

        if state.buffered.len() >= self.flush_max_records {
            self.flush_buffer_locked(&mut state)?;
        }

        Ok(position)
    }

    fn flush_buffer(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("wal writer mutex poisoned"))?;

        self.flush_buffer_locked(&mut state)
    }

    fn flush_buffer_locked(&self, state: &mut WalWriterState) -> io::Result<()> {
        if state.buffered.is_empty() {
            return Ok(());
        }

        let buffered = std::mem::take(&mut state.buffered);
        let rollback_segment = state.segment_id;
        let rollback_offset = state.offset;
        let rollback_next_lsn = buffered
            .first()
            .map(|entry| entry.record.lsn)
            .unwrap_or(state.next_lsn);
        let has_waiters = buffered.iter().any(|entry| entry.response.is_some());
        let mut positions = Vec::with_capacity(buffered.len());
        let flush_result: io::Result<()> = (|| {
            let mut touched_segments = BTreeSet::new();
            for entry in &buffered {
                let bytes = serialize_frame(&entry.record)?;
                let position = self.append_frame_locked(state, &bytes, entry.record.lsn, false)?;
                touched_segments.insert(position.segment);
                positions.push(position);
            }
            for segment_id in touched_segments {
                self.sync_segment(segment_id)?;
            }
            Ok(())
        })();

        if let Err(error) = flush_result {
            self.truncate_after(rollback_segment, rollback_offset, state.segment_id)?;
            state.segment_id = rollback_segment;
            state.offset = rollback_offset;
            if has_waiters {
                state.next_lsn = rollback_next_lsn;
                notify_buffered_failure(buffered, error.to_string());
            } else {
                state.buffered = buffered;
            }
            return Err(error);
        }

        notify_buffered_success(buffered, positions);
        Ok(())
    }

    fn append_frame_locked(
        &self,
        state: &mut WalWriterState,
        bytes: &[u8],
        lsn: u64,
        sync: bool,
    ) -> io::Result<WalPosition> {
        if state.offset > SEGMENT_HEADER_LEN
            && state.offset + bytes.len() as u64 > self.segment_max_bytes
        {
            state.segment_id += 1;
            state.offset = SEGMENT_HEADER_LEN;
            ensure_segment_file(&self.dir, state.segment_id, &self.node_id, lsn)?;
        }
        self.ensure_disk_space(bytes.len() as u64)?;

        let path = segment_path(&self.dir, state.segment_id);
        let mut file = OpenOptions::new().append(true).open(&path)?;
        let position = WalPosition {
            lsn,
            segment: state.segment_id,
            offset: state.offset,
        };
        file.write_all(&bytes)?;
        if sync {
            file.sync_data()?;
        }
        state.offset += bytes.len() as u64;
        Ok(position)
    }

    fn ensure_disk_space(&self, estimated_wal_bytes: u64) -> io::Result<()> {
        ensure_wal_disk_space(&self.dir, self.min_free_bytes, estimated_wal_bytes)
    }

    fn snapshot(&self) -> io::Result<WalSnapshot> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("wal writer mutex poisoned"))?;
        let checkpoint_lsn = read_checkpoint(&self.dir, &self.node_id)?
            .map(|checkpoint| checkpoint.checkpoint_lsn)
            .unwrap_or(0);
        let available_bytes = fs2::available_space(&self.dir)?;
        let ready = available_bytes >= self.min_free_bytes;

        Ok(WalSnapshot {
            node_id: self.node_id.clone(),
            ready,
            no_sync: self.no_sync,
            available_bytes,
            min_free_bytes: self.min_free_bytes,
            active_segment_id: state.segment_id,
            active_segment_bytes: state.offset,
            max_lsn: state.next_lsn.saturating_sub(1),
            checkpoint_lsn,
        })
    }

    fn sync_segment(&self, segment_id: u64) -> io::Result<()> {
        let path = segment_path(&self.dir, segment_id);
        OpenOptions::new().read(true).open(&path)?.sync_data()
    }

    fn truncate_after(
        &self,
        segment_id: u64,
        offset: u64,
        current_segment_id: u64,
    ) -> io::Result<()> {
        let path = segment_path(&self.dir, segment_id);
        let file = OpenOptions::new().write(true).open(&path)?;
        file.set_len(offset)?;
        file.sync_data()?;

        for stale_segment_id in (segment_id + 1)..=current_segment_id {
            let path = segment_path(&self.dir, stale_segment_id);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        sync_directory(&self.dir)
    }
}

fn notify_buffered_success(buffered: Vec<BufferedWalRecord>, positions: Vec<WalPosition>) {
    for (entry, position) in buffered.into_iter().zip(positions) {
        if let Some(response) = entry.response {
            let _ = response.send(Ok(position));
        }
    }
}

fn notify_buffered_failure(buffered: Vec<BufferedWalRecord>, message: String) {
    for entry in buffered {
        if let Some(response) = entry.response {
            let _ = response.send(Err(io::Error::other(message.clone())));
        }
    }
}

fn ensure_wal_disk_space(
    dir: &Path,
    min_free_bytes: u64,
    estimated_wal_bytes: u64,
) -> io::Result<()> {
    if min_free_bytes == 0 {
        return Ok(());
    }
    let available_bytes = fs2::available_space(dir)?;
    if available_bytes.saturating_sub(estimated_wal_bytes) < min_free_bytes {
        return Err(io::Error::other(format!(
            "wal disk space is insufficient: available_bytes={available_bytes} estimated_wal_bytes={estimated_wal_bytes} min_free_bytes={min_free_bytes}"
        )));
    }
    Ok(())
}

fn spawn_flush_loop(inner: Weak<WalWriterInner>) {
    thread::Builder::new()
        .name("ingest4x-wal-flush".to_string())
        .spawn(move || loop {
            let Some(flush_max_interval) = inner.upgrade().map(|inner| inner.flush_max_interval)
            else {
                break;
            };
            thread::sleep(flush_max_interval);
            let Some(inner) = inner.upgrade() else {
                break;
            };
            if let Err(error) = inner.flush_buffer() {
                error!(error = %error, "failed to flush wal buffer");
            }
        })
        .expect("spawn wal flush thread");
}

pub fn new_record(
    method: impl Into<String>,
    path: impl Into<String>,
    query: Option<String>,
    remote_addr: Option<String>,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
) -> WalRecord {
    let received_at_ms = now_ms();
    let sequence = RECORD_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    WalRecord {
        record_id: format!("wal-{received_at_ms}-{sequence}"),
        lsn: 0,
        node_id: String::new(),
        received_at_ms,
        method: method.into(),
        path: path.into(),
        query,
        remote_addr,
        headers,
        body,
    }
}

pub fn read_all_records(dir: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
    let dir = dir.as_ref();
    let mut segments = segment_ids(dir)?;
    segments.sort_unstable();

    let mut records = Vec::new();
    for segment_id in segments {
        records.extend(read_segment_records(&segment_path(dir, segment_id))?);
    }
    Ok(records)
}

pub fn read_entries_after(
    dir: impl AsRef<Path>,
    checkpoint: Option<WalPosition>,
) -> io::Result<Vec<WalEntry>> {
    read_entries_after_limit(dir, checkpoint, None)
}

pub fn read_entries_after_limit(
    dir: impl AsRef<Path>,
    checkpoint: Option<WalPosition>,
    limit: Option<usize>,
) -> io::Result<Vec<WalEntry>> {
    let dir = dir.as_ref();
    let mut segments = segment_ids(dir)?;
    segments.sort_unstable();

    let mut entries = Vec::new();
    for segment_id in segments {
        if checkpoint.is_some_and(|checkpoint| segment_id < checkpoint.segment) {
            continue;
        }

        let remaining = limit.map(|limit| limit.saturating_sub(entries.len()));
        if remaining == Some(0) {
            return Ok(entries);
        }
        let start_offset = checkpoint
            .filter(|checkpoint| checkpoint.segment == segment_id)
            .map(|checkpoint| checkpoint.offset)
            .unwrap_or(SEGMENT_HEADER_LEN);
        entries.extend(read_segment_entries_after(
            &segment_path(dir, segment_id),
            start_offset,
            remaining,
        )?);
        if limit.is_some_and(|limit| entries.len() >= limit) {
            return Ok(entries);
        }
    }
    Ok(entries)
}

pub fn remove_segments_before(dir: impl AsRef<Path>, segment: u64) -> io::Result<()> {
    let dir = dir.as_ref();
    for segment_id in segment_ids(dir)? {
        if segment_id >= segment {
            continue;
        }
        fs::remove_file(segment_path(dir, segment_id))?;
    }
    sync_directory(dir)
}

fn acquire_wal_lock(dir: &Path) -> io::Result<File> {
    let path = dir.join(WAL_LOCK_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;

    if let Err(error) = file.try_lock() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "wal directory lock is already held: {}: {error}",
                path.display()
            ),
        ));
    }

    file.set_len(0)?;
    file.write_all(std::process::id().to_string().as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    sync_directory(dir)?;
    Ok(file)
}

pub fn remove_segments_covered_by_checkpoint(
    dir: impl AsRef<Path>,
    checkpoint_lsn: u64,
    keep_from_segment: u64,
) -> io::Result<()> {
    let dir = dir.as_ref();
    let mut removed = false;
    for segment_id in segment_ids(dir)? {
        if segment_id >= keep_from_segment {
            continue;
        }
        let path = segment_path(dir, segment_id);
        let max_lsn = scan_segment(&path)?
            .records
            .into_iter()
            .map(|record| record.lsn)
            .max()
            .unwrap_or(0);
        if max_lsn <= checkpoint_lsn {
            fs::remove_file(path)?;
            removed = true;
        }
    }
    if removed {
        sync_directory(dir)?;
    }
    Ok(())
}

fn read_segment_records(path: &Path) -> io::Result<Vec<WalRecord>> {
    scan_segment(path).map(|scan| scan.records)
}

fn read_segment_entries_after(
    path: &Path,
    start_offset: u64,
    limit: Option<usize>,
) -> io::Result<Vec<WalEntry>> {
    let mut reader = BufReader::new(File::open(path)?);
    let header = read_segment_header(&mut reader, path)?;
    if start_offset < SEGMENT_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid wal checkpoint offset: {}", path.display()),
        ));
    }
    let segment_len = reader.get_ref().metadata()?.len();
    if start_offset > segment_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid wal checkpoint offset: {}", path.display()),
        ));
    }
    reader.seek(SeekFrom::Start(start_offset))?;

    let segment_id = segment_id_from_path(path)?;
    let mut entries = Vec::new();
    let mut valid_offset = start_offset;
    let validate_start_lsn = start_offset == SEGMENT_HEADER_LEN;
    let mut first_lsn = None;
    loop {
        if limit.is_some_and(|limit| entries.len() >= limit) {
            break;
        }

        let frame_start = valid_offset;
        let Some((record, next_offset)) = read_record_frame(&mut reader, path, frame_start)? else {
            break;
        };
        first_lsn.get_or_insert(record.lsn);
        entries.push(WalEntry {
            position: WalPosition {
                lsn: record.lsn,
                segment: segment_id,
                offset: frame_start,
            },
            next_position: WalPosition {
                lsn: record.lsn,
                segment: segment_id,
                offset: next_offset,
            },
            record,
        });
        valid_offset = next_offset;
    }

    if validate_start_lsn && first_lsn.is_some_and(|lsn| lsn != header.start_lsn) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal segment start_lsn mismatch: {}", path.display()),
        ));
    }

    Ok(entries)
}

fn serialize_frame(record: &WalRecord) -> io::Result<Vec<u8>> {
    maybe_fail_test_write()?;
    let payload = bitcode::serialize(record).map_err(io::Error::other)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "wal record payload is larger than u32::MAX",
        )
    })?;
    let node_id = record.node_id.as_bytes();
    let node_id_len = u16::try_from(node_id.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "wal record node_id is larger than u16::MAX",
        )
    })?;
    let header_len = u16::try_from(RECORD_HEADER_FIXED_LEN + node_id.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "wal record header is larger than u16::MAX",
        )
    })?;
    let payload_crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(header_len as usize + payload.len());
    frame.extend_from_slice(RECORD_MAGIC);
    frame.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    frame.extend_from_slice(&header_len.to_be_bytes());
    frame.push(RECORD_TYPE_DATA);
    frame.push(RECORD_FLAGS_NONE);
    frame.extend_from_slice(&[0_u8; 2]);
    frame.extend_from_slice(&record.lsn.to_be_bytes());
    frame.extend_from_slice(&record.received_at_ms.to_be_bytes());
    frame.extend_from_slice(&node_id_len.to_be_bytes());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(&payload_crc.to_be_bytes());
    frame.extend_from_slice(node_id);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

#[cfg(debug_assertions)]
pub fn fail_after_test_writes(writes: usize) {
    FAIL_AFTER_TEST_WRITES.with(|remaining| remaining.set(writes));
}

#[cfg(debug_assertions)]
fn maybe_fail_test_write() -> io::Result<()> {
    FAIL_AFTER_TEST_WRITES.with(|remaining| {
        let writes = remaining.get();
        if writes == usize::MAX {
            return Ok(());
        }
        if writes == 0 {
            return Err(io::Error::other("injected wal write failure"));
        }
        remaining.set(writes - 1);
        Ok(())
    })
}

#[cfg(not(debug_assertions))]
fn maybe_fail_test_write() -> io::Result<()> {
    Ok(())
}

fn ensure_segment_file(
    dir: &Path,
    segment_id: u64,
    node_id: &str,
    start_lsn: u64,
) -> io::Result<File> {
    let path = segment_path(dir, segment_id);
    if !path.exists() {
        return create_segment_file(dir, segment_id, node_id, start_lsn);
    }

    let mut file = OpenOptions::new().read(true).append(true).open(&path)?;
    file.seek(SeekFrom::Start(0))?;
    let header = read_segment_header(&mut file, &path)?;
    if header.segment_id != segment_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "wal segment id mismatch: expected {segment_id}, got {}: {}",
                header.segment_id,
                path.display()
            ),
        ));
    }
    if header.node_id != node_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal segment node_id mismatch: {}", path.display()),
        ));
    }
    if file.metadata()?.len() == SEGMENT_HEADER_LEN && header.start_lsn != start_lsn {
        drop(file);
        fs::remove_file(&path)?;
        sync_directory(dir)?;
        return create_segment_file(dir, segment_id, node_id, start_lsn);
    }
    file.seek(SeekFrom::End(0))?;

    Ok(file)
}

fn create_segment_file(
    dir: &Path,
    segment_id: u64,
    node_id: &str,
    start_lsn: u64,
) -> io::Result<File> {
    let path = segment_path(dir, segment_id);
    let tmp_path = segment_tmp_path(dir, segment_id);
    match fs::remove_file(&tmp_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&tmp_path)?;
    file.write_all(&serialize_segment_header(segment_id, node_id, start_lsn)?)?;
    file.sync_data()?;
    drop(file);
    fs::rename(&tmp_path, &path)?;
    sync_directory(dir)?;
    OpenOptions::new().read(true).append(true).open(&path)
}

fn repair_segment_tail(path: &Path) -> io::Result<u64> {
    let scan = scan_segment(path)?;
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(scan.valid_offset)?;
    file.sync_data()?;
    Ok(scan.valid_offset)
}

struct SegmentScan {
    records: Vec<WalRecord>,
    valid_offset: u64,
}

fn scan_segment(path: &Path) -> io::Result<SegmentScan> {
    let mut reader = BufReader::new(File::open(path)?);
    let header = read_segment_header(&mut reader, path)?;

    let mut records = Vec::new();
    let mut valid_offset = SEGMENT_HEADER_LEN;
    let segment_id = segment_id_from_path(path)?;
    let mut first_lsn = None;
    loop {
        let frame_start = valid_offset;
        let Some((record, next_offset)) = read_record_frame(&mut reader, path, frame_start)? else {
            break;
        };
        first_lsn.get_or_insert(record.lsn);
        let entry = WalEntry {
            position: WalPosition {
                lsn: record.lsn,
                segment: segment_id,
                offset: frame_start,
            },
            next_position: WalPosition {
                lsn: record.lsn,
                segment: segment_id,
                offset: next_offset,
            },
            record,
        };
        records.push(entry.record.clone());
        valid_offset = next_offset;
    }

    if first_lsn.is_some_and(|lsn| lsn != header.start_lsn) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal segment start_lsn mismatch: {}", path.display()),
        ));
    }

    Ok(SegmentScan {
        records,
        valid_offset,
    })
}

fn read_record_frame(
    reader: &mut impl Read,
    path: &Path,
    frame_start: u64,
) -> io::Result<Option<(WalRecord, u64)>> {
    let mut magic = [0_u8; 8];
    match reader.read_exact(&mut magic) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }

    if &magic != RECORD_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal record magic: {}", path.display()),
        ));
    }

    let mut fixed_header = [0_u8; RECORD_HEADER_FIXED_LEN - 8];
    match reader.read_exact(&mut fixed_header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }

    let version = u16::from_be_bytes(fixed_header[0..2].try_into().expect("version bytes"));
    if version != RECORD_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported wal record version {version}: {}",
                path.display()
            ),
        ));
    }
    let header_len = u16::from_be_bytes(
        fixed_header[2..4]
            .try_into()
            .expect("record header length bytes"),
    ) as usize;
    if header_len < RECORD_HEADER_FIXED_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal record header length: {}", path.display()),
        ));
    }
    let record_type = fixed_header[4];
    if record_type != RECORD_TYPE_DATA {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported wal record type {record_type}: {}",
                path.display()
            ),
        ));
    }
    let flags = fixed_header[5];
    if flags != RECORD_FLAGS_NONE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported wal record flags {flags}: {}", path.display()),
        ));
    }
    let lsn = u64::from_be_bytes(fixed_header[8..16].try_into().expect("lsn bytes"));
    let received_at_ms = u64::from_be_bytes(
        fixed_header[16..24]
            .try_into()
            .expect("received_at_ms bytes"),
    );
    let node_id_len = u16::from_be_bytes(
        fixed_header[24..26]
            .try_into()
            .expect("node_id length bytes"),
    ) as usize;
    let expected_header_len = RECORD_HEADER_FIXED_LEN + node_id_len;
    if header_len != expected_header_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal record header length mismatch: {}", path.display()),
        ));
    }
    let payload_len = u32::from_be_bytes(
        fixed_header[26..30]
            .try_into()
            .expect("payload length bytes"),
    ) as usize;
    let expected_crc =
        u32::from_be_bytes(fixed_header[30..34].try_into().expect("payload crc bytes"));

    let mut node_id_bytes = vec![0; node_id_len];
    match reader.read_exact(&mut node_id_bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let node_id = String::from_utf8(node_id_bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal record node_id: {error}"),
        )
    })?;

    let mut payload = vec![0; payload_len];
    match reader.read_exact(&mut payload) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let actual_crc = crc32fast::hash(&payload);
    if actual_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal frame crc mismatch: {}", path.display()),
        ));
    }

    let record = bitcode::deserialize::<WalRecord>(&payload).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal record payload: {error}"),
        )
    })?;
    if record.lsn != lsn {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal record lsn mismatch: {}", path.display()),
        ));
    }
    if record.received_at_ms != received_at_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal record timestamp mismatch: {}", path.display()),
        ));
    }
    if record.node_id != node_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal record node_id mismatch: {}", path.display()),
        ));
    }
    Ok(Some((
        record,
        frame_start + header_len as u64 + payload_len as u64,
    )))
}

#[derive(Debug)]
struct SegmentHeader {
    segment_id: u64,
    start_lsn: u64,
    node_id: String,
}

fn read_segment_header(reader: &mut impl Read, path: &Path) -> io::Result<SegmentHeader> {
    let mut header = [0_u8; SEGMENT_HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    if &header[0..8] != SEGMENT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal segment identifier: {}", path.display()),
        ));
    }
    let version = u16::from_be_bytes(header[8..10].try_into().expect("segment version bytes"));
    if version != SEGMENT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported wal segment version {version}: {}",
                path.display()
            ),
        ));
    }
    let header_len =
        u16::from_be_bytes(header[10..12].try_into().expect("segment header len bytes")) as u64;
    if header_len != SEGMENT_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal segment header length: {}", path.display()),
        ));
    }
    let expected_header_crc = u32::from_be_bytes(
        header[SEGMENT_HEADER_CRC_OFFSET..SEGMENT_HEADER_CRC_OFFSET + 4]
            .try_into()
            .expect("segment header crc bytes"),
    );
    let actual_header_crc = compute_segment_header_crc(&header);
    if actual_header_crc != expected_header_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal segment header crc mismatch: {}", path.display()),
        ));
    }
    let segment_id = u64::from_be_bytes(header[12..20].try_into().expect("segment id bytes"));
    let start_lsn = u64::from_be_bytes(header[28..36].try_into().expect("segment start_lsn bytes"));
    let node_id_len = u16::from_be_bytes(
        header[36..38]
            .try_into()
            .expect("segment node_id len bytes"),
    ) as usize;
    let node_id_end = SEGMENT_NODE_ID_OFFSET + node_id_len;
    if node_id_end > SEGMENT_HEADER_CRC_OFFSET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal segment node_id length: {}", path.display()),
        ));
    }
    let node_id = String::from_utf8(header[SEGMENT_NODE_ID_OFFSET..node_id_end].to_vec()).map_err(
        |error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid wal segment node_id: {error}"),
            )
        },
    )?;
    Ok(SegmentHeader {
        segment_id,
        start_lsn,
        node_id,
    })
}

fn serialize_segment_header(segment_id: u64, node_id: &str, start_lsn: u64) -> io::Result<Vec<u8>> {
    let node_id = node_id.as_bytes();
    let max_node_id_len = SEGMENT_HEADER_CRC_OFFSET - SEGMENT_NODE_ID_OFFSET;
    if node_id.len() > max_node_id_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("wal segment node_id is longer than {max_node_id_len} bytes"),
        ));
    }
    let node_id_len = u16::try_from(node_id.len()).expect("bounded segment node_id length");
    let mut header = vec![0_u8; SEGMENT_HEADER_LEN as usize];
    header[0..8].copy_from_slice(SEGMENT_MAGIC);
    header[8..10].copy_from_slice(&SEGMENT_VERSION.to_be_bytes());
    header[10..12].copy_from_slice(&(SEGMENT_HEADER_LEN as u16).to_be_bytes());
    header[12..20].copy_from_slice(&segment_id.to_be_bytes());
    header[20..28].copy_from_slice(&now_ms().to_be_bytes());
    header[28..36].copy_from_slice(&start_lsn.to_be_bytes());
    header[36..38].copy_from_slice(&node_id_len.to_be_bytes());
    header[SEGMENT_NODE_ID_OFFSET..SEGMENT_NODE_ID_OFFSET + node_id.len()].copy_from_slice(node_id);
    let crc = compute_segment_header_crc(&header);
    header[SEGMENT_HEADER_CRC_OFFSET..SEGMENT_HEADER_CRC_OFFSET + 4]
        .copy_from_slice(&crc.to_be_bytes());
    Ok(header)
}

fn compute_segment_header_crc(header: &[u8]) -> u32 {
    crc32fast::hash(&header[..SEGMENT_HEADER_CRC_OFFSET])
}

fn assign_wal_metadata(record: &mut WalRecord, lsn: u64, node_id: &str) {
    record.lsn = lsn;
    record.node_id = node_id.to_string();
}

fn recover_active_segment_id(dir: &Path, checkpoint: Option<&WalCheckpoint>) -> io::Result<u64> {
    let last_segment_id = last_segment_id(dir)?;
    let checkpoint_next_segment = checkpoint.map(|checkpoint| checkpoint.checkpoint_segment_id + 1);
    Ok(last_segment_id
        .into_iter()
        .chain(checkpoint_next_segment)
        .max()
        .unwrap_or(FIRST_SEGMENT_ID))
}

fn recover_next_lsn(dir: &Path, checkpoint: Option<&WalCheckpoint>) -> io::Result<u64> {
    let mut max_lsn = 0;
    for segment_id in segment_ids(dir)? {
        for record in read_segment_records(&segment_path(dir, segment_id))? {
            max_lsn = max_lsn.max(record.lsn);
        }
    }
    if let Some(checkpoint) = checkpoint {
        max_lsn = max_lsn.max(checkpoint.checkpoint_lsn);
    }
    Ok(max_lsn + 1)
}

#[derive(Serialize, Deserialize)]
struct WalCheckpoint {
    version: u16,
    node_id: String,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
    updated_at: u64,
    checksum: u32,
}

#[derive(Serialize)]
struct WalCheckpointChecksum<'a> {
    version: u16,
    node_id: &'a str,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
    updated_at: u64,
}

fn read_checkpoint(dir: &Path, node_id: &str) -> io::Result<Option<WalCheckpoint>> {
    let path = dir.join(CHECKPOINT_FILE);
    if !path.exists() {
        return Ok(None);
    }

    let checkpoint = serde_json::from_slice::<WalCheckpoint>(&fs::read(&path)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if checkpoint.node_id != node_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checkpoint node_id mismatch: checkpoint={} current={}",
                checkpoint.node_id, node_id
            ),
        ));
    }
    let bytes = serde_json::to_vec(&WalCheckpointChecksum {
        version: checkpoint.version,
        node_id: checkpoint.node_id.as_str(),
        checkpoint_lsn: checkpoint.checkpoint_lsn,
        checkpoint_segment_id: checkpoint.checkpoint_segment_id,
        checkpoint_segment_offset: checkpoint.checkpoint_segment_offset,
        updated_at: checkpoint.updated_at,
    })
    .map_err(io::Error::other)?;
    let expected_checksum = crc32fast::hash(&bytes);
    if checkpoint.checksum != expected_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checkpoint checksum mismatch: {}", path.display()),
        ));
    }

    Ok(Some(checkpoint))
}

fn sync_directory(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn resolve_node_id(configured_node_id: Option<&str>, dir: &Path) -> io::Result<String> {
    if let Some(node_id) = configured_node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let path = dir.join(NODE_ID_FILE);
        if path.exists() {
            let existing = fs::read_to_string(&path)?;
            let existing = existing.trim();
            if existing != node_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "configured wal node_id '{node_id}' conflicts with persisted node_id '{existing}'"
                    ),
                ));
            }
        } else {
            write_node_id_file(dir, node_id)?;
        }
        return Ok(node_id.to_string());
    }

    let path = dir.join(NODE_ID_FILE);
    if path.exists() {
        let node_id = fs::read_to_string(&path)?;
        let node_id = node_id.trim();
        if !node_id.is_empty() {
            return Ok(node_id.to_string());
        }
    }

    let node_id = Uuid::new_v4().to_string();
    write_node_id_file(dir, &node_id)?;
    Ok(node_id)
}

fn write_node_id_file(dir: &Path, node_id: &str) -> io::Result<()> {
    let tmp_path = dir.join(format!("{NODE_ID_FILE}.tmp"));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(node_id.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_data()?;
    }
    fs::rename(&tmp_path, dir.join(NODE_ID_FILE))?;
    sync_directory(dir)?;
    Ok(())
}

fn segment_ids(dir: &Path) -> io::Result<Vec<u64>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let Ok(id) = stem.parse::<u64>() else {
            continue;
        };
        ids.push(id);
    }
    Ok(ids)
}

fn last_segment_id(dir: &Path) -> io::Result<Option<u64>> {
    Ok(segment_ids(dir)?.into_iter().max())
}

fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:016}.{SEGMENT_EXTENSION}"))
}

fn segment_tmp_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:016}.{SEGMENT_EXTENSION}.tmp"))
}

fn segment_id_from_path(path: &Path) -> io::Result<u64> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid wal segment path: {}", path.display()),
            )
        })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}
