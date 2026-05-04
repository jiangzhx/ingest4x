use crate::settings::WalSettings;
use serde::{Deserialize, Serialize};
#[cfg(debug_assertions)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::error;

const FILE_TYPE_IDENTIFIER: &[u8] = b"i4x.001";
const SEGMENT_HEADER_LEN: u64 = FILE_TYPE_IDENTIFIER.len() as u64 + 4;
const SEGMENT_EXTENSION: &str = "wal";
const FIRST_SEGMENT_ID: u64 = 1;

static RECORD_SEQUENCE: AtomicU64 = AtomicU64::new(1);
#[cfg(debug_assertions)]
thread_local! {
    static FAIL_AFTER_TEST_WRITES: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub record_id: String,
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
    pub segment: u64,
    pub offset: u64,
}

#[derive(Debug)]
pub struct WalWriter {
    inner: Arc<WalWriterInner>,
}

#[derive(Debug)]
struct WalWriterInner {
    dir: PathBuf,
    segment_max_bytes: u64,
    wal_flush_interval: Duration,
    wal_max_write_buffer_size: usize,
    no_sync: bool,
    state: Mutex<WalWriterState>,
}

#[derive(Debug)]
struct WalWriterState {
    segment_id: u64,
    offset: u64,
    buffered: Vec<WalRecord>,
}

impl WalWriter {
    pub fn new(settings: &WalSettings) -> io::Result<Self> {
        let dir = PathBuf::from(&settings.dir);
        fs::create_dir_all(&dir)?;
        let segment_id = last_segment_id(&dir)?.unwrap_or(FIRST_SEGMENT_ID);
        ensure_segment_file(&dir, segment_id)?;
        let offset = repair_segment_tail(&segment_path(&dir, segment_id))?;
        let wal_flush_interval =
            humantime::parse_duration(&settings.wal_flush_interval).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid wal_flush_interval: {error}"),
                )
            })?;
        let wal_flush_interval = wal_flush_interval.max(Duration::from_millis(1));

        let inner = Arc::new(WalWriterInner {
            dir,
            segment_max_bytes: settings.wal_segment_max_bytes.max(SEGMENT_HEADER_LEN + 1),
            wal_flush_interval,
            wal_max_write_buffer_size: settings.wal_max_write_buffer_size.max(1),
            no_sync: settings.no_sync,
            state: Mutex::new(WalWriterState {
                segment_id,
                offset,
                buffered: Vec::new(),
            }),
        });

        if inner.no_sync {
            spawn_flush_loop(Arc::downgrade(&inner));
        }

        Ok(Self { inner })
    }

    pub fn append(&self, record: &WalRecord) -> io::Result<WalPosition> {
        self.inner.append(record)
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
            self.append_buffered(record)
        } else {
            self.append_persisted(record)
        }
    }

    fn append_buffered(&self, record: &WalRecord) -> io::Result<WalPosition> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("wal writer mutex poisoned"))?;
        let position = WalPosition {
            segment: state.segment_id,
            offset: state.offset,
        };
        state.buffered.push(record.clone());

        if state.buffered.len() >= self.wal_max_write_buffer_size {
            self.flush_buffer_locked(&mut state)?;
        }

        Ok(position)
    }

    fn append_persisted(&self, record: &WalRecord) -> io::Result<WalPosition> {
        let bytes = serialize_frame(record)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("wal writer mutex poisoned"))?;

        self.append_frame_locked(&mut state, &bytes, true)
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
        let flush_result = (|| {
            let mut touched_segments = BTreeSet::new();
            for record in &buffered {
                let bytes = serialize_frame(record)?;
                let position = self.append_frame_locked(state, &bytes, false)?;
                touched_segments.insert(position.segment);
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
            state.buffered = buffered;
            return Err(error);
        }

        Ok(())
    }

    fn append_frame_locked(
        &self,
        state: &mut WalWriterState,
        bytes: &[u8],
        sync: bool,
    ) -> io::Result<WalPosition> {
        if state.offset > SEGMENT_HEADER_LEN
            && state.offset + bytes.len() as u64 > self.segment_max_bytes
        {
            state.segment_id += 1;
            state.offset = SEGMENT_HEADER_LEN;
            ensure_segment_file(&self.dir, state.segment_id)?;
        }

        let path = segment_path(&self.dir, state.segment_id);
        let mut file = OpenOptions::new().append(true).open(&path)?;
        let position = WalPosition {
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

fn spawn_flush_loop(inner: Weak<WalWriterInner>) {
    thread::Builder::new()
        .name("ingest4x-wal-flush".to_string())
        .spawn(move || {
            while let Some(inner) = inner.upgrade() {
                thread::sleep(inner.wal_flush_interval);
                if let Err(error) = inner.flush_buffer() {
                    error!(error = %error, "failed to flush wal buffer");
                }
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

fn read_segment_records(path: &Path) -> io::Result<Vec<WalRecord>> {
    scan_segment(path).map(|scan| scan.records)
}

fn serialize_frame(record: &WalRecord) -> io::Result<Vec<u8>> {
    maybe_fail_test_write()?;
    let payload = bitcode::serialize(record).map_err(io::Error::other)?;
    let len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "wal record payload is larger than u32::MAX",
        )
    })?;
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&crc.to_be_bytes());
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

fn ensure_segment_file(dir: &Path, segment_id: u64) -> io::Result<File> {
    let path = segment_path(dir, segment_id);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)?;

    if file.metadata()?.len() == 0 {
        file.write_all(FILE_TYPE_IDENTIFIER)?;
        file.write_all(&compute_header_crc().to_be_bytes())?;
        file.sync_data()?;
        sync_directory(dir)?;
    } else {
        let mut identifier = vec![0; FILE_TYPE_IDENTIFIER.len()];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut identifier)?;
        if identifier != FILE_TYPE_IDENTIFIER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid wal segment identifier: {}", path.display()),
            ));
        }
        let mut header_crc = [0_u8; 4];
        file.read_exact(&mut header_crc)?;
        let expected_header_crc = u32::from_be_bytes(header_crc);
        let actual_header_crc = compute_header_crc();
        if actual_header_crc != expected_header_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("wal segment header crc mismatch: {}", path.display()),
            ));
        }
        file.seek(SeekFrom::End(0))?;
    }

    Ok(file)
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
    verify_segment_header(&mut reader, path)?;

    let mut records = Vec::new();
    let mut valid_offset = SEGMENT_HEADER_LEN;
    loop {
        let frame_start = valid_offset;
        let mut header = [0_u8; 8];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }

        let len = u32::from_be_bytes(header[0..4].try_into().expect("length bytes")) as usize;
        let expected_crc = u32::from_be_bytes(header[4..8].try_into().expect("crc bytes"));
        let mut payload = vec![0; len];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
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
        records.push(record);
        valid_offset = frame_start + 8 + len as u64;
    }

    Ok(SegmentScan {
        records,
        valid_offset,
    })
}

fn verify_segment_header(reader: &mut impl Read, path: &Path) -> io::Result<()> {
    let mut identifier = vec![0; FILE_TYPE_IDENTIFIER.len()];
    reader.read_exact(&mut identifier)?;
    if identifier != FILE_TYPE_IDENTIFIER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid wal segment identifier: {}", path.display()),
        ));
    }
    let mut header_crc = [0_u8; 4];
    reader.read_exact(&mut header_crc)?;
    let expected_header_crc = u32::from_be_bytes(header_crc);
    let actual_header_crc = compute_header_crc();
    if actual_header_crc != expected_header_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal segment header crc mismatch: {}", path.display()),
        ));
    }
    Ok(())
}

fn compute_header_crc() -> u32 {
    crc32fast::hash(FILE_TYPE_IDENTIFIER)
}

fn sync_directory(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
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
    dir.join(format!("{segment_id:020}.{SEGMENT_EXTENSION}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}
