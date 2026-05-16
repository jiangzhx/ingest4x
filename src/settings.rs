use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Settings {
    pub ingest: IngestSettings,
    #[serde(default)]
    pub logging: LoggingSettings,
    pub management: ManagementSettings,
    #[serde(default)]
    pub database: Option<DatabaseSettings>,
    pub wal: WalSettings,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct IngestSettings {
    pub bind_address: String,
    #[serde(default = "default_max_event_bytes")]
    pub max_event_bytes: usize,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct LoggingSettings {
    #[serde(default)]
    pub level: LogLevel,
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingSettings {
    fn default() -> Self {
        Self {
            level: LogLevel::default(),
            format: default_log_format(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct ManagementSettings {
    pub bind_address: String,
    #[serde(default)]
    pub admin_password: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    Earliest,
    #[default]
    Latest,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct DatabaseSettings {
    pub url: String,
    #[serde(default = "default_database_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct WalSettings {
    pub dir: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub write: WalWriteSettings,
    #[serde(default)]
    pub checkpoint: CheckpointSettings,
    #[serde(default)]
    pub replay: ReplaySettings,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct WalWriteSettings {
    #[serde(default = "default_wal_write_flush_interval")]
    pub flush_interval: String,
    #[serde(default = "default_wal_write_flush_records")]
    pub flush_records: usize,
    #[serde(default)]
    pub no_sync: bool,
    #[serde(default = "default_wal_write_segment_max_bytes")]
    pub segment_max_bytes: u64,
    #[serde(default)]
    pub min_free_bytes: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct CheckpointSettings {
    #[serde(default = "default_checkpoint_flush_interval")]
    pub flush_interval: String,
    #[serde(default = "default_checkpoint_flush_records")]
    pub flush_records: usize,
    #[serde(default = "default_checkpoint_flush_bytes")]
    pub flush_bytes: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct ReplaySettings {
    #[serde(default = "default_replay_max_records")]
    pub max_records: usize,
    #[serde(default = "default_replay_max_bytes")]
    pub max_bytes: u64,
}

impl Default for WalWriteSettings {
    fn default() -> Self {
        Self {
            flush_interval: default_wal_write_flush_interval(),
            flush_records: default_wal_write_flush_records(),
            no_sync: false,
            segment_max_bytes: default_wal_write_segment_max_bytes(),
            min_free_bytes: 0,
        }
    }
}

impl Default for CheckpointSettings {
    fn default() -> Self {
        Self {
            flush_interval: default_checkpoint_flush_interval(),
            flush_records: default_checkpoint_flush_records(),
            flush_bytes: default_checkpoint_flush_bytes(),
        }
    }
}

impl Default for ReplaySettings {
    fn default() -> Self {
        Self {
            max_records: default_replay_max_records(),
            max_bytes: default_replay_max_bytes(),
        }
    }
}

pub const fn default_database_refresh_interval_secs() -> u64 {
    3
}

pub const fn default_wal_write_segment_max_bytes() -> u64 {
    128 * 1024 * 1024
}

pub fn default_wal_write_flush_interval() -> String {
    "10ms".to_string()
}

pub const fn default_wal_write_flush_records() -> usize {
    1000
}

pub fn default_checkpoint_flush_interval() -> String {
    "1s".to_string()
}

pub const fn default_checkpoint_flush_records() -> usize {
    1000
}

pub const fn default_checkpoint_flush_bytes() -> u64 {
    64 * 1024 * 1024
}

pub const fn default_replay_max_records() -> usize {
    1000
}

pub const fn default_replay_max_bytes() -> u64 {
    64 * 1024 * 1024
}

pub const fn default_processor_max_operations() -> u64 {
    10_000
}

pub const fn default_max_event_bytes() -> usize {
    256 * 1024
}

pub fn default_log_format() -> String {
    "json".to_string()
}

impl Settings {
    pub fn new(config: Option<String>) -> Result<Self, ConfigError> {
        match config {
            None => Self::init(),
            Some(file) => Self::init_with_file(file.as_str()),
        }
    }

    pub fn init() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("ingest4x.toml"))
            .build()?
            .try_deserialize()
    }

    pub fn init_with_file(config_file: &str) -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name(config_file))
            .build()?
            .try_deserialize()
    }
}
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
    Off,
}

impl LogLevel {
    pub const fn as_tracing_level(self) -> Option<tracing::Level> {
        match self {
            Self::Error => Some(tracing::Level::ERROR),
            Self::Warn => Some(tracing::Level::WARN),
            Self::Info => Some(tracing::Level::INFO),
            Self::Debug => Some(tracing::Level::DEBUG),
            Self::Trace => Some(tracing::Level::TRACE),
            Self::Off => None,
        }
    }
}
