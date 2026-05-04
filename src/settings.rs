use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct Settings {
    pub server: ServerSettings,
    #[serde(alias = "metrics")]
    pub management: ManagementSettings,
    #[serde(default)]
    pub database: Option<DatabaseSettings>,
    #[serde(default)]
    pub wal: Option<WalSettings>,
    #[serde(default)]
    pub events: EventsSettings,
    pub redis: Option<RedisSettings>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct ServerSettings {
    pub bind_address: String,
    #[serde(default)]
    pub log_level: LogLevel,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_max_event_bytes")]
    pub max_event_bytes: usize,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct ManagementSettings {
    pub bind_address: String,
    #[serde(default)]
    pub admin_password: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct EventsSettings {
    #[serde(default)]
    pub sink: HashMap<String, EventSinkConfig>,
    #[serde(default)]
    pub valid: EventRouteSet,
    #[serde(default)]
    pub invalid: EventRouteSet,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventSinkConfig {
    Kafka {
        bootstrap_servers: String,
        topic: String,
        #[serde(default = "default_kafka_delivery_timeout_ms")]
        delivery_timeout_ms: String,
        #[serde(default = "default_kafka_queue_buffering_max_ms")]
        queue_buffering_max_ms: String,
        #[serde(default = "default_kafka_batch_num_messages")]
        batch_num_messages: String,
        #[serde(default = "default_kafka_queue_buffering_max_messages")]
        queue_buffering_max_messages: String,
        #[serde(default = "default_kafka_linger_ms")]
        linger_ms: String,
    },
    Stdout,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct EventRouteSet {
    #[serde(default)]
    pub routes: Vec<EventRouteSettings>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct EventRouteSettings {
    pub appid: Option<Vec<String>>,
    pub xwhat: Option<Vec<String>>,
    #[serde(default)]
    pub sinks: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct RedisSettings {
    pub address: String,
    pub connections_max_size: u32,
    pub connections_min_size: Option<u32>,
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
    #[serde(default = "default_wal_flush_interval")]
    pub wal_flush_interval: String,
    #[serde(default = "default_wal_max_write_buffer_size")]
    pub wal_max_write_buffer_size: usize,
    #[serde(default)]
    pub no_sync: bool,
    #[serde(default = "default_wal_segment_max_bytes")]
    pub wal_segment_max_bytes: u64,
    #[serde(default)]
    pub min_free_bytes: u64,
}

pub fn default_kafka_delivery_timeout_ms() -> String {
    "3000".to_string()
}

pub fn default_kafka_queue_buffering_max_ms() -> String {
    "0".to_string()
}

pub fn default_kafka_batch_num_messages() -> String {
    "100".to_string()
}

pub fn default_kafka_queue_buffering_max_messages() -> String {
    "300".to_string()
}

pub fn default_kafka_linger_ms() -> String {
    "100".to_string()
}

pub const fn default_database_refresh_interval_secs() -> u64 {
    3
}

pub const fn default_wal_segment_max_bytes() -> u64 {
    128 * 1024 * 1024
}

pub fn default_wal_flush_interval() -> String {
    "1s".to_string()
}

pub const fn default_wal_max_write_buffer_size() -> usize {
    100_000
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
