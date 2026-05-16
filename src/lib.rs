pub mod admin;
pub mod db;
pub mod entities;
pub mod event;
pub mod ingest;
pub mod logging;
pub mod repositories;
pub mod rhai_ctx;
pub mod routes;
pub mod server;
pub mod services;
pub mod settings;
pub mod sinks;
pub mod utils;
pub mod wal;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp_as_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
