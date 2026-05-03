pub mod admin;
pub mod admin_ui;
pub mod db;
pub mod errors;
pub mod event;
pub mod ingest;
pub mod jlt;
pub mod logging;
pub mod projects;
pub mod rhai_ctx;
pub mod rules;
pub mod server;
pub mod settings;
pub mod utils;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp_as_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
