use crate::current_timestamp_as_u64;
use actix_web::HttpRequest;
use local_ip_address::local_ip;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::IpAddr;

pub mod events;
pub mod kafka;
pub mod prometheus;

pub fn insert_ip(req: &HttpRequest, xcontext: &mut HashMap<String, Value>) {
    if let Some(addr) = req.peer_addr() {
        match addr.ip() {
            IpAddr::V4(ip) => xcontext.insert("ip".to_string(), Value::String(ip.to_string())),
            IpAddr::V6(ip) => xcontext.insert("ip".to_string(), Value::String(ip.to_string())),
        };
    };
}

pub fn get_ip(req: &HttpRequest) -> Option<String> {
    if let Some(addr) = req.peer_addr() {
        match addr.ip() {
            IpAddr::V4(ip) => Some(ip.to_string()),
            IpAddr::V6(ip) => Some(ip.to_string()),
        }
    } else {
        Some("".to_string())
    }
}

pub fn get_process_info() -> HashMap<String, Value> {
    let mut process_info: HashMap<String, Value> = HashMap::new();
    process_info.insert(
        "receive_host_ip".to_string(),
        Value::String(local_ip().unwrap().to_string()),
    );
    process_info.insert(
        "receive_time".to_string(),
        json!(current_timestamp_as_u64()),
    );
    process_info.insert(
        "ingest4x_version".to_string(),
        Value::String(env!("CARGO_PKG_VERSION").to_string()),
    );
    process_info
}

pub fn get_host_ip() -> String {
    local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| String::new())
}
