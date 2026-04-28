use crate::current_timestamp_as_u64;
use crate::event::Event;
use crate::utils::get_ip;
use crate::utils::get_process_info;
use actix_web::HttpRequest;
use log::warn;
use serde_json::{Map, Value};

pub fn normalize_ingest_event(event: &mut Event, req: &HttpRequest) -> anyhow::Result<()> {
    if event.xwhen().is_none() {
        event.set_xwhen(current_timestamp_as_u64());
    }

    let xcontext = event.xcontext_mut();

    normalize_os(xcontext);
    normalize_currency_type(xcontext);
    fill_platform(xcontext);

    if !xcontext.contains_key("ip") || xcontext["ip"].is_null() {
        if let Some(ip) = get_ip(req) {
            xcontext.insert("ip".to_string(), Value::String(ip));
        }
    }

    match serde_json::to_value(get_process_info()) {
        Ok(process_info) => {
            xcontext.insert("process_info".to_string(), process_info);
        }
        Err(err) => {
            warn!("Failed to serialize process info: {err}");
        }
    }

    Ok(())
}

fn normalize_os(xcontext: &mut Map<String, Value>) {
    if let Some(os) = xcontext.get("os").and_then(Value::as_str) {
        let lower = os.to_lowercase();
        xcontext.insert("os".to_string(), Value::String(lower));
    }
}

fn normalize_currency_type(xcontext: &mut Map<String, Value>) {
    if let Some(currency_type) = xcontext.get("currencytype").and_then(Value::as_str) {
        xcontext.insert(
            "currencytype".to_string(),
            Value::String(currency_type.to_uppercase()),
        );
    }
}

fn fill_platform(xcontext: &mut Map<String, Value>) {
    let needs_platform = !xcontext.contains_key("platform") || xcontext["platform"].is_null();
    if !needs_platform {
        return;
    }

    let platform = match xcontext.get("os").and_then(Value::as_str) {
        Some("ios") => Some("ios"),
        Some("android") => Some("android"),
        Some("harmony") => Some("harmony"),
        Some("wechat") => Some("wechat"),
        Some("toutiao") => Some("toutiao"),
        Some("tiktok") => Some("tiktok"),
        _ => None,
    };

    if let Some(platform) = platform {
        xcontext.insert("platform".to_string(), Value::String(platform.to_string()));
    }
}
