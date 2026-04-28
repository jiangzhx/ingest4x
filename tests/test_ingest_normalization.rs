#![cfg(feature = "ingest")]

use actix_web::test::TestRequest;
use ingest4x::event::Event;
use ingest4x::ingest::normalize::normalize_ingest_event;
use serde_json::{json, Value};
use std::net::SocketAddr;

#[test]
fn normalize_ingest_event_fills_and_normalizes_expected_fields() {
    let mut event = Event::from_value(json!({
        "appid": "APPID",
        "xwhat": "payment",
        "xwho": "user-1",
        "xwhen": null,
        "xcontext": {
            "os": "iOS",
            "installid": "install-1",
            "currencytype": "cny",
            "idfa": "idfa-1"
        }
    }))
    .unwrap();

    let req = TestRequest::default()
        .peer_addr("8.8.8.8:8080".parse::<SocketAddr>().unwrap())
        .to_http_request();

    normalize_ingest_event(&mut event, &req).unwrap();

    assert!(event.xwhen().is_some());
    assert_eq!(
        event.xcontext().get("os"),
        Some(&Value::String("ios".to_string()))
    );
    assert_eq!(
        event.xcontext().get("currencytype"),
        Some(&Value::String("CNY".to_string()))
    );
    assert_eq!(
        event.xcontext().get("platform"),
        Some(&Value::String("ios".to_string()))
    );
    assert_eq!(
        event.xcontext().get("ip"),
        Some(&Value::String("8.8.8.8".to_string()))
    );
    assert!(event.xcontext().contains_key("process_info"));
}
