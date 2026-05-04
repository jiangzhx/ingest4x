#![cfg(feature = "ingest")]

use actix_http::StatusCode;
use actix_web::{test, App};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ingest4x::server;
use ingest4x::settings::{Settings, WalSettings};
use ingest4x::wal::{
    new_record, read_all_records, read_entries_after_limit, WalPosition, WalWriter,
};
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

#[actix_rt::test]
async fn post_ingest_writes_raw_request_to_wal_when_configured() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .insert_header(("x-test-header", "kept"))
        .set_payload(serde_json::to_vec(&payload).expect("serialize payload"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body = test::read_body(resp).await;
    assert_eq!(std::str::from_utf8(body.as_ref()).unwrap(), "200");

    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.method, "POST");
    assert_eq!(record.path, "/ingest");
    assert_eq!(
        record.headers.get("x-test-header").map(String::as_str),
        Some("kept")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&record.body).expect("raw json body"),
        payload
    );
    assert!(record.record_id.starts_with("wal-"));
    assert!(record.received_at_ms > 0);
}

#[actix_rt::test]
async fn get_ingest_writes_decoded_payload_to_wal_when_configured() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-2",
            "os": "android",
            "oaid": "oaid-1"
        }
    });
    let encoded = STANDARD.encode(serde_json::to_vec(&payload).expect("serialize payload"));
    let query = serde_urlencoded::to_string([("data", encoded.as_str())]).expect("encode query");

    let req = test::TestRequest::get()
        .uri(format!("/ingest?{query}").as_str())
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.method, "GET");
    assert_eq!(record.path, "/ingest");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&record.body).expect("raw json body"),
        payload
    );
}

#[actix_rt::test]
async fn post_ingest_rejects_invalid_json_before_wal_append() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_payload("{not-json")
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(read_all_records(&wal_dir)
        .expect("read wal records")
        .is_empty());
}

#[actix_rt::test]
async fn post_ingest_rejects_payload_over_server_max_event_bytes_before_wal_append() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"
max_event_bytes = 128

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-too-large",
            "os": "ios",
            "idfa": "idfa-too-large",
            "extra": "x".repeat(160)
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_payload(serde_json::to_vec(&payload).expect("serialize payload"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(read_all_records(&wal_dir)
        .expect("read wal records")
        .is_empty());
}

#[actix_rt::test]
async fn get_ingest_rejects_decoded_payload_over_server_max_event_bytes_before_wal_append() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"
max_event_bytes = 128

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-get-too-large",
            "os": "ios",
            "idfa": "idfa-get-too-large",
            "extra": "x".repeat(160)
        }
    });
    let encoded = STANDARD.encode(serde_json::to_vec(&payload).expect("serialize payload"));
    let query = serde_urlencoded::to_string([("data", encoded.as_str())]).expect("encode query");

    let req = test::TestRequest::get()
        .uri(format!("/ingest?{query}").as_str())
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(read_all_records(&wal_dir)
        .expect("read wal records")
        .is_empty());
}

#[actix_rt::test]
async fn post_ingest_rejects_unknown_project_before_wal_append() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "UNKNOWN",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-unknown",
            "os": "ios"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_payload(serde_json::to_vec(&payload).expect("serialize payload"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert!(read_all_records(&wal_dir)
        .expect("read wal records")
        .is_empty());
}

#[actix_rt::test]
async fn no_sync_buffers_until_wal_max_write_buffer_size_is_reached() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"
no_sync = true
wal_flush_interval = "1h"
wal_max_write_buffer_size = 2

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;

    for index in 1..=2 {
        let payload = json!({
            "appid": "APPID",
            "xwhat": "custom_event",
            "xcontext": {
                "installid": format!("iid-{index}"),
                "os": "ios",
                "idfa": format!("idfa-{index}")
            }
        });
        let req = test::TestRequest::post()
            .uri("/ingest")
            .set_payload(serde_json::to_vec(&payload).expect("serialize payload"))
            .insert_header(("content-type", "application/json"))
            .to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::OK);

        let records = read_all_records(&wal_dir).expect("read wal records");
        if index == 1 {
            assert!(records.is_empty());
        } else {
            assert_eq!(records.len(), 2);
        }
    }
}

#[actix_rt::test]
async fn wal_segment_uses_explicit_record_header_and_binary_record_payload() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let config_path = temp.path().join("wal-config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[server]
bind_address = "127.0.0.1:8090"

[management]
bind_address = "127.0.0.1:18090"

[wal]
dir = "{}"

[events.sink.kafka_valid]
type = "kafka"
bootstrap_servers = "127.0.0.1:65535"
topic = "unused-valid"

[[events.valid.routes]]
sinks = ["kafka_valid"]
"#,
            wal_dir.display()
        ),
    )
    .expect("write config");

    let settings = Arc::new(
        Settings::init_with_file(config_path.to_str().expect("config path"))
            .expect("settings should load"),
    );
    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await;
    let payload = json!({
        "appid": "APPID",
        "xwhat": "custom_event",
        "xcontext": {
            "installid": "iid-format",
            "os": "ios",
            "idfa": "idfa-format"
        }
    });

    let req = test::TestRequest::post()
        .uri("/ingest")
        .set_payload(serde_json::to_vec(&payload).expect("serialize payload"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let resp = test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = fs::read(wal_segment_path(&wal_dir)).expect("read wal segment");
    let identifier = b"i4x.seg\0";
    assert!(bytes.starts_with(identifier));

    let segment_header_len =
        u16::from_be_bytes(bytes[10..12].try_into().expect("segment header len")) as usize;
    assert_eq!(segment_header_len, 512);
    assert_eq!(
        u16::from_be_bytes(bytes[8..10].try_into().expect("segment version")),
        1
    );
    assert_eq!(
        u64::from_be_bytes(bytes[12..20].try_into().expect("segment id")),
        1
    );
    assert!(u64::from_be_bytes(bytes[20..28].try_into().expect("segment created_at")) > 0);
    assert_eq!(
        u64::from_be_bytes(bytes[28..36].try_into().expect("segment start_lsn")),
        1
    );
    let segment_node_id_len =
        u16::from_be_bytes(bytes[36..38].try_into().expect("segment node id len")) as usize;
    assert!(segment_node_id_len > 0);
    let segment_node_id = std::str::from_utf8(&bytes[38..38 + segment_node_id_len]).unwrap();
    assert_eq!(segment_node_id, wal_node_id(&wal_dir));
    let expected_segment_header_crc =
        u32::from_be_bytes(bytes[508..512].try_into().expect("segment header crc"));
    assert_eq!(crc32fast::hash(&bytes[..508]), expected_segment_header_crc);

    let frame_offset = segment_header_len;
    assert_eq!(&bytes[frame_offset..frame_offset + 8], b"i4x.rec\0");
    assert_eq!(
        u16::from_be_bytes(
            bytes[frame_offset + 8..frame_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    let record_header_len = u16::from_be_bytes(
        bytes[frame_offset + 10..frame_offset + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(record_header_len > 42);
    assert_eq!(bytes[frame_offset + 12], 1);
    assert_eq!(bytes[frame_offset + 13], 0);
    let lsn = u64::from_be_bytes(
        bytes[frame_offset + 16..frame_offset + 24]
            .try_into()
            .unwrap(),
    );
    assert_eq!(lsn, 1);
    let received_at_ms = u64::from_be_bytes(
        bytes[frame_offset + 24..frame_offset + 32]
            .try_into()
            .unwrap(),
    );
    assert!(received_at_ms > 0);
    let node_id_len = u16::from_be_bytes(
        bytes[frame_offset + 32..frame_offset + 34]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(node_id_len > 0);
    assert_eq!(record_header_len, 42 + node_id_len);
    let payload_len = u32::from_be_bytes(
        bytes[frame_offset + 34..frame_offset + 38]
            .try_into()
            .unwrap(),
    ) as usize;
    let payload_crc = u32::from_be_bytes(
        bytes[frame_offset + 38..frame_offset + 42]
            .try_into()
            .unwrap(),
    );
    let node_id =
        std::str::from_utf8(&bytes[frame_offset + 42..frame_offset + record_header_len]).unwrap();
    assert_eq!(node_id, wal_node_id(&wal_dir));

    let payload_start = frame_offset + record_header_len;
    let payload_bytes = &bytes[payload_start..payload_start + payload_len];
    assert_eq!(crc32fast::hash(payload_bytes), payload_crc);
    assert_ne!(payload_bytes.first(), Some(&b'{'));
    assert!(!payload_bytes
        .windows(STANDARD.encode(serde_json::to_vec(&payload).unwrap()).len())
        .any(|window| window
            == STANDARD
                .encode(serde_json::to_vec(&payload).unwrap())
                .as_bytes()));
}

fn wal_segment_path(wal_dir: &Path) -> std::path::PathBuf {
    wal_dir.join("00000000000000000001.wal")
}

#[actix_rt::test]
async fn read_all_records_rejects_segment_header_start_lsn_mismatch() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    writer.append(&test_record("first")).expect("append record");
    drop(writer);

    rewrite_segment_start_lsn(&wal_segment_path(&wal_dir), 2);

    let error = read_all_records(&wal_dir).expect_err("segment start_lsn mismatch should fail");
    assert!(error.to_string().contains("wal segment start_lsn mismatch"));
}

#[actix_rt::test]
async fn read_all_records_ignores_trailing_partial_frame() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let record = test_record("complete");
    writer.append(&record).expect("append record");
    drop(writer);

    append_bytes(&wal_segment_path(&wal_dir), b"i4x.rec\0\x00\x01");

    let records = read_all_records(&wal_dir).expect("read wal records");
    let node_id = wal_node_id(&wal_dir);
    assert_eq!(records, vec![with_wal_metadata(record, 1, &node_id)]);
}

#[actix_rt::test]
async fn wal_writer_assigns_lsn_and_recovers_next_lsn_after_restart() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let first = test_record("first");
    let first_position = writer.append(&first).expect("append first");
    let second = test_record("second");
    let second_position = writer.append(&second).expect("append second");
    drop(writer);

    assert_eq!(first_position.lsn, 1);
    assert_eq!(second_position.lsn, 2);
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].lsn, 1);
    assert_eq!(records[1].lsn, 2);

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("restart wal writer");
    let third_position = writer.append(&test_record("third")).expect("append third");
    drop(writer);

    assert_eq!(third_position.lsn, 3);
    let records = read_all_records(&wal_dir).expect("read wal records after restart");
    assert_eq!(
        records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[actix_rt::test]
async fn wal_writer_recovers_next_lsn_from_checkpoint_when_segments_are_deleted() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join("node_id"), "checkpoint-node\n").expect("write node id");
    write_checkpoint(&wal_dir, "checkpoint-node", 7, 1, 512);

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let position = writer
        .append(&test_record("after-checkpoint"))
        .expect("append record after checkpoint");
    drop(writer);

    assert_eq!(position.lsn, 8);
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].lsn, 8);
}

#[actix_rt::test]
async fn wal_writer_creates_segment_after_checkpoint_when_segments_are_deleted() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join("node_id"), "checkpoint-node\n").expect("write node id");
    write_checkpoint(&wal_dir, "checkpoint-node", 7, 3, 2048);

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let position = writer
        .append(&test_record("after-deleted-segment"))
        .expect("append record after deleted segment");
    drop(writer);

    assert_eq!(position.segment, 4);
    assert!(wal_dir.join("00000000000000000004.wal").exists());
    assert!(!wal_dir.join("00000000000000000001.wal").exists());
}

#[actix_rt::test]
async fn wal_writer_recreates_empty_active_segment_with_stale_start_lsn() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join("node_id"), "checkpoint-node\n").expect("write node id");
    write_checkpoint(&wal_dir, "checkpoint-node", 7, 3, 2048);
    write_empty_segment(&wal_dir, 4, "checkpoint-node", 1);

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let position = writer
        .append(&test_record("after-stale-empty-segment"))
        .expect("append record after stale empty segment");
    drop(writer);

    assert_eq!(position.segment, 4);
    assert_eq!(position.lsn, 8);
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].lsn, 8);
}

#[actix_rt::test]
async fn wal_writer_rejects_second_writer_for_same_directory() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("first wal writer");

    let error = WalWriter::new(&wal_settings(&wal_dir)).expect_err("second writer should fail");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);

    drop(writer);
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("writer after lock release");
    drop(writer);
}

#[actix_rt::test]
async fn wal_writer_rejects_append_when_min_free_bytes_cannot_be_met() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let mut settings = wal_settings(&wal_dir);
    settings.min_free_bytes = u64::MAX;
    let writer = WalWriter::new(&settings).expect("wal writer");

    let error = writer
        .append(&test_record("oversized free space requirement"))
        .expect_err("append should fail when WAL min free bytes cannot be preserved");

    assert!(error.to_string().contains("wal disk space is insufficient"));
    assert!(read_all_records(&wal_dir)
        .expect("read wal records")
        .is_empty());
}

#[actix_rt::test]
async fn wal_writer_generates_and_reuses_persistent_node_id_when_unconfigured() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let first = test_record("first");
    writer.append(&first).expect("append first");
    drop(writer);

    let node_id_path = wal_dir.join("node_id");
    let node_id = fs::read_to_string(&node_id_path).expect("read node id");
    assert!(!node_id.trim().is_empty());
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].node_id, node_id.trim());

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("restart wal writer");
    writer
        .append(&test_record("second"))
        .expect("append second");
    drop(writer);

    let records = read_all_records(&wal_dir).expect("read wal records after restart");
    assert_eq!(records[1].node_id, node_id.trim());
}

#[actix_rt::test]
async fn wal_writer_persists_configured_node_id_and_rejects_conflict() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let mut settings = wal_settings(&wal_dir);
    settings.node_id = Some("configured-node".to_string());

    let writer = WalWriter::new(&settings).expect("wal writer");
    writer.append(&test_record("first")).expect("append first");
    drop(writer);

    assert_eq!(wal_node_id(&wal_dir), "configured-node");
    let records = read_all_records(&wal_dir).expect("read wal records");
    assert_eq!(records[0].node_id, "configured-node");

    settings.node_id = Some("different-node".to_string());
    let error = WalWriter::new(&settings).expect_err("node id conflict should fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[actix_rt::test]
async fn wal_segment_creation_removes_stale_tmp_before_rename() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    let stale_tmp = wal_dir.join("00000000000000000002.wal.tmp");
    fs::write(&stale_tmp, b"stale tmp").expect("write stale tmp");

    let mut settings = wal_settings(&wal_dir);
    settings.wal_segment_max_bytes = 16;
    let writer = WalWriter::new(&settings).expect("wal writer");
    writer.append(&test_record("first")).expect("append first");
    writer
        .append(&test_record("second"))
        .expect("append second");
    drop(writer);

    assert!(wal_dir.join("00000000000000000002.wal").exists());
    assert!(!stale_tmp.exists());
}

#[actix_rt::test]
async fn wal_writer_truncates_trailing_partial_frame_before_append() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let first = test_record("first");
    writer.append(&first).expect("append first");
    drop(writer);

    append_bytes(&wal_segment_path(&wal_dir), b"i4x.rec\0\x00\x01");

    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let second = test_record("second");
    writer.append(&second).expect("append second");
    drop(writer);

    let records = read_all_records(&wal_dir).expect("read wal records");
    let node_id = wal_node_id(&wal_dir);
    assert_eq!(
        records,
        vec![
            with_wal_metadata(first, 1, &node_id),
            with_wal_metadata(second, 2, &node_id)
        ]
    );
}

#[actix_rt::test]
async fn read_entries_after_limit_stops_before_scanning_extra_frames() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    let first = test_record("first");
    writer.append(&first).expect("append first");
    drop(writer);

    append_bytes(&wal_segment_path(&wal_dir), &[0, 0, 0, 1, 0, 0, 0, 0, 1]);

    let entries =
        read_entries_after_limit(&wal_dir, None, Some(1)).expect("read first limited entry");
    assert_eq!(entries.len(), 1);
    let node_id = wal_node_id(&wal_dir);
    assert_eq!(entries[0].record, with_wal_metadata(first, 1, &node_id));

    let err = read_entries_after_limit(&wal_dir, Some(entries[0].next_position), Some(1))
        .expect_err("next frame should still be corrupt");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[actix_rt::test]
async fn read_entries_after_rejects_checkpoint_offset_beyond_segment_file() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let writer = WalWriter::new(&wal_settings(&wal_dir)).expect("wal writer");
    writer.append(&test_record("first")).expect("append first");
    drop(writer);

    let segment_len = fs::metadata(wal_segment_path(&wal_dir))
        .expect("segment metadata")
        .len();
    let error = read_entries_after_limit(
        &wal_dir,
        Some(WalPosition {
            lsn: 1,
            segment: 1,
            offset: segment_len + 1,
        }),
        None,
    )
    .expect_err("checkpoint offset beyond segment file should fail");

    assert!(error.to_string().contains("invalid wal checkpoint offset"));
}

#[actix_rt::test]
async fn no_sync_flush_failure_rolls_back_written_batch() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let mut settings = wal_settings(&wal_dir);
    settings.no_sync = true;
    settings.wal_max_write_buffer_size = 2;
    settings.wal_flush_interval = "1h".to_string();

    let writer = WalWriter::new(&settings).expect("wal writer");
    ingest4x::wal::fail_after_test_writes(1);
    let first = test_record("first");
    writer.append(&first).expect("buffer first");
    let second = test_record("second");
    let err = writer.append(&second).expect_err("flush should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::Other);

    ingest4x::wal::fail_after_test_writes(usize::MAX);
    writer.flush().expect("flush retry");

    let records = read_all_records(&wal_dir).expect("read wal records");
    let node_id = wal_node_id(&wal_dir);
    assert_eq!(
        records,
        vec![
            with_wal_metadata(first, 1, &node_id),
            with_wal_metadata(second, 2, &node_id)
        ]
    );
}

#[actix_rt::test]
async fn no_sync_flush_failure_removes_segments_created_by_batch() {
    let temp = tempdir().expect("temp dir");
    let wal_dir = temp.path().join("wal");
    let mut settings = wal_settings(&wal_dir);
    settings.no_sync = true;
    settings.wal_max_write_buffer_size = 3;
    settings.wal_flush_interval = "1h".to_string();
    settings.wal_segment_max_bytes = 16;

    let writer = WalWriter::new(&settings).expect("wal writer");
    ingest4x::wal::fail_after_test_writes(2);
    let first = test_record("first");
    writer.append(&first).expect("buffer first");
    let second = test_record("second");
    writer.append(&second).expect("buffer second");
    let third = test_record("third");
    let err = writer.append(&third).expect_err("flush should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::Other);

    assert!(!wal_dir.join("00000000000000000002.wal").exists());

    ingest4x::wal::fail_after_test_writes(usize::MAX);
    writer.flush().expect("flush retry");

    let records = read_all_records(&wal_dir).expect("read wal records");
    let node_id = wal_node_id(&wal_dir);
    assert_eq!(
        records,
        vec![
            with_wal_metadata(first, 1, &node_id),
            with_wal_metadata(second, 2, &node_id),
            with_wal_metadata(third, 3, &node_id)
        ]
    );
}

fn wal_settings(wal_dir: &Path) -> WalSettings {
    ingest4x::wal::fail_after_test_writes(usize::MAX);
    WalSettings {
        dir: wal_dir.display().to_string(),
        node_id: None,
        wal_flush_interval: "1s".to_string(),
        wal_max_write_buffer_size: 100_000,
        no_sync: false,
        wal_segment_max_bytes: 128 * 1024 * 1024,
        min_free_bytes: 0,
    }
}

fn test_record(body: &str) -> ingest4x::wal::WalRecord {
    new_record(
        "POST",
        "/ingest",
        None,
        None,
        BTreeMap::new(),
        body.as_bytes().to_vec(),
    )
}

fn with_wal_metadata(
    mut record: ingest4x::wal::WalRecord,
    lsn: u64,
    node_id: &str,
) -> ingest4x::wal::WalRecord {
    record.lsn = lsn;
    record.node_id = node_id.to_string();
    record
}

fn wal_node_id(wal_dir: &Path) -> String {
    fs::read_to_string(wal_dir.join("node_id"))
        .expect("read node id")
        .trim()
        .to_string()
}

fn append_bytes(path: &Path, bytes: &[u8]) {
    OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open wal segment")
        .write_all(bytes)
        .expect("append bytes");
}

fn rewrite_segment_start_lsn(path: &Path, start_lsn: u64) {
    let mut bytes = fs::read(path).expect("read wal segment");
    bytes[28..36].copy_from_slice(&start_lsn.to_be_bytes());
    let crc = crc32fast::hash(&bytes[..508]);
    bytes[508..512].copy_from_slice(&crc.to_be_bytes());
    fs::write(path, bytes).expect("rewrite wal segment");
}

#[derive(Serialize)]
struct TestCheckpoint<'a> {
    version: u16,
    node_id: &'a str,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
    updated_at: u64,
    checksum: u32,
}

#[derive(Serialize)]
struct TestCheckpointChecksum<'a> {
    version: u16,
    node_id: &'a str,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
    updated_at: u64,
}

fn write_checkpoint(
    wal_dir: &Path,
    node_id: &str,
    checkpoint_lsn: u64,
    checkpoint_segment_id: u64,
    checkpoint_segment_offset: u64,
) {
    let updated_at = 1_777_877_000_000;
    let checksum = crc32fast::hash(
        &serde_json::to_vec(&TestCheckpointChecksum {
            version: 1,
            node_id,
            checkpoint_lsn,
            checkpoint_segment_id,
            checkpoint_segment_offset,
            updated_at,
        })
        .expect("serialize checkpoint checksum"),
    );
    fs::write(
        wal_dir.join("checkpoint.json"),
        serde_json::to_vec(&TestCheckpoint {
            version: 1,
            node_id,
            checkpoint_lsn,
            checkpoint_segment_id,
            checkpoint_segment_offset,
            updated_at,
            checksum,
        })
        .expect("serialize checkpoint"),
    )
    .expect("write checkpoint");
}

fn write_empty_segment(wal_dir: &Path, segment_id: u64, node_id: &str, start_lsn: u64) {
    let node_id = node_id.as_bytes();
    let mut header = vec![0_u8; 512];
    header[0..8].copy_from_slice(b"i4x.seg\0");
    header[8..10].copy_from_slice(&1_u16.to_be_bytes());
    header[10..12].copy_from_slice(&512_u16.to_be_bytes());
    header[12..20].copy_from_slice(&segment_id.to_be_bytes());
    header[20..28].copy_from_slice(&1_777_877_000_000_u64.to_be_bytes());
    header[28..36].copy_from_slice(&start_lsn.to_be_bytes());
    header[36..38].copy_from_slice(&(node_id.len() as u16).to_be_bytes());
    header[38..38 + node_id.len()].copy_from_slice(node_id);
    let crc = crc32fast::hash(&header[..508]);
    header[508..512].copy_from_slice(&crc.to_be_bytes());
    fs::write(wal_dir.join(format!("{segment_id:020}.wal")), header).expect("write empty segment");
}
