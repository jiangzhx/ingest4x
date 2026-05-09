use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{RegisterServiceNodeInput, ServiceNodeRepository, ServiceNodeStatus};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn service_node_registration_upserts_current_node_and_refreshes_heartbeat() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ServiceNodeRepository::new(db);

    let first = repo
        .register_service_node(RegisterServiceNodeInput {
            node_id: "node-a".to_string(),
            hostname: Some("ingest-a".to_string()),
            machine_ip: Some("10.0.0.1".to_string()),
            ingest_bind_address: "0.0.0.0:8090".to_string(),
            management_bind_address: "127.0.0.1:18090".to_string(),
            version: "0.0.1".to_string(),
            status: ServiceNodeStatus::Running,
            metadata_json: Some(json!({"zone": "az-a"})),
        })
        .await
        .expect("service node should register");

    assert_eq!(first.node_id, "node-a");
    assert_eq!(first.status, ServiceNodeStatus::Running);
    assert_eq!(first.metadata_json, Some(json!({"zone": "az-a"})));

    tokio::time::sleep(Duration::from_millis(2)).await;

    let second = repo
        .register_service_node(RegisterServiceNodeInput {
            node_id: "node-a".to_string(),
            hostname: Some("ingest-a-renamed".to_string()),
            machine_ip: Some("10.0.0.2".to_string()),
            ingest_bind_address: "0.0.0.0:8091".to_string(),
            management_bind_address: "127.0.0.1:18091".to_string(),
            version: "0.0.2".to_string(),
            status: ServiceNodeStatus::Running,
            metadata_json: None,
        })
        .await
        .expect("service node should upsert");

    assert_eq!(second.node_id, "node-a");
    assert_eq!(second.hostname.as_deref(), Some("ingest-a-renamed"));
    assert_eq!(second.machine_ip.as_deref(), Some("10.0.0.2"));
    assert_eq!(second.ingest_bind_address, "0.0.0.0:8091");
    assert_eq!(second.management_bind_address, "127.0.0.1:18091");
    assert_eq!(second.version, "0.0.2");
    assert!(second.started_at >= first.started_at);
    assert!(second.last_seen_at >= first.last_seen_at);

    tokio::time::sleep(Duration::from_millis(2)).await;

    let refreshed = repo
        .mark_service_node_seen("node-a")
        .await
        .expect("service node heartbeat should refresh")
        .expect("service node should still exist");

    assert_eq!(refreshed.status, ServiceNodeStatus::Running);
    assert!(refreshed.last_seen_at >= second.last_seen_at);

    let nodes = repo
        .list_service_nodes()
        .await
        .expect("service nodes should list");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], refreshed);
}
