use crate::support::mock_services::build_app_state_with_test_processor;
use actix_http::StatusCode;
use actix_web::{test, web, App, HttpResponse};
use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{CreateProjectInput, ProjectRepository, UpdateProjectInput};
use ingest4x::server;
use ingest4x::services::ProjectRegistryState;
use ingest4x::settings::{
    CheckpointSettings, DatabaseSettings, IngestSettings, ManagementSettings, Settings,
    WalSettings, WalWriteSettings,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";

#[tokio::test]
async fn load_only_keeps_enabled_projects_in_memory() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    repository
        .create_project(CreateProjectInput {
            name: "Enabled".to_string(),
            enabled: true,
            ingest_token: "igx_enabled_app".to_string(),
        })
        .await
        .expect("enabled project should be created");
    repository
        .create_project(CreateProjectInput {
            name: "Disabled".to_string(),
            enabled: false,
            ingest_token: "igx_disabled_app".to_string(),
        })
        .await
        .expect("disabled project should be created");

    let registry = ProjectRegistryState::load(repository)
        .await
        .expect("registry should load");

    assert!(registry.project_by_key("Enabled").is_some());
    assert!(registry.project_by_key("Disabled").is_none());
}

#[tokio::test]
async fn refresh_if_needed_replaces_snapshot_when_version_changes() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    let app_a = repository
        .create_project(CreateProjectInput {
            name: "App A".to_string(),
            enabled: true,
            ingest_token: "igx_app_a".to_string(),
        })
        .await
        .expect("seed project should be created");

    let registry = ProjectRegistryState::load(repository.clone())
        .await
        .expect("registry should load");
    assert!(registry.project_by_key("App-A").is_some());
    assert!(registry.project_by_key("App-B").is_none());

    repository
        .update_project(
            app_a.id,
            UpdateProjectInput {
                name: None,
                enabled: Some(false),
                ingest_token: None,
            },
        )
        .await
        .expect("app-a should be disabled");
    repository
        .create_project(CreateProjectInput {
            name: "App B".to_string(),
            enabled: true,
            ingest_token: "igx_app_b".to_string(),
        })
        .await
        .expect("app-b should be created");

    let changed = registry
        .refresh_if_needed()
        .await
        .expect("refresh should succeed");

    assert!(
        changed,
        "version change should trigger snapshot replacement"
    );
    assert!(registry.project_by_key("App-A").is_none());
    assert!(registry.project_by_key("App-B").is_some());
}

#[tokio::test]
async fn refresh_if_needed_returns_false_when_version_is_unchanged() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    repository
        .create_project(CreateProjectInput {
            name: "Stable".to_string(),
            enabled: true,
            ingest_token: "igx_stable_app".to_string(),
        })
        .await
        .expect("seed project should be created");

    let registry = ProjectRegistryState::load(repository)
        .await
        .expect("registry should load");

    let changed = registry
        .refresh_if_needed()
        .await
        .expect("refresh should succeed");

    assert!(!changed, "unchanged version should not replace snapshot");
    assert!(registry.project_by_key("Stable").is_some());
}

#[actix_rt::test]
async fn build_app_state_initializes_mock_registry_with_default_project() {
    let temp = tempdir().expect("temp dir");
    let settings = Arc::new(Settings {
        ingest: IngestSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        logging: Default::default(),
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: None,
        },
        database: None,
        wal: test_wal_settings(temp.path()),
    });

    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
        cfg.route("/registry/{project_key}", web::get().to(registry_probe));
    }))
    .await;

    let enabled = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/Enabled")
            .to_request(),
    )
    .await;
    let disabled = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/Disabled")
            .to_request(),
    )
    .await;

    let appid = test::call_service(
        &app,
        test::TestRequest::get().uri("/registry/APPID").to_request(),
    )
    .await;

    assert_eq!(appid.status(), actix_http::StatusCode::OK);
    assert_eq!(enabled.status(), actix_http::StatusCode::NOT_FOUND);
    assert_eq!(disabled.status(), actix_http::StatusCode::NOT_FOUND);
}

#[actix_rt::test]
async fn build_app_state_allows_database_config_for_registry_backed_ingest() {
    let temp = tempdir().expect("temp dir");
    let settings = Arc::new(Settings {
        ingest: IngestSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        logging: Default::default(),
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: None,
        },
        database: Some(DatabaseSettings {
            url: "sqlite::memory:".to_string(),
            refresh_interval_secs: 3,
        }),
        wal: test_wal_settings(temp.path()),
    });

    build_app_state_with_test_processor(settings)
        .await
        .expect("database-backed ingest should initialize registry lookup");
}

#[actix_rt::test]
async fn build_app_state_seeds_local_kafka_delivery_target_without_toml_sinks() {
    let temp = tempdir().expect("temp dir");
    let settings = Arc::new(Settings {
        ingest: IngestSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        logging: Default::default(),
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: Some("ingest4x".to_string()),
        },
        database: Some(DatabaseSettings {
            url: "sqlite::memory:".to_string(),
            refresh_interval_secs: 3,
        }),
        wal: test_wal_settings(temp.path()),
    });

    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
    }))
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/admin/delivery-targets")
            .insert_header((ADMIN_PASSWORD_HEADER, "ingest4x"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let targets: serde_json::Value = test::read_body_json(response).await;
    let local_kafka = targets
        .as_array()
        .expect("targets should be an array")
        .iter()
        .find(|target| target["target_id"] == "local_kafka")
        .expect("local kafka delivery target should be seeded");

    assert_eq!(local_kafka["name"], "Local Kafka");
    assert_eq!(local_kafka["target_type"], "kafka");
    assert_eq!(local_kafka["enabled"], true);
    assert_eq!(
        local_kafka["config_json"]["bootstrap_servers"],
        "127.0.0.1:9092"
    );
    let loadtest_blackhole = targets
        .as_array()
        .expect("targets should be an array")
        .iter()
        .find(|target| target["target_id"] == "loadtest_blackhole")
        .expect("loadtest blackhole delivery target should be seeded");

    assert_eq!(loadtest_blackhole["name"], "Loadtest Blackhole");
    assert_eq!(loadtest_blackhole["target_type"], "blackhole");
    assert_eq!(loadtest_blackhole["enabled"], true);
    let local_parquet = targets
        .as_array()
        .expect("targets should be an array")
        .iter()
        .find(|target| target["target_id"] == "local_parquet")
        .expect("local parquet delivery target should be seeded");

    assert_eq!(local_parquet["name"], "Local Parquet");
    assert_eq!(local_parquet["target_type"], "parquet");
    assert_eq!(local_parquet["enabled"], true);
    assert_eq!(local_parquet["config_json"]["scheme"], "fs");
    assert_eq!(local_parquet["config_json"]["options"]["root"], "data");

    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/admin/event-sinks")
            .insert_header((ADMIN_PASSWORD_HEADER, "ingest4x"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let sinks: serde_json::Value = test::read_body_json(response).await;
    let sink_ids = sinks
        .as_array()
        .expect("sinks should be an array")
        .iter()
        .map(|sink| {
            sink["sink_id"]
                .as_str()
                .expect("sink_id should be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sink_ids,
        vec![
            "events",
            "events_error",
            "parquet_events",
            "loadtest_events"
        ]
    );
    let parquet_events = sinks
        .as_array()
        .expect("sinks should be an array")
        .iter()
        .find(|sink| sink["sink_id"] == "parquet_events")
        .expect("parquet events sink should be seeded");
    assert_eq!(parquet_events["destination_json"]["path_prefix"], "events");
    assert_eq!(
        parquet_events["destination_json"]["batch"]["max_events"],
        1000
    );
    assert_eq!(
        parquet_events["destination_json"]["batch"]["max_bytes"],
        64 * 1024 * 1024
    );
    assert_eq!(
        parquet_events["destination_json"]["batch"]["timeout"],
        "60s"
    );
}

#[actix_rt::test]
async fn build_app_state_seeds_default_test_app_with_rule_set_assignment() {
    let temp = tempdir().expect("temp dir");
    let settings = Arc::new(Settings {
        ingest: IngestSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        logging: Default::default(),
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: Some("ingest4x".to_string()),
        },
        database: Some(DatabaseSettings {
            url: "sqlite::memory:".to_string(),
            refresh_interval_secs: 3,
        }),
        wal: test_wal_settings(temp.path()),
    });

    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
        cfg.route("/registry/{project_key}", web::get().to(registry_probe));
    }))
    .await;

    let seeded_project = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/test_app")
            .to_request(),
    )
    .await;
    assert_eq!(seeded_project.status(), StatusCode::OK);

    let loadtest_project = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/loadtest_app")
            .to_request(),
    )
    .await;
    assert_eq!(loadtest_project.status(), StatusCode::OK);

    let bindings = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/admin/project-processors")
            .insert_header((ADMIN_PASSWORD_HEADER, "ingest4x"))
            .to_request(),
    )
    .await;
    assert_eq!(bindings.status(), StatusCode::OK);

    let bindings: serde_json::Value = test::read_body_json(bindings).await;
    assert_eq!(
        bindings
            .as_array()
            .expect("bindings should be an array")
            .iter()
            .filter(|binding| binding["project_id"] == 1)
            .count(),
        1
    );
}

fn test_wal_settings(dir: &Path) -> WalSettings {
    WalSettings {
        dir: dir.join("wal").display().to_string(),
        node_id: None,
        write: WalWriteSettings {
            flush_interval: "1ms".to_string(),
            flush_records: 1,
            no_sync: false,
            segment_max_bytes: ingest4x::settings::default_wal_write_segment_max_bytes(),
            min_free_bytes: 0,
        },
        checkpoint: CheckpointSettings::default(),
        replay: Default::default(),
    }
}

async fn registry_probe(
    project_key: web::Path<String>,
    registry: web::Data<ProjectRegistryState>,
) -> HttpResponse {
    if registry.project_by_key(&project_key).is_some() {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
    }
}
