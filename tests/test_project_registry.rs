use actix_http::StatusCode;
use actix_web::{test, web, App, HttpResponse};
use ingest4x::db::init_sqlite_database;
use ingest4x::projects::ProjectRegistryState;
use ingest4x::projects::{CreateProjectInput, ProjectRepository, UpdateProjectInput};
use ingest4x::server;
use ingest4x::settings::{
    DatabaseSettings, EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings, LogLevel,
    ManagementSettings, ServerSettings, Settings,
};
use std::collections::HashMap;
use std::sync::Arc;

const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";

#[tokio::test]
async fn load_only_keeps_enabled_projects_in_memory() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    repository
        .create_project(CreateProjectInput {
            appid: "enabled-app".to_string(),
            name: "Enabled".to_string(),
            enabled: true,
        })
        .await
        .expect("enabled project should be created");
    repository
        .create_project(CreateProjectInput {
            appid: "disabled-app".to_string(),
            name: "Disabled".to_string(),
            enabled: false,
        })
        .await
        .expect("disabled project should be created");

    let registry = ProjectRegistryState::load(repository)
        .await
        .expect("registry should load");

    assert!(registry.contains("enabled-app"));
    assert!(!registry.contains("disabled-app"));
}

#[tokio::test]
async fn refresh_if_needed_replaces_snapshot_when_version_changes() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    repository
        .create_project(CreateProjectInput {
            appid: "app-a".to_string(),
            name: "App A".to_string(),
            enabled: true,
        })
        .await
        .expect("seed project should be created");

    let registry = ProjectRegistryState::load(repository.clone())
        .await
        .expect("registry should load");
    assert!(registry.contains("app-a"));
    assert!(!registry.contains("app-b"));

    repository
        .update_project(
            "app-a",
            UpdateProjectInput {
                name: None,
                enabled: Some(false),
            },
        )
        .await
        .expect("app-a should be disabled");
    repository
        .create_project(CreateProjectInput {
            appid: "app-b".to_string(),
            name: "App B".to_string(),
            enabled: true,
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
    assert!(!registry.contains("app-a"));
    assert!(registry.contains("app-b"));
}

#[tokio::test]
async fn refresh_if_needed_returns_false_when_version_is_unchanged() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);

    repository
        .create_project(CreateProjectInput {
            appid: "stable-app".to_string(),
            name: "Stable".to_string(),
            enabled: true,
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
    assert!(registry.contains("stable-app"));
}

#[actix_rt::test]
async fn build_app_state_initializes_mock_registry_with_default_project() {
    let settings = Arc::new(Settings {
        server: ServerSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            log_level: LogLevel::Info,
            log_format: "json".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: None,
        },
        database: None,
        wal: None,
        events: test_events_settings(),
        redis: None,
    });

    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
        cfg.route("/registry/{appid}", web::get().to(registry_probe));
    }))
    .await;

    let enabled = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/enabled-app")
            .to_request(),
    )
    .await;
    let disabled = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/registry/disabled-app")
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
async fn build_app_state_allows_database_config_without_redis_for_registry_backed_ingest() {
    let settings = Arc::new(Settings {
        server: ServerSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            log_level: LogLevel::Info,
            log_format: "json".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: None,
        },
        database: Some(DatabaseSettings {
            url: "sqlite::memory:".to_string(),
            refresh_interval_secs: 3,
        }),
        wal: None,
        events: test_events_settings(),
        redis: None,
    });

    server::build_app_state(settings)
        .await
        .expect("database-backed ingest should not require redis for registry lookup");
}

#[actix_rt::test]
async fn build_app_state_seeds_default_test_app_with_rule_set_assignment() {
    let settings = Arc::new(Settings {
        server: ServerSettings {
            bind_address: "127.0.0.1:8090".to_string(),
            log_level: LogLevel::Info,
            log_format: "json".to_string(),
            max_event_bytes: ingest4x::settings::default_max_event_bytes(),
        },
        management: ManagementSettings {
            bind_address: "127.0.0.1:18090".to_string(),
            admin_password: Some("ingest4x".to_string()),
        },
        database: Some(DatabaseSettings {
            url: "sqlite::memory:".to_string(),
            refresh_interval_secs: 3,
        }),
        wal: None,
        events: test_events_settings(),
        redis: None,
    });

    let app_state = server::build_app_state(settings)
        .await
        .expect("build app state");
    let app = test::init_service(App::new().configure(|cfg| {
        server::configure_private_app(cfg, app_state.clone());
        cfg.route("/registry/{appid}", web::get().to(registry_probe));
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

    let assignments = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/admin/projects/test_app/rule-sets")
            .insert_header((ADMIN_PASSWORD_HEADER, "ingest4x"))
            .to_request(),
    )
    .await;
    assert_eq!(assignments.status(), StatusCode::OK);

    let assignments: serde_json::Value = test::read_body_json(assignments).await;
    assert_eq!(
        assignments
            .as_array()
            .expect("assignments should be an array")
            .len(),
        1
    );
}

fn test_events_settings() -> EventsSettings {
    EventsSettings {
        sink: HashMap::from([("stdout".to_string(), EventSinkConfig::Stdout)]),
        valid: EventRouteSet {
            routes: vec![EventRouteSettings {
                sinks: vec!["stdout".to_string()],
                ..Default::default()
            }],
        },
        invalid: EventRouteSet {
            routes: vec![EventRouteSettings {
                sinks: vec!["stdout".to_string()],
                ..Default::default()
            }],
        },
    }
}

async fn registry_probe(
    appid: web::Path<String>,
    registry: web::Data<ProjectRegistryState>,
) -> HttpResponse {
    if registry.contains(&appid) {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
    }
}
