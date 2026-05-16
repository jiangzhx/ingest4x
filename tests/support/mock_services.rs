#![allow(dead_code)]

use crate::support::sinks::init_kafka_event_sinks;
use actix_http::Request;
use actix_web::dev::{Service, ServiceResponse};
use actix_web::web::Data;
use actix_web::{test, web, App};
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::ingest;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::repositories::{
    CreateProjectInput, ProjectAuthMode, ProjectRepository, UpdateProjectIngestSettingsInput,
};
use ingest4x::server;
use ingest4x::services::ProjectRegistryState;
use ingest4x::settings::{
    CheckpointSettings, IngestSettings, ManagementSettings, ReplaySettings, Settings, WalSettings,
    WalWriteSettings,
};
use ingest4x::wal::replay::{
    initialize_replay_checkpoint, replay_once as replay_wal_once, WalReplayContext,
};
use ingest4x::wal::WalWriter;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

pub const TEST_PROCESSOR_SCRIPT: &str = r#"
fn process(event, request) {
    try {
        event.required("appid").string().min(1);
        event.required("xwhat").string().min(1);
        event.required("xcontext").object();
        event.required("xcontext.installid").string().min(1);
        event.required("xcontext.os").string().min(1);

        if !event.contains("xwhen") || event["xwhen"] == () {
            event["xwhen"] = request.received_at_ms();
        }
        if event.contains("xcontext") && event["xcontext"] != () {
            let xcontext = event["xcontext"];
            if xcontext.contains("os") && xcontext["os"] != () {
                xcontext["os"] = xcontext["os"].to_lower();
                if !xcontext.contains("platform") || xcontext["platform"] == () {
                    xcontext["platform"] = xcontext["os"];
                }
            }
            if xcontext.contains("currencytype") && xcontext["currencytype"] != () {
                xcontext["currencytype"] = xcontext["currencytype"].to_upper();
            }
            if !xcontext.contains("ip") || xcontext["ip"] == () {
                let ip = request.ip();
                if ip != () {
                    xcontext["ip"] = ip;
                }
            }
            xcontext["process_info"] = #{
                receive_time: request.received_at_ms(),
                ingest4x_version: ingest4x_version()
            };
            event["xcontext"] = xcontext;
        }

        emit(SINK_EVENTS, event);
    } catch (err) {
        if !event.contains("xcontext") || event["xcontext"] == () {
            event["xcontext"] = #{};
        }
        let xcontext = event["xcontext"];
        xcontext["process_info"] = #{
            receive_time: request.received_at_ms(),
            ingest4x_version: ingest4x_version(),
            reason: `${err}`,
            error_code: `${err}`
        };
        event["xcontext"] = xcontext;
        emit(SINK_EVENTS_ERROR, event);
    }
}
"#;

pub fn test_processor_state() -> ProcessorState {
    ProcessorState::new(TEST_PROCESSOR_SCRIPT.to_string(), 10_000)
        .expect("test processor should initialize")
}

pub async fn build_app_state_with_test_processor(
    settings: Arc<Settings>,
) -> std::io::Result<server::AppState> {
    server::build_app_state_with_processor(settings, test_processor_state()).await
}

pub struct TestService {
    pub bootstrap_servers: String,
    pub topic: String,
    pub error_topic: String,
    pub kafka_cluster: MockCluster<'static, DefaultProducerContext>,
    wal_dir: TempDir,
    event_sinks: Data<ingest4x::sinks::EventSinkState>,
    project_registry: Data<ProjectRegistryState>,
    processor: Data<ProcessorState>,
    checkpoint: CheckpointSettings,
    replay: ReplaySettings,
}

fn create_kafka_cluster(topic: &str) -> (MockCluster<'static, DefaultProducerContext>, String) {
    let kafka_cluster = MockCluster::new(3).expect("create kafka mock cluster");
    kafka_cluster
        .create_topic(topic, 1, 1)
        .expect("create kafka mock topic");
    let bootstrap_servers = kafka_cluster.bootstrap_servers();
    (kafka_cluster, bootstrap_servers)
}

pub async fn create_configured_app(
) -> impl Service<Request, Response = ServiceResponse, Error = actix_web::Error> {
    let wal_dir = tempdir().expect("temp wal dir");
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
        wal: test_wal_settings(wal_dir.path()),
    });
    let app_state = build_app_state_with_test_processor(settings)
        .await
        .expect("build app state");

    test::init_service(App::new().configure(|cfg| {
        server::configure_app(cfg, app_state.clone());
    }))
    .await
}

pub async fn create_app() -> (
    impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
    TestService,
) {
    create_app_with_project(HashMap::from([
        ("re_attribution".to_string(), "300".to_string()),
        ("os".to_string(), "android".to_string()),
    ]))
    .await
}

pub async fn create_app_with_project(
    project: HashMap<String, String>,
) -> (
    impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
    TestService,
) {
    create_app_with_project_and_event_settings(project).await
}

pub async fn create_app_with_processor_script(
    script: &str,
) -> (
    impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
    TestService,
) {
    create_app_with_project_event_settings_and_processor(
        HashMap::from([
            ("re_attribution".to_string(), "300".to_string()),
            ("os".to_string(), "android".to_string()),
        ]),
        Some(script),
    )
    .await
}

async fn create_app_with_project_and_event_settings(
    project: HashMap<String, String>,
) -> (
    impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
    TestService,
) {
    create_app_with_project_event_settings_and_processor(project, None).await
}

async fn create_app_with_project_event_settings_and_processor(
    project: HashMap<String, String>,
    processor_script: Option<&str>,
) -> (
    impl Service<Request, Response = ServiceResponse, Error = actix_web::Error>,
    TestService,
) {
    const TOPIC: &str = "fake_topic";
    const ERROR_TOPIC: &str = "fake_topic_error";
    let wal_dir = tempdir().expect("temp wal dir");
    let (kafka_cluster, bootstrap_servers) = create_kafka_cluster(TOPIC);
    kafka_cluster
        .create_topic(ERROR_TOPIC, 1, 1)
        .expect("create kafka mock error topic");
    let event_sinks = init_kafka_event_sinks(bootstrap_servers.as_str(), TOPIC, ERROR_TOPIC);
    let project_registry = create_project_state(project).await;
    let project_registry = Data::new(project_registry);
    let processor = match processor_script {
        Some(script) => ProcessorState::new(script.to_string(), 10_000)
            .expect("test processor should initialize"),
        None => test_processor_state(),
    };
    let processor = Data::new(processor);
    let wal_settings = test_wal_settings(wal_dir.path());
    let checkpoint = wal_settings.checkpoint.clone();
    let replay = wal_settings.replay.clone();
    let wal = Data::new(WalWriter::new(&wal_settings).expect("test wal should initialize"));
    initialize_replay_checkpoint(wal_dir.path(), &event_sinks)
        .expect("test replay checkpoint should initialize");

    let mut app = App::new().app_data(event_sinks.clone());
    app = app
        .app_data(project_registry.clone())
        .app_data(processor.clone())
        .app_data(wal)
        .route("/ingest/{project_key}", web::post().to(ingest))
        .route("/ingest/{project_key}", web::get().to(ingest));

    (
        test::init_service(app).await,
        TestService {
            bootstrap_servers,
            topic: TOPIC.to_string(),
            error_topic: ERROR_TOPIC.to_string(),
            kafka_cluster,
            wal_dir,
            event_sinks,
            project_registry,
            processor,
            checkpoint,
            replay,
        },
    )
}

pub async fn replay_once(testservice: &TestService) -> anyhow::Result<usize> {
    replay_wal_once(WalReplayContext {
        dir: testservice.wal_dir.path(),
        event_sinks: &testservice.event_sinks,
        project_registry: &testservice.project_registry,
        processor: testservice.processor.get_ref(),
        checkpoint: testservice.checkpoint.clone(),
        replay: testservice.replay.clone(),
    })
    .await
}

fn test_wal_settings(dir: &Path) -> WalSettings {
    WalSettings {
        dir: dir.display().to_string(),
        node_id: None,
        write: WalWriteSettings {
            flush_interval: "1ms".to_string(),
            flush_records: 1,
            no_sync: false,
            segment_max_bytes: ingest4x::settings::default_wal_write_segment_max_bytes(),
            min_free_bytes: 0,
        },
        checkpoint: CheckpointSettings::default(),
        replay: ReplaySettings::default(),
    }
}

async fn create_project_state(project: HashMap<String, String>) -> ProjectRegistryState {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db.clone());

    if !project.is_empty() {
        let project_settings = project;
        let allowed_ips = project_settings.get("allowed_ips").map(|ips| {
            ips.split(',')
                .map(str::trim)
                .filter(|ip| !ip.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        });
        let auth_mode = project_settings
            .get("auth_mode")
            .map(|strategy| ProjectAuthMode::from_storage(strategy));
        let _project = repository
            .create_project_with_ingest_settings(
                CreateProjectInput {
                    name: project_settings
                        .get("name")
                        .cloned()
                        .unwrap_or_else(|| "APPID".to_string()),
                    enabled: true,
                    ingest_token: "igx_test_token".to_string(),
                },
                UpdateProjectIngestSettingsInput {
                    project_key: project_settings.get("project_key").cloned(),
                    auth_mode,
                    allowed_ips,
                },
            )
            .await
            .expect("mock project should be created");
    }

    ProjectRegistryState::load(repository)
        .await
        .expect("project registry should load")
}
