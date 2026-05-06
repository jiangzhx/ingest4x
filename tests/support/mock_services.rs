#![allow(dead_code)]

use actix_http::Request;
use actix_web::dev::{Service, ServiceResponse};
use actix_web::web::Data;
use actix_web::{test, web, App};
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::ingest;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::repositories::{
    CreateProjectInput, CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput,
    ProjectRepository, RuleRepository, UpdateRuleSetInput,
};
use ingest4x::server;
use ingest4x::services::ProjectRegistryState;
use ingest4x::settings::{
    AutoOffsetReset, CheckpointSettings, EventSinkConfig, EventsSettings, IngestSettings,
    ManagementSettings, Settings, WalSettings,
};
use ingest4x::utils::events::init_event_sinks;
use ingest4x::wal::replay::{
    initialize_sink_checkpoints, replay_once as replay_wal_once, WalReplayContext,
};
use ingest4x::wal::WalWriter;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

pub struct TestService {
    pub bootstrap_servers: String,
    pub topic: String,
    pub error_topic: String,
    pub kafka_cluster: MockCluster<'static, DefaultProducerContext>,
    wal_dir: TempDir,
    event_sinks: Data<ingest4x::utils::events::EventSinkState>,
    project_registry: Data<ProjectRegistryState>,
    rule_repository: Data<RuleRepository>,
    processor: Data<ProcessorState>,
    checkpoint: CheckpointSettings,
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
        events: stdout_events_settings(),
    });
    let app_state = server::build_app_state(settings)
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
    let event_sinks = init_event_sinks(&kafka_events_settings(
        bootstrap_servers.as_str(),
        TOPIC,
        ERROR_TOPIC,
    ))
    .expect("event sinks should initialize");
    let (project_registry, rule_repository) = create_project_state(project).await;
    let project_registry = Data::new(project_registry);
    let rule_repository = Data::new(rule_repository);
    let processor = match processor_script {
        Some(script) => ProcessorState::new(script.to_string(), 10_000)
            .expect("test processor should initialize"),
        None => ProcessorState::from_default_entry().expect("processor should initialize"),
    };
    let processor = Data::new(processor);
    let wal_settings = test_wal_settings(wal_dir.path());
    let checkpoint = wal_settings.checkpoint.clone();
    let wal = Data::new(WalWriter::new(&wal_settings).expect("test wal should initialize"));
    initialize_sink_checkpoints(wal_dir.path(), &event_sinks)
        .expect("test sink checkpoints should initialize");

    let mut app = App::new().app_data(event_sinks.clone());
    app = app
        .app_data(project_registry.clone())
        .app_data(rule_repository.clone())
        .app_data(processor.clone())
        .app_data(wal)
        .route("/ingest", web::post().to(ingest));

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
            rule_repository,
            processor,
            checkpoint,
        },
    )
}

pub async fn replay_once(testservice: &TestService) -> anyhow::Result<usize> {
    replay_wal_once(WalReplayContext {
        dir: testservice.wal_dir.path(),
        event_sinks: &testservice.event_sinks,
        project_registry: &testservice.project_registry,
        rule_repository: &testservice.rule_repository,
        processor: &testservice.processor,
        checkpoint: testservice.checkpoint.clone(),
    })
    .await
}

fn stdout_events_settings() -> EventsSettings {
    EventsSettings {
        sink: HashMap::from([
            ("events".to_string(), EventSinkConfig::stdout()),
            ("events_error".to_string(), EventSinkConfig::stdout()),
        ]),
    }
}

fn kafka_events_settings(
    bootstrap_servers: &str,
    topic: &str,
    error_topic: &str,
) -> EventsSettings {
    let sink = HashMap::from([
        (
            "events".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: bootstrap_servers.to_string(),
                topic: topic.to_string(),
                auto_offset_reset: AutoOffsetReset::Latest,
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        ),
        (
            "events_error".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: bootstrap_servers.to_string(),
                topic: error_topic.to_string(),
                auto_offset_reset: AutoOffsetReset::Latest,
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        ),
    ]);

    EventsSettings { sink }
}

fn test_wal_settings(dir: &Path) -> WalSettings {
    WalSettings {
        dir: dir.display().to_string(),
        node_id: None,
        flush_max_interval: "1ms".to_string(),
        flush_max_records: 1,
        no_sync: false,
        wal_segment_max_bytes: ingest4x::settings::default_wal_segment_max_bytes(),
        min_free_bytes: 0,
        checkpoint: CheckpointSettings::default(),
    }
}

async fn create_project_state(
    project: HashMap<String, String>,
) -> (ProjectRegistryState, RuleRepository) {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db.clone());
    let rule_repository = RuleRepository::new(db);

    if !project.is_empty() {
        repository
            .create_project(CreateProjectInput {
                appid: "APPID".to_string(),
                name: project
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "APPID".to_string()),
                enabled: true,
            })
            .await
            .expect("mock project should be created");

        let rule_set = rule_repository
            .create_rule_set(CreateRuleSetInput {
                name: "Test ingest rules".to_string(),
                description: None,
                enabled: true,
            })
            .await
            .expect("test rule set should be created");
        let default_rule = rule_repository
            .create_rule(CreateRuleInput {
                rule_set_id: rule_set.id,
                parent_id: None,
                name: "Default".to_string(),
                xwhat: None,
                content: r#"
fields:
  appid:
    required: true
    type: string
  xwhat:
    required: true
    type: string
  xcontext:
    required: true
    type: object
  xcontext.installid:
    required: true
    type: string
  xcontext.os:
    required: true
    type: string
"#
                .to_string(),
                enabled: true,
            })
            .await
            .expect("test default rule should be created");
        rule_repository
            .update_rule_set(
                rule_set.id,
                UpdateRuleSetInput {
                    name: None,
                    description: None,
                    enabled: None,
                    wildcard_rule_id: Some(Some(default_rule.id)),
                },
            )
            .await
            .expect("test default rule should be selected as wildcard");
        rule_repository
            .assign_rule_set_to_project(
                "APPID",
                CreateProjectRuleSetInput {
                    rule_set_id: rule_set.id,
                    enabled: true,
                },
            )
            .await
            .expect("test rule set should be assigned");
    }

    let registry = ProjectRegistryState::load(repository)
        .await
        .expect("project registry should load");
    (registry, rule_repository)
}
