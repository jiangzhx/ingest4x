#![allow(dead_code)]

use actix_http::Request;
use actix_web::dev::{Service, ServiceResponse};
use actix_web::web::Data;
use actix_web::{test, web, App};
use ingest4x::db::init_sqlite_database;
use ingest4x::ingest::ingest;
use ingest4x::ingest::processor::ProcessorState;
use ingest4x::projects::{CreateProjectInput, ProjectRegistryState, ProjectRepository};
use ingest4x::rules::{
    CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput, RuleRepository,
    UpdateRuleSetInput,
};
use ingest4x::server;
use ingest4x::settings::{
    EventRouteSet, EventRouteSettings, EventSinkConfig, EventsSettings, LogLevel,
    ManagementSettings, ServerSettings, Settings,
};
use ingest4x::utils::events::init_event_sinks;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TestService {
    pub bootstrap_servers: String,
    pub topic: String,
    pub error_topic: String,
    pub kafka_cluster: MockCluster<'static, DefaultProducerContext>,
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
        checkpoint: Default::default(),
        events: stdout_events_settings(),
        redis: None,
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
    let processor = match processor_script {
        Some(script) => ProcessorState::new(script.to_string(), 10_000)
            .expect("test processor should initialize"),
        None => ProcessorState::from_default_entry().expect("processor should initialize"),
    };

    let mut app = App::new().app_data(event_sinks);
    app = app
        .app_data(Data::new(project_registry))
        .app_data(Data::new(rule_repository))
        .app_data(Data::new(processor))
        .route("/ingest", web::post().to(ingest));

    (
        test::init_service(app).await,
        TestService {
            bootstrap_servers,
            topic: TOPIC.to_string(),
            error_topic: ERROR_TOPIC.to_string(),
            kafka_cluster,
        },
    )
}

fn stdout_events_settings() -> EventsSettings {
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

fn kafka_events_settings(
    bootstrap_servers: &str,
    topic: &str,
    error_topic: &str,
) -> EventsSettings {
    let sink = HashMap::from([
        (
            "kafka_valid".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: bootstrap_servers.to_string(),
                topic: topic.to_string(),
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        ),
        (
            "kafka_invalid".to_string(),
            EventSinkConfig::Kafka {
                bootstrap_servers: bootstrap_servers.to_string(),
                topic: error_topic.to_string(),
                delivery_timeout_ms: "5000".to_string(),
                queue_buffering_max_ms: "0".to_string(),
                batch_num_messages: "1".to_string(),
                queue_buffering_max_messages: "300".to_string(),
                linger_ms: "0".to_string(),
            },
        ),
    ]);

    EventsSettings {
        sink,
        valid: EventRouteSet {
            routes: vec![EventRouteSettings {
                sinks: vec!["kafka_valid".to_string()],
                ..Default::default()
            }],
        },
        invalid: EventRouteSet {
            routes: vec![EventRouteSettings {
                sinks: vec!["kafka_invalid".to_string()],
                ..Default::default()
            }],
        },
    }
}

async fn create_project_state(
    project: HashMap<String, String>,
) -> (ProjectRegistryState, RuleRepository) {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repository = ProjectRepository::new(db);
    let rule_repository = RuleRepository::new(repository.database());

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
