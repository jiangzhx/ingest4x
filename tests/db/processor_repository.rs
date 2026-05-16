use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::ingest::processor::{ProcessorRequestContext, ProcessorState};
use ingest4x::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, CreateProcessorScriptInput,
    CreateProcessorScriptModuleInput, CreateProjectInput, DeliveryTargetType, EventSinkRepository,
    ProcessorRepository, ProcessorRepositoryError, ProcessorScriptStatus, ProjectRepository,
    UpdateProcessorScriptInput, UpdateProcessorScriptModuleInput, UpdateProcessorScriptStatusInput,
    ValidateProcessorScriptInput, ValidateProcessorScriptModuleInput,
};
use ingest4x::settings::AutoOffsetReset;
use serde_json::json;
use std::path::Path;

async fn create_stdout_sink(repository: &EventSinkRepository, sink_id: &str) {
    let target = repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: format!("{sink_id}_target"),
            name: format!("{sink_id} target"),
            target_type: DeliveryTargetType::stdout(),
            config_json: json!({}),
            enabled: true,
        })
        .await
        .expect("delivery target should be created");
    repository
        .create_event_sink(CreateEventSinkInput {
            sink_id: sink_id.to_string(),
            name: sink_id.to_string(),
            delivery_target_id: target.id,
            destination_json: json!({}),
            auto_offset_reset: AutoOffsetReset::Earliest,
            enabled: true,
        })
        .await
        .expect("event sink should be created");
}

async fn create_default_sinks(repository: &EventSinkRepository) {
    create_stdout_sink(repository, "events").await;
    create_stdout_sink(repository, "events_error").await;
}

#[tokio::test]
async fn loads_project_bound_processor_script_with_multiple_modules() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let script = processors
        .create_script(CreateProcessorScriptInput {
            script_key: "custom_pipeline".to_string(),
            name: "Custom pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![
                CreateProcessorScriptModuleInput {
                    module_name: "main".to_string(),
                    source: r#"
import "custom" as custom;

fn process(event, request) {
    event = custom::mark(event);
    emit(SINK_EVENTS, event);
}
"#
                    .to_string(),
                },
                CreateProcessorScriptModuleInput {
                    module_name: "custom".to_string(),
                    source: r#"
fn mark(event) {
    event["xcontext"]["processor_marker"] = "db-module";
    return event;
}
"#
                    .to_string(),
                },
            ],
        })
        .await
        .expect("processor script should be created");

    processors
        .assign_project_processor(project.id, script.id, true)
        .await
        .expect("project processor should be assigned");

    let runtime = processors
        .runtime_script_for_project(project.id)
        .await
        .expect("runtime processor should load");

    assert_eq!(runtime.script_key, "custom_pipeline");
    assert_eq!(runtime.entry_module, "main");
    assert_eq!(runtime.modules.len(), 2);

    let processor = ProcessorState::new_with_modules(
        runtime.entry_source.clone(),
        runtime.resolver_modules(),
        10_000,
    )
    .expect("runtime processor should compile");
    let output = processor
        .process(
            json!({
                "appid": "app-1",
                "xwhat": "custom_event",
                "xcontext": {}
            }),
            ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
        )
        .expect("processor should run");

    assert_eq!(output.deliveries.len(), 1);
    assert_eq!(output.deliveries[0].target, "events");
    assert_eq!(
        output.deliveries[0].event["xcontext"]["processor_marker"],
        json!("db-module")
    );
}

#[tokio::test]
async fn seed_creates_minimal_default_processor_script() {
    assert!(
        !Path::new("pipeline/main.rhai").exists(),
        "default processor seed should come from src/db/seed.rs instead of pipeline/main.rhai",
    );

    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    seed::run(&projects, &sinks, &processors)
        .await
        .expect("seed should run");

    let runtime = processors
        .default_runtime_script()
        .await
        .expect("default processor should load");

    assert_eq!(runtime.script_key, "default");
    assert_eq!(runtime.entry_module, "main");
    assert_eq!(runtime.modules.len(), 1);
    assert!(runtime.entry_source.contains("event.required(\"xwhat\")"));
    assert!(runtime.entry_source.contains("try {"));
    assert!(runtime.entry_source.contains("emit(SINK_EVENTS, event)"));
    assert!(runtime
        .entry_source
        .contains("emit(SINK_EVENTS_ERROR, event)"));

    let loadtest_project = projects
        .find_enabled_project_by_ingest_token("igx_loadtest_token")
        .await
        .expect("loadtest project lookup should succeed")
        .expect("loadtest project should be seeded");
    let loadtest_runtime = processors
        .runtime_script_for_project(loadtest_project.id)
        .await
        .expect("loadtest processor should load");

    assert_eq!(loadtest_runtime.script_key, "loadtest_blackhole_processor");
    assert!(loadtest_runtime
        .entry_source
        .contains("emit(SINK_LOADTEST_EVENTS, event)"));
    assert!(loadtest_runtime
        .entry_source
        .contains("loadtest_validation_code_from_error(err)"));
    assert!(!loadtest_runtime
        .entry_source
        .contains("loadtest_validation_code\"] = `${err}`"));
}

#[tokio::test]
async fn seed_keeps_disabled_loadtest_project_without_duplicate_token_error() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    projects
        .create_project(CreateProjectInput {
            name: "loadtest_app".to_string(),
            enabled: false,
            ingest_token: "igx_loadtest_token".to_string(),
        })
        .await
        .expect("disabled loadtest project should be created");

    seed::run(&projects, &sinks, &processors)
        .await
        .expect("seed should tolerate disabled loadtest project");

    let loadtest_project = projects
        .list_projects()
        .await
        .expect("projects should load")
        .into_iter()
        .find(|project| project.ingest_token == "igx_loadtest_token")
        .expect("loadtest project should still exist");
    assert!(!loadtest_project.enabled);

    let runtime = processors
        .runtime_script_for_project(loadtest_project.id)
        .await
        .expect("loadtest processor binding should still be maintained");
    assert_eq!(runtime.script_key, "loadtest_blackhole_processor");
}

#[tokio::test]
async fn create_script_rejects_duplicate_script_key() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let input = CreateProcessorScriptInput {
        script_key: "custom_pipeline".to_string(),
        name: "Custom pipeline".to_string(),
        entry_module: "main".to_string(),
        status: ProcessorScriptStatus::Active,
        modules: vec![CreateProcessorScriptModuleInput {
            module_name: "main".to_string(),
            source: r#"fn process(event, request) { emit(SINK_EVENTS, event); }"#.to_string(),
        }],
    };

    let first = processors
        .create_script(input.clone())
        .await
        .expect("first script should be created");
    assert_eq!(first.version, 1);

    let duplicate = processors
        .create_script(CreateProcessorScriptInput {
            name: "Custom pipeline duplicate".to_string(),
            ..input
        })
        .await
        .expect_err("duplicate script_key should be rejected");

    assert!(matches!(
        duplicate,
        ProcessorRepositoryError::DuplicateProcessorScriptKey { ref script_key }
            if script_key == "custom_pipeline"
    ));
}

#[tokio::test]
async fn validate_script_rejects_invalid_rhai_without_persisting() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let error = processors
        .validate_script(ValidateProcessorScriptInput {
            entry_module: "main".to_string(),
            modules: vec![ValidateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit(SINK_EVENTS, event);"#.to_string(),
            }],
        })
        .await
        .expect_err("invalid Rhai script should be rejected");

    assert!(matches!(
        error,
        ProcessorRepositoryError::InvalidScript { .. }
    ));
    assert!(error.to_string().contains("Rhai module `main`"));
    let scripts = processors
        .list_scripts()
        .await
        .expect("list scripts should still work");
    assert!(scripts.is_empty());
}

#[tokio::test]
async fn validate_script_allows_complex_module_expressions_like_runtime_compile() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;
    let nested_expression = format!("{}event{}", "(".repeat(96), ")".repeat(96));

    processors
        .validate_script(ValidateProcessorScriptInput {
            entry_module: "main".to_string(),
            modules: vec![
                ValidateProcessorScriptModuleInput {
                    module_name: "main".to_string(),
                    source: r#"
import "custom" as custom;

fn process(event, request) {
    emit(SINK_EVENTS, custom::pass(event));
}
"#
                    .to_string(),
                },
                ValidateProcessorScriptModuleInput {
                    module_name: "custom".to_string(),
                    source: format!(
                        r#"
fn pass(event) {{
    return {nested_expression};
}}
"#
                    ),
                },
            ],
        })
        .await
        .expect("validation should match runtime expression depth limits");
}

#[tokio::test]
async fn validate_script_reports_invalid_module_name_for_module_source() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let error = processors
        .validate_script(ValidateProcessorScriptInput {
            entry_module: "main".to_string(),
            modules: vec![
                ValidateProcessorScriptModuleInput {
                    module_name: "main".to_string(),
                    source: r#"import "custom" as custom;

fn process(event, request) {
    event = custom::mark(event);
    emit(SINK_EVENTS, event);
}"#
                    .to_string(),
                },
                ValidateProcessorScriptModuleInput {
                    module_name: "custom".to_string(),
                    source: r#"fn mark(event) { let broken = ; return event; }"#.to_string(),
                },
            ],
        })
        .await
        .expect_err("invalid module source should be rejected");

    assert!(matches!(
        error,
        ProcessorRepositoryError::InvalidScript { .. }
    ));
    assert!(error.to_string().contains("Rhai module `custom`"));
}

#[tokio::test]
async fn validate_script_rejects_string_emit_targets() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_stdout_sink(&sinks, "events").await;

    let error = processors
        .validate_script(ValidateProcessorScriptInput {
            entry_module: "main".to_string(),
            modules: vec![ValidateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit("events", event); }"#.to_string(),
            }],
        })
        .await
        .expect_err("string emit target should be rejected");

    assert!(matches!(
        error,
        ProcessorRepositoryError::InvalidScript { .. }
    ));
    assert!(error.to_string().contains("Rhai module `main`"));
    assert!(error.to_string().contains("SINK_EVENTS"));
}

#[tokio::test]
async fn processor_runtime_resolves_sink_target_constants() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_stdout_sink(&sinks, "events").await;

    let script = processors
        .create_script(CreateProcessorScriptInput {
            script_key: "constant_pipeline".to_string(),
            name: "Constant pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit(SINK_EVENTS, event); }"#.to_string(),
            }],
        })
        .await
        .expect("constant target script should be created");
    let (_, modules) = processors
        .get_script(script.id)
        .await
        .expect("script should load")
        .expect("script should exist");
    let entry_source = modules
        .iter()
        .find(|module| module.module_name == "main")
        .expect("entry module should exist")
        .source
        .clone();
    let sink_targets = processors
        .enabled_sink_ids()
        .await
        .expect("sink ids should load");
    let processor =
        ProcessorState::new_with_sink_targets(entry_source, Vec::new(), sink_targets, 10_000)
            .expect("processor should compile with sink constants");

    let output = processor
        .process(
            json!({
                "appid": "app-1",
                "xwhat": "constant_event",
                "xcontext": {}
            }),
            ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
        )
        .expect("processor should run");

    assert_eq!(output.deliveries.len(), 1);
    assert_eq!(output.deliveries[0].target, "events");
}

#[tokio::test]
async fn update_script_replaces_modules_and_increments_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let script = processors
        .create_script(CreateProcessorScriptInput {
            script_key: "editable_pipeline".to_string(),
            name: "Editable pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Draft,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit(SINK_EVENTS, event); }"#.to_string(),
            }],
        })
        .await
        .expect("script should be created");

    let updated = processors
        .update_script(
            script.id,
            UpdateProcessorScriptInput {
                name: "Updated pipeline".to_string(),
                entry_module: "main".to_string(),
                status: ProcessorScriptStatus::Active,
                modules: vec![
                    UpdateProcessorScriptModuleInput {
                        module_name: "main".to_string(),
                        source: r#"
import "custom" as custom;

fn process(event, request) {
    event = custom::mark(event);
    emit(SINK_EVENTS, event);
}
"#
                        .to_string(),
                    },
                    UpdateProcessorScriptModuleInput {
                        module_name: "custom".to_string(),
                        source: r#"
fn mark(event) {
    event["xcontext"]["processor_marker"] = "updated";
    return event;
}
"#
                        .to_string(),
                    },
                ],
            },
        )
        .await
        .expect("script should be updated");

    assert_eq!(updated.id, script.id);
    assert_eq!(updated.script_key, "editable_pipeline");
    assert_eq!(updated.name, "Updated pipeline");
    assert_eq!(updated.version, 2);
    assert_eq!(updated.status, ProcessorScriptStatus::Active);
    assert_ne!(updated.checksum, script.checksum);
    assert!(updated.activated_at.is_some());

    let (_, modules) = processors
        .get_script(script.id)
        .await
        .expect("detail should load")
        .expect("script should exist");
    assert_eq!(modules.len(), 2);
    assert!(modules.iter().any(|module| module.module_name == "custom"));
}

#[tokio::test]
async fn assign_project_processor_requires_active_script() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let project = projects
        .create_project(CreateProjectInput {
            name: "Draft App".to_string(),
            enabled: true,
            ingest_token: "igx_app_draft".to_string(),
        })
        .await
        .expect("project should be created");
    let script = processors
        .create_script(CreateProcessorScriptInput {
            script_key: "draft_pipeline".to_string(),
            name: "Draft pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Draft,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit(SINK_EVENTS, event); }"#.to_string(),
            }],
        })
        .await
        .expect("draft script should be created");

    let error = processors
        .assign_project_processor(project.id, script.id, true)
        .await
        .expect_err("draft script should not be assignable");

    assert!(matches!(
        error,
        ProcessorRepositoryError::ProcessorScriptNotActive { id } if id == script.id
    ));
}

#[tokio::test]
async fn update_script_status_rejects_disabling_script_in_use() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let sinks = EventSinkRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);
    create_default_sinks(&sinks).await;

    let project = projects
        .create_project(CreateProjectInput {
            name: "Active App".to_string(),
            enabled: true,
            ingest_token: "igx_app_active".to_string(),
        })
        .await
        .expect("project should be created");
    let script = processors
        .create_script(CreateProcessorScriptInput {
            script_key: "active_pipeline".to_string(),
            name: "Active pipeline".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: r#"fn process(event, request) { emit(SINK_EVENTS, event); }"#.to_string(),
            }],
        })
        .await
        .expect("active script should be created");
    processors
        .assign_project_processor(project.id, script.id, true)
        .await
        .expect("script should be assigned");

    let error = processors
        .update_script_status(
            script.id,
            UpdateProcessorScriptStatusInput {
                status: ProcessorScriptStatus::Archived,
            },
        )
        .await
        .expect_err("script in use should not be disabled");

    assert!(matches!(
        error,
        ProcessorRepositoryError::ProcessorScriptInUse { id } if id == script.id
    ));

    processors
        .delete_project_processor(project.id)
        .await
        .expect("binding should be removed");
    let disabled = processors
        .update_script_status(
            script.id,
            UpdateProcessorScriptStatusInput {
                status: ProcessorScriptStatus::Archived,
            },
        )
        .await
        .expect("unused script should be disabled");

    assert_eq!(disabled.status, ProcessorScriptStatus::Archived);
}
