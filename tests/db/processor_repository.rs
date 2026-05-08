use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::ingest::processor::{ProcessorRequestContext, ProcessorState};
use ingest4x::repositories::{
    CreateProcessorScriptInput, CreateProcessorScriptModuleInput, CreateProjectInput,
    ProcessorRepository, ProcessorRepositoryError, ProcessorScriptStatus, ProjectRepository,
    RuleRepository, UpdateProcessorScriptStatusInput,
};
use ingest4x::rules::Rules;
use serde_json::json;

#[tokio::test]
async fn loads_project_bound_processor_script_with_multiple_modules() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

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
    emit("events", event);
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
            Rules::default(),
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
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

    seed::run(&projects, &rules, &processors)
        .await
        .expect("seed should run");

    let runtime = processors
        .default_runtime_script()
        .await
        .expect("default processor should load");

    assert_eq!(runtime.script_key, "default");
    assert_eq!(runtime.entry_module, "main");
    assert_eq!(runtime.modules.len(), 1);
    assert!(runtime.entry_source.contains("validate(event)"));
    assert!(runtime.entry_source.contains("emit(\"events\", event)"));
    assert!(runtime
        .entry_source
        .contains("emit(\"events_error\", event)"));
}

#[tokio::test]
async fn create_script_rejects_duplicate_script_key() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let processors = ProcessorRepository::new(db);

    let input = CreateProcessorScriptInput {
        script_key: "custom_pipeline".to_string(),
        name: "Custom pipeline".to_string(),
        entry_module: "main".to_string(),
        status: ProcessorScriptStatus::Active,
        modules: vec![CreateProcessorScriptModuleInput {
            module_name: "main".to_string(),
            source: r#"fn process(event, request) { emit("events", event); }"#.to_string(),
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
async fn assign_project_processor_requires_active_script() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let processors = ProcessorRepository::new(db);

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
                source: r#"fn process(event, request) { emit("events", event); }"#.to_string(),
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
    let processors = ProcessorRepository::new(db);

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
                source: r#"fn process(event, request) { emit("events", event); }"#.to_string(),
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
