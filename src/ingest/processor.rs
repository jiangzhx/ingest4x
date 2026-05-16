use crate::rhai_ctx::{
    enter_processor_context, push_sink_target_constants, register_api, ProcessorDelivery,
};
use crate::settings::default_processor_max_operations;
use anyhow::{anyhow, Result};
use futures::lock::Mutex as AsyncMutex;
use rhai::module_resolvers::StaticModuleResolver;
use rhai::serde::to_dynamic;
use rhai::{Dynamic, Engine, Module, Scope, AST};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use crate::repositories::{ProcessorRepository, RuntimeProcessorScript};
pub use crate::rhai_ctx::ProcessorRequestContext;

const DEFAULT_SINK_TARGETS: &[&str] = &["events", "events_error"];

#[derive(Clone)]
pub struct ProcessorState {
    engine: Arc<Engine>,
    ast: AST,
}

pub struct ProcessorOutput {
    pub deliveries: Vec<ProcessorDelivery>,
}

pub trait ProcessorRuntime: Send + Sync {
    fn process_event(
        &self,
        project_id: i32,
        event: Value,
        request: ProcessorRequestContext,
    ) -> Result<ProcessorOutput>;
}

impl ProcessorState {
    pub fn new(script: String, max_operations: u64) -> Result<Self> {
        Self::new_with_modules(script, Vec::new(), max_operations)
    }

    pub fn new_with_modules(
        script: String,
        modules: Vec<(String, String)>,
        max_operations: u64,
    ) -> Result<Self> {
        Self::new_with_sink_targets(script, modules, default_sink_targets(), max_operations)
    }

    pub fn new_with_sink_targets(
        script: String,
        modules: Vec<(String, String)>,
        sink_targets: Vec<String>,
        max_operations: u64,
    ) -> Result<Self> {
        let (engine, ast) = compile_script(&script, modules, &sink_targets, max_operations)?;
        Ok(Self { engine, ast })
    }

    pub fn process(
        &self,
        event: Value,
        request: ProcessorRequestContext,
    ) -> Result<ProcessorOutput> {
        let input = to_dynamic(event).map_err(|err| anyhow!(err.to_string()))?;
        let mut scope = Scope::new();
        let processor_context = enter_processor_context();
        let _: Dynamic = self
            .engine
            .call_fn(&mut scope, &self.ast, "process", (input, request))
            .map_err(|err| anyhow!(err.to_string()))?;
        parse_processor_output(processor_context.deliveries())
    }
}

impl ProcessorRuntime for ProcessorState {
    fn process_event(
        &self,
        _project_id: i32,
        event: Value,
        request: ProcessorRequestContext,
    ) -> Result<ProcessorOutput> {
        self.process(event, request)
    }
}

#[derive(Clone)]
pub struct ProcessorRegistryState {
    router: Arc<RwLock<Arc<ProcessorRouter>>>,
    repository: Option<ProcessorRepository>,
    version: Arc<AtomicU64>,
    refresh_lock: Arc<AsyncMutex<()>>,
}

impl ProcessorRegistryState {
    pub fn from_processor(processor: ProcessorState) -> Self {
        Self {
            router: Arc::new(RwLock::new(Arc::new(ProcessorRouter {
                default: processor,
                projects: HashMap::new(),
            }))),
            repository: None,
            version: Arc::new(AtomicU64::new(0)),
            refresh_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub async fn load(repository: ProcessorRepository) -> Result<Self> {
        let (router, version) = load_processor_router_snapshot(&repository).await?;
        Ok(Self {
            router: Arc::new(RwLock::new(Arc::new(router))),
            repository: Some(repository),
            version: Arc::new(AtomicU64::new(version)),
            refresh_lock: Arc::new(AsyncMutex::new(())),
        })
    }

    pub async fn refresh_if_needed(&self) -> Result<bool> {
        let Some(repository) = self.repository.as_ref() else {
            return Ok(false);
        };

        let _guard = self.refresh_lock.lock().await;
        let current_version = self.version.load(Ordering::Acquire);
        let latest_version = repository.processor_scripts_version().await?;
        if latest_version == current_version {
            return Ok(false);
        }

        let (router, version) = load_processor_router_snapshot(repository).await?;
        if version <= self.version.load(Ordering::Acquire) {
            return Ok(false);
        }

        let mut guard = self
            .router
            .write()
            .expect("processor router write lock poisoned");
        *guard = Arc::new(router);
        self.version.store(version, Ordering::Release);
        Ok(true)
    }

    fn current_router(&self) -> Arc<ProcessorRouter> {
        self.router
            .read()
            .expect("processor router read lock poisoned")
            .clone()
    }
}

impl ProcessorRuntime for ProcessorRegistryState {
    fn process_event(
        &self,
        project_id: i32,
        event: Value,
        request: ProcessorRequestContext,
    ) -> Result<ProcessorOutput> {
        self.current_router()
            .processor_for_project(project_id)
            .process(event, request)
    }
}

struct ProcessorRouter {
    default: ProcessorState,
    projects: HashMap<i32, ProcessorState>,
}

impl ProcessorRouter {
    fn processor_for_project(&self, project_id: i32) -> &ProcessorState {
        self.projects.get(&project_id).unwrap_or(&self.default)
    }
}

async fn load_processor_router_snapshot(
    repository: &ProcessorRepository,
) -> Result<(ProcessorRouter, u64)> {
    loop {
        let version_before = repository.processor_scripts_version().await?;
        let sink_targets = repository.enabled_sink_ids().await?;
        let default =
            compile_runtime_script(repository.default_runtime_script().await?, &sink_targets)?;
        let project_scripts = repository.list_enabled_runtime_project_processors().await?;
        let version_after = repository.processor_scripts_version().await?;

        if version_before != version_after {
            continue;
        }

        let mut projects = HashMap::new();
        for (project_id, script) in project_scripts {
            projects.insert(project_id, compile_runtime_script(script, &sink_targets)?);
        }

        return Ok((ProcessorRouter { default, projects }, version_after));
    }
}

fn compile_runtime_script(
    script: RuntimeProcessorScript,
    sink_targets: &[String],
) -> Result<ProcessorState> {
    ProcessorState::new_with_sink_targets(
        script.entry_source.clone(),
        script.resolver_modules(),
        sink_targets.to_vec(),
        default_processor_max_operations(),
    )
}

fn compile_script(
    script: &str,
    modules: Vec<(String, String)>,
    sink_targets: &[String],
    max_operations: u64,
) -> Result<(Arc<Engine>, AST)> {
    let mut engine = Engine::new();
    engine.set_max_operations(max_operations);
    engine.set_max_expr_depths(0, 0);
    register_api(&mut engine);
    let mut scope = Scope::new();
    push_sink_target_constants(&mut scope, sink_targets);
    register_script_modules(&mut engine, &scope, modules)?;
    let ast = engine.compile_into_self_contained(&scope, script)?;
    Ok((Arc::new(engine), ast))
}

fn register_script_modules(
    engine: &mut Engine,
    scope: &Scope,
    modules: Vec<(String, String)>,
) -> Result<()> {
    if modules.is_empty() {
        return Ok(());
    }

    let mut resolver = StaticModuleResolver::new();
    for (name, script) in modules {
        let ast = engine
            .compile_with_scope(scope, script)
            .map_err(|err| anyhow!("failed to compile Rhai module `{name}`: {err}"))?;
        let module = Module::eval_ast_as_new(scope.clone(), &ast, engine)
            .map_err(|err| anyhow!("failed to initialize Rhai module `{name}`: {err}"))?;
        resolver.insert(name, module);
    }
    engine.set_module_resolver(resolver);
    Ok(())
}

fn default_sink_targets() -> Vec<String> {
    DEFAULT_SINK_TARGETS
        .iter()
        .map(|target| (*target).to_string())
        .collect()
}

fn parse_processor_output(deliveries: Vec<ProcessorDelivery>) -> Result<ProcessorOutput> {
    Ok(ProcessorOutput { deliveries })
}

#[cfg(test)]
mod tests {
    use super::{compile_script, ProcessorState};
    use crate::ingest::processor::ProcessorRequestContext;
    use serde_json::json;

    #[test]
    fn compile_script_accepts_database_supplied_modules() {
        let entry = r#"
import "custom" as custom;

fn process(event, request) {
    event = custom::custom_step(event);
    emit(SINK_STDOUT, event);
}
"#;
        let modules = vec![(
            "custom".to_string(),
            r#"
fn custom_step(event) {
    event["xcontext"]["custom_step"] = true;
    return event;
}
"#
            .to_string(),
        )];

        compile_script(entry, modules, &["stdout".to_string()], 10_000)
            .expect("compiled processor modules");
    }

    #[test]
    fn processor_does_not_expose_drop_decision_helper() {
        let processor = ProcessorState::new(
            r#"
fn process(event, request) {
    return drop("do not persist this event");
}
"#
            .to_string(),
            10_000,
        )
        .expect("processor should compile");

        let error = match processor.process(
            json!({
                "appid": "APPID",
                "xwhat": "custom_event",
                "xcontext": {}
            }),
            ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
        ) {
            Ok(_) => panic!("drop helper should not be available"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("Function not found: drop"));
    }

    #[test]
    fn processor_requires_process_entrypoint() {
        let processor = ProcessorState::new(
            r#"
fn main(event, request) {
    emit(SINK_STDOUT, event);
}
"#
            .to_string(),
            10_000,
        )
        .expect("processor should compile");

        let error = match processor.process(
            json!({
                "appid": "APPID",
                "xwhat": "custom_event",
                "xcontext": {}
            }),
            ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
        ) {
            Ok(_) => panic!("main entrypoint should not be called"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("Function not found: process"));
    }

    #[test]
    fn processor_collects_emit_deliveries_per_event() {
        let processor = ProcessorState::new_with_sink_targets(
            r#"
fn process(event, request) {
    emit(SINK_KAFKA_RAW, event);
    event["xcontext"]["normalized"] = true;
    emit(SINK_KAFKA_VALID, event);
    emit(SINK_KAFKA_RAW, event);
}
"#
            .to_string(),
            Vec::new(),
            vec!["kafka_raw".to_string(), "kafka_valid".to_string()],
            10_000,
        )
        .expect("processor should compile");

        let output = processor
            .process(
                json!({
                    "appid": "APPID",
                    "xwhat": "custom_event",
                    "xcontext": {}
                }),
                ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
            )
            .expect("processor should run");

        assert_eq!(output.deliveries.len(), 3);
        assert_eq!(output.deliveries[0].target, "kafka_raw");
        assert_eq!(output.deliveries[0].event["xcontext"], json!({}));
        assert_eq!(output.deliveries[1].target, "kafka_valid");
        assert_eq!(
            output.deliveries[1].event["xcontext"]["normalized"],
            json!(true)
        );
        assert_eq!(output.deliveries[2].target, "kafka_raw");
    }

    #[test]
    fn processor_allows_zero_emits() {
        let processor = ProcessorState::new(
            r#"
fn process(event, request) {
}
"#
            .to_string(),
            10_000,
        )
        .expect("processor should compile");

        let output = processor
            .process(
                json!({
                    "appid": "APPID",
                    "xwhat": "custom_event",
                    "xcontext": {}
                }),
                ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
            )
            .expect("processor without emit should be a normal drop");

        assert!(output.deliveries.is_empty());
    }

    #[test]
    fn processor_supports_event_validation_helpers_inline() {
        let processor = ProcessorState::new(
            r#"
fn process(event, request) {
    let xwhat = event.required("xwhat").string().min(1);
    let os = event.required("xcontext.os").string().ignore_case().enum(["ios", "android"]);

    if os.eq("ios") {
        event.any(["xcontext.idfa", "xcontext.caid"]).required();
    }

    if xwhat.eq("install") {
        event.required("xcontext.installid").string().min(1);
    }

    emit(SINK_EVENTS, event);
}
"#
            .to_string(),
            10_000,
        )
        .expect("processor should compile");

        let output = processor
            .process(
                json!({
                    "appid": "APPID",
                    "xwhat": "install",
                    "xcontext": {
                        "os": "ios",
                        "idfa": "idfa-1",
                        "installid": "iid-1"
                    }
                }),
                ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
            )
            .expect("processor should accept inline validation helpers");

        assert_eq!(output.deliveries.len(), 1);
        assert_eq!(output.deliveries[0].target, "events");

        let error = match processor.process(
            json!({
                "appid": "APPID",
                "xwhat": "install",
                "xcontext": {
                    "os": "ios",
                    "installid": "iid-1"
                }
            }),
            ProcessorRequestContext::new(None, "POST", "/ingest", Default::default()),
        ) {
            Ok(_) => panic!("missing ios identifier should fail"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("at least one field is required: xcontext.idfa, xcontext.caid"));
    }
}
