use crate::rules::Rules;
use crate::settings::default_processor_max_operations;
use crate::utils::get_host_ip;
use anyhow::{anyhow, Result};
use rhai::module_resolvers::StaticModuleResolver;
use rhai::serde::{from_dynamic, to_dynamic};
use rhai::{Dynamic, Engine, EvalAltResult, ImmutableString, Map, Module, Scope, AST};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_RHAI_PROCESSOR_PATH: &str = "pipeline/main.rhai";

#[derive(Clone)]
pub struct ProcessorState {
    engine: Arc<Engine>,
    ast: AST,
}

struct ProcessorScript {
    entry: String,
    modules: Vec<(String, String)>,
}

pub enum ProcessorOutput {
    Accepted(Value),
    Rejected { event: Value, error: String },
    Dropped { reason: String },
}

#[derive(Clone, Debug, Default)]
pub struct ProcessorRequestContext {
    ip: Option<String>,
    method: String,
    path: String,
    headers: HashMap<String, String>,
}

#[derive(Clone)]
struct ValidationContext {
    rules: Rules,
    event_name: String,
}

thread_local! {
    static VALIDATION_CONTEXT: RefCell<Option<ValidationContext>> = const { RefCell::new(None) };
}

struct ValidationContextGuard(Option<ValidationContext>);

impl Drop for ValidationContextGuard {
    fn drop(&mut self) {
        VALIDATION_CONTEXT.with(|context| {
            context.replace(self.0.take());
        });
    }
}

impl ProcessorRequestContext {
    pub fn new(
        ip: Option<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            ip,
            method: method.into(),
            path: path.into(),
            headers,
        }
    }

    fn ip(&mut self) -> Dynamic {
        self.ip
            .as_ref()
            .map(|ip| ip.to_string().into())
            .unwrap_or(Dynamic::UNIT)
    }

    fn method(&mut self) -> ImmutableString {
        self.method.clone().into()
    }

    fn path(&mut self) -> ImmutableString {
        self.path.clone().into()
    }

    fn header(&mut self, name: &str) -> Dynamic {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(|value| value.to_string().into())
            .unwrap_or(Dynamic::UNIT)
    }
}

impl ProcessorState {
    pub fn from_default_entry() -> Result<Self> {
        let script = read_processor_script(DEFAULT_RHAI_PROCESSOR_PATH)?;
        Self::new_with_modules(
            script.entry,
            script.modules,
            default_processor_max_operations(),
        )
    }

    pub fn new(script: String, max_operations: u64) -> Result<Self> {
        Self::new_with_modules(script, Vec::new(), max_operations)
    }

    fn new_with_modules(
        script: String,
        modules: Vec<(String, String)>,
        max_operations: u64,
    ) -> Result<Self> {
        let (engine, ast) = compile_script(&script, modules, max_operations)?;
        Ok(Self { engine, ast })
    }

    pub fn process(
        &self,
        event: Value,
        rules: Rules,
        request: ProcessorRequestContext,
    ) -> Result<ProcessorOutput> {
        let event_name = event
            .get("xwhat")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let input = to_dynamic(event).map_err(|err| anyhow!(err.to_string()))?;
        let mut scope = Scope::new();
        let _validation_context = enter_validation_context(rules, event_name);
        let result: Dynamic = self
            .engine
            .call_fn(&mut scope, &self.ast, "main", (input, request))
            .map_err(|err| anyhow!(err.to_string()))?;
        parse_processor_output(result)
    }
}

fn compile_script(
    script: &str,
    modules: Vec<(String, String)>,
    max_operations: u64,
) -> Result<(Arc<Engine>, AST)> {
    let mut engine = Engine::new();
    engine.set_max_operations(max_operations);
    engine.set_max_expr_depths(0, 0);
    register_processor_helpers(&mut engine);
    register_validate_helper(&mut engine);
    register_script_modules(&mut engine, modules)?;
    let ast = engine.compile_into_self_contained(&Scope::new(), script)?;
    Ok((Arc::new(engine), ast))
}

fn register_script_modules(engine: &mut Engine, modules: Vec<(String, String)>) -> Result<()> {
    if modules.is_empty() {
        return Ok(());
    }

    let mut resolver = StaticModuleResolver::new();
    for (name, script) in modules {
        let ast = engine
            .compile(script)
            .map_err(|err| anyhow!("failed to compile Rhai module `{name}`: {err}"))?;
        let module = Module::eval_ast_as_new(Scope::new(), &ast, engine)
            .map_err(|err| anyhow!("failed to initialize Rhai module `{name}`: {err}"))?;
        resolver.insert(name, module);
    }
    engine.set_module_resolver(resolver);
    Ok(())
}

fn register_processor_helpers(engine: &mut Engine) {
    engine.register_type::<ProcessorRequestContext>();
    engine.register_fn("ip", ProcessorRequestContext::ip);
    engine.register_fn("method", ProcessorRequestContext::method);
    engine.register_fn("path", ProcessorRequestContext::path);
    engine.register_fn("header", ProcessorRequestContext::header);
    engine.register_fn("epoch_ms", crate::current_timestamp_as_u64);
    engine.register_fn("host_ip", get_host_ip);
    engine.register_fn("ingest4x_version", || env!("CARGO_PKG_VERSION"));
    engine.register_fn("accept", |event: Dynamic| -> Map {
        let mut output = Map::new();
        output.insert("status".into(), "accepted".into());
        output.insert("event".into(), event);
        output
    });
    engine.register_fn("reject", |event: Dynamic, error: &str| -> Map {
        let mut output = Map::new();
        output.insert("status".into(), "rejected".into());
        output.insert("event".into(), event);
        output.insert("error".into(), error.into());
        output
    });
    engine.register_fn("drop", |reason: &str| -> Map {
        let mut output = Map::new();
        output.insert("status".into(), "dropped".into());
        output.insert("reason".into(), reason.into());
        output
    });
}

fn enter_validation_context(rules: Rules, event_name: String) -> ValidationContextGuard {
    let context = ValidationContext { rules, event_name };
    let previous = VALIDATION_CONTEXT.with(|current| current.replace(Some(context)));
    ValidationContextGuard(previous)
}

fn register_validate_helper(engine: &mut Engine) {
    engine.register_fn(
        "validate",
        |event: Dynamic| -> Result<Map, Box<EvalAltResult>> {
            let value: Value = from_dynamic(&event).map_err(|err| {
                EvalAltResult::ErrorRuntime(err.to_string().into(), rhai::Position::NONE)
            })?;
            let result: anyhow::Result<()> = VALIDATION_CONTEXT.with(
                |context| -> std::result::Result<anyhow::Result<()>, Box<EvalAltResult>> {
                    let context = context.borrow();
                    let context = context.as_ref().ok_or_else(|| {
                        EvalAltResult::ErrorRuntime(
                            "validate(event) called outside processor validation context".into(),
                            rhai::Position::NONE,
                        )
                    })?;
                    Ok(context.rules.validate(&context.event_name, &value))
                },
            )?;
            let mut output = Map::new();
            match result {
                Ok(()) => {
                    output.insert("ok".into(), true.into());
                }
                Err(err) => {
                    output.insert("ok".into(), false.into());
                    output.insert("error".into(), err.to_string().into());
                }
            }
            Ok(output)
        },
    );
}

fn parse_processor_output(result: Dynamic) -> Result<ProcessorOutput> {
    let value: Value = from_dynamic(&result).map_err(|err| anyhow!(err.to_string()))?;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("processor result must include string status"))?;

    match status {
        "accepted" => Ok(ProcessorOutput::Accepted(
            value
                .get("event")
                .cloned()
                .ok_or_else(|| anyhow!("accepted processor result must include event"))?,
        )),
        "rejected" => Ok(ProcessorOutput::Rejected {
            event: value
                .get("event")
                .cloned()
                .ok_or_else(|| anyhow!("rejected processor result must include event"))?,
            error: value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("processor rejected event")
                .to_string(),
        }),
        "dropped" => Ok(ProcessorOutput::Dropped {
            reason: value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("processor dropped event")
                .to_string(),
        }),
        other => Err(anyhow!("unsupported processor status `{other}`")),
    }
}

fn read_processor_script(path: impl AsRef<Path>) -> Result<ProcessorScript> {
    let path = path.as_ref();
    match fs::read_to_string(path) {
        Ok(script) => {
            let modules = read_processor_modules(path)?;
            Ok(ProcessorScript {
                entry: script,
                modules,
            })
        }
        Err(err) => Err(anyhow!(
            "failed to read Rhai processor `{}`: {err}",
            path.display()
        )),
    }
}

fn read_processor_modules(entry_path: &Path) -> Result<Vec<(String, String)>> {
    let directory = entry_path.parent().unwrap_or_else(|| Path::new("."));
    let mut paths = fs::read_dir(directory)
        .map_err(|err| {
            anyhow!(
                "failed to read Rhai pipeline directory `{}`: {err}",
                directory.display()
            )
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<PathBuf>>>()
        .map_err(|err| {
            anyhow!(
                "failed to list Rhai pipeline directory `{}`: {err}",
                directory.display()
            )
        })?;

    paths.sort();

    let mut modules = Vec::new();
    for path in paths {
        if path == entry_path {
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("rhai") {
            continue;
        }

        let module_name = rhai_module_name(&path)?;
        let script = fs::read_to_string(&path)
            .map_err(|err| anyhow!("failed to read Rhai processor `{}`: {err}", path.display()))?;
        modules.push((module_name, script));
    }

    Ok(modules)
}

fn rhai_module_name(path: &Path) -> Result<String> {
    let module_name = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Rhai module file `{}` must have a valid name",
                path.display()
            )
        })?;

    let mut chars = module_name.chars();
    let Some(first) = chars.next() else {
        return Err(anyhow!(
            "Rhai module file `{}` must have a non-empty name",
            path.display()
        ));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(anyhow!(
            "Rhai module file `{}` must be a valid identifier",
            path.display()
        ));
    }

    Ok(module_name.to_string())
}

#[cfg(test)]
mod tests {
    use super::{compile_script, read_processor_script};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn default_entry_loads_all_neighbor_rhai_files_before_main() {
        let temp = tempdir().expect("temp dir");
        let pipeline = temp.path().join("pipeline");
        fs::create_dir(&pipeline).expect("create pipeline dir");
        fs::write(
            pipeline.join("main.rhai"),
            r#"
import "custom" as custom;

fn main(event, request) {
    event = custom::custom_step(event);
    return accept(event);
}
"#,
        )
        .expect("write main");
        fs::write(
            pipeline.join("custom.rhai"),
            r#"
fn custom_step(event) {
    event["xcontext"]["custom_step"] = true;
    return event;
}
"#,
        )
        .expect("write custom");

        let script = read_processor_script(pipeline.join("main.rhai")).expect("read pipeline");

        assert!(script.entry.contains("import \"custom\" as custom;"));
        assert!(script.entry.contains("fn main"));
        assert_eq!(script.modules[0].0, "custom");
        assert!(script.modules[0].1.contains("fn custom_step"));
        compile_script(&script.entry, script.modules, 10_000).expect("compiled processor modules");
    }
}
