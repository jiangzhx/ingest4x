use crate::rules::Rules;
use crate::utils::get_host_ip;
use rhai::packages::Package;
use rhai::serde::from_dynamic;
use rhai::{def_package, Dynamic, Engine, EvalAltResult, ImmutableString, Map};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;

// Host-side APIs exposed to Rhai processor scripts.
def_package! {
    pub ProcessorApiPackage(module) {
        module.set_native_fn("epoch_ms", epoch_ms);
        module.set_native_fn("host_ip", host_ip);
        module.set_native_fn("ingest4x_version", ingest4x_version);
        module.set_native_fn("accept", accept);
        module.set_native_fn("reject", reject);
        module.set_native_fn("validate", validate);
    } |> |engine| {
        register_request_api(engine);
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProcessorRequestContext {
    ip: Option<String>,
    method: String,
    path: String,
    headers: HashMap<String, String>,
    request_id: Option<String>,
}

#[derive(Clone)]
struct ValidationContext {
    rules: Rules,
    event_name: String,
}

thread_local! {
    static VALIDATION_CONTEXT: RefCell<Option<ValidationContext>> = const { RefCell::new(None) };
}

pub(crate) struct ValidationContextGuard(Option<ValidationContext>);

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
            request_id: None,
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
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

    fn request_id(&mut self) -> Dynamic {
        self.request_id
            .as_ref()
            .map(|request_id| request_id.to_string().into())
            .unwrap_or(Dynamic::UNIT)
    }
}

pub(crate) fn register_api(engine: &mut Engine) {
    ProcessorApiPackage::new().register_into_engine(engine);
}

pub(crate) fn enter_validation_context(rules: Rules, event_name: String) -> ValidationContextGuard {
    let context = ValidationContext { rules, event_name };
    let previous = VALIDATION_CONTEXT.with(|current| current.replace(Some(context)));
    ValidationContextGuard(previous)
}

fn register_request_api(engine: &mut Engine) {
    engine.register_type::<ProcessorRequestContext>();
    engine.register_fn("ip", ProcessorRequestContext::ip);
    engine.register_fn("method", ProcessorRequestContext::method);
    engine.register_fn("path", ProcessorRequestContext::path);
    engine.register_fn("header", ProcessorRequestContext::header);
    engine.register_fn("request_id", ProcessorRequestContext::request_id);
}

fn epoch_ms() -> Result<rhai::INT, Box<EvalAltResult>> {
    Ok(crate::current_timestamp_as_u64() as rhai::INT)
}

fn host_ip() -> Result<ImmutableString, Box<EvalAltResult>> {
    Ok(get_host_ip().into())
}

fn ingest4x_version() -> Result<ImmutableString, Box<EvalAltResult>> {
    Ok(env!("CARGO_PKG_VERSION").into())
}

fn accept(event: Dynamic) -> Result<Map, Box<EvalAltResult>> {
    let mut output = Map::new();
    output.insert("status".into(), "accepted".into());
    output.insert("event".into(), event);
    Ok(output)
}

fn reject(event: Dynamic, error: &str) -> Result<Map, Box<EvalAltResult>> {
    let mut output = Map::new();
    output.insert("status".into(), "rejected".into());
    output.insert("event".into(), event);
    output.insert("error".into(), error.into());
    Ok(output)
}

fn validate(event: Dynamic) -> Result<Map, Box<EvalAltResult>> {
    let value: Value = from_dynamic(&event)
        .map_err(|err| EvalAltResult::ErrorRuntime(err.to_string().into(), rhai::Position::NONE))?;
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
}
