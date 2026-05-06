use crate::rules::Rules;
use crate::utils::get_host_ip;
use rhai::packages::Package;
use rhai::serde::from_dynamic;
use rhai::{def_package, Dynamic, Engine, EvalAltResult, ImmutableString, Map};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tracing::warn;

// Host-side APIs exposed to Rhai processor scripts.
def_package! {
    pub ProcessorApiPackage(module) {
        module.set_native_fn("epoch_ms", epoch_ms);
        module.set_native_fn("host_ip", host_ip);
        module.set_native_fn("ingest4x_version", ingest4x_version);
        module.set_native_fn("validate", validate);
        module.set_native_fn("emit", emit);
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
    received_at_ms: Option<u64>,
}

#[derive(Clone)]
struct ProcessorContext {
    rules: Rules,
    event_name: String,
    deliveries: Rc<RefCell<Vec<ProcessorDelivery>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessorDelivery {
    pub target: String,
    pub event: Value,
}

thread_local! {
    static PROCESSOR_CONTEXT: RefCell<Option<ProcessorContext>> = const { RefCell::new(None) };
}

pub(crate) struct ProcessorContextGuard {
    previous: Option<ProcessorContext>,
    deliveries: Rc<RefCell<Vec<ProcessorDelivery>>>,
}

impl Drop for ProcessorContextGuard {
    fn drop(&mut self) {
        PROCESSOR_CONTEXT.with(|context| {
            context.replace(self.previous.take());
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
            received_at_ms: None,
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_received_at_ms(mut self, received_at_ms: u64) -> Self {
        self.received_at_ms = Some(received_at_ms);
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

    fn received_at_ms(&mut self) -> Dynamic {
        self.received_at_ms
            .map(|received_at_ms| (received_at_ms as rhai::INT).into())
            .unwrap_or(Dynamic::UNIT)
    }
}

pub(crate) fn register_api(engine: &mut Engine) {
    ProcessorApiPackage::new().register_into_engine(engine);
}

pub(crate) fn enter_processor_context(rules: Rules, event_name: String) -> ProcessorContextGuard {
    let deliveries = Rc::new(RefCell::new(Vec::new()));
    let context = ProcessorContext {
        rules,
        event_name,
        deliveries: Rc::clone(&deliveries),
    };
    let previous = PROCESSOR_CONTEXT.with(|current| current.replace(Some(context)));
    ProcessorContextGuard {
        previous,
        deliveries,
    }
}

impl ProcessorContextGuard {
    pub(crate) fn deliveries(&self) -> Vec<ProcessorDelivery> {
        self.deliveries.borrow().clone()
    }
}

fn register_request_api(engine: &mut Engine) {
    engine.register_type::<ProcessorRequestContext>();
    engine.register_fn("ip", ProcessorRequestContext::ip);
    engine.register_fn("method", ProcessorRequestContext::method);
    engine.register_fn("path", ProcessorRequestContext::path);
    engine.register_fn("header", ProcessorRequestContext::header);
    engine.register_fn("request_id", ProcessorRequestContext::request_id);
    engine.register_fn("received_at_ms", ProcessorRequestContext::received_at_ms);
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

fn validate(event: Dynamic) -> Result<Map, Box<EvalAltResult>> {
    let value: Value = from_dynamic(&event)
        .map_err(|err| EvalAltResult::ErrorRuntime(err.to_string().into(), rhai::Position::NONE))?;
    let result =
        PROCESSOR_CONTEXT.with(|context| -> std::result::Result<_, Box<EvalAltResult>> {
            let context = context.borrow();
            let context = context.as_ref().ok_or_else(|| {
                EvalAltResult::ErrorRuntime(
                    "validate(event) called outside processor validation context".into(),
                    rhai::Position::NONE,
                )
            })?;
            Ok(context.rules.validate(&context.event_name, &value))
        })?;
    let mut output = Map::new();
    match result {
        Ok(()) => {
            output.insert("ok".into(), true.into());
        }
        Err(err) => {
            output.insert("ok".into(), false.into());
            output.insert("code".into(), err.code().into());
            output.insert("error".into(), err.message().into());
            output.insert("message".into(), err.message().into());
            if let Some(path) = err.path() {
                output.insert("path".into(), path.into());
            }
        }
    }
    Ok(output)
}

fn emit(target: &str, event: Dynamic) -> Result<(), Box<EvalAltResult>> {
    if target.trim().is_empty() {
        warn!("processor emit ignored empty sink target");
        return Ok(());
    }
    let event: Value = match from_dynamic(&event) {
        Ok(event) => event,
        Err(error) => {
            warn!(error = %error, "processor emit ignored non-json event");
            return Ok(());
        }
    };

    PROCESSOR_CONTEXT.with(|context| {
        let context = context.borrow();
        let context = context.as_ref().ok_or_else(|| {
            EvalAltResult::ErrorRuntime(
                "emit(target, event) called outside processor context".into(),
                rhai::Position::NONE,
            )
        })?;
        context.deliveries.borrow_mut().push(ProcessorDelivery {
            target: target.to_string(),
            event,
        });
        Ok(())
    })
}
