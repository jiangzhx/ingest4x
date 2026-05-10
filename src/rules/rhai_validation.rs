use super::error::{RulesValidationCode, RulesValidationError};
use anyhow::{Context, Result};
use rhai::serde::to_dynamic;
use rhai::{Array, Dynamic, Engine, EvalAltResult, ImmutableString, Map, Scope, AST};
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct RhaiValidationRules {
    engine: Engine,
    ast: AST,
}

#[derive(Clone, Debug)]
struct ValidationEvent {
    state: Arc<Mutex<ValidationState>>,
}

#[derive(Clone, Debug)]
struct FieldRef {
    state: Arc<Mutex<ValidationState>>,
    path: String,
}

#[derive(Clone, Debug)]
struct AnyFields {
    state: Arc<Mutex<ValidationState>>,
    paths: Vec<String>,
}

#[derive(Clone, Debug)]
struct ValidationState {
    payload: Value,
    errors: Vec<RulesValidationError>,
}

impl RhaiValidationRules {
    pub(crate) fn compile(script: &str) -> Result<Arc<Self>> {
        let mut engine = Engine::new();
        register_validation_api(&mut engine);
        let ast = engine
            .compile(script)
            .context("failed to compile Rhai validation rules")?;
        Ok(Arc::new(Self { engine, ast }))
    }

    pub(crate) fn validate(
        &self,
        payload: &Value,
    ) -> std::result::Result<(), RulesValidationError> {
        let event = ValidationEvent::new(payload.clone());
        let script_event = event.clone();
        let _ = self
            .engine
            .call_fn::<Dynamic>(&mut Scope::new(), &self.ast, "validate", (script_event,))
            .map_err(|error| {
                RulesValidationError::new(
                    RulesValidationCode::ScriptExecutionFailed,
                    error.to_string(),
                    None::<String>,
                )
            })?;

        match event.first_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl ValidationEvent {
    fn new(payload: Value) -> Self {
        Self {
            state: Arc::new(Mutex::new(ValidationState {
                payload,
                errors: Vec::new(),
            })),
        }
    }

    fn field(&mut self, path: &str) -> FieldRef {
        FieldRef {
            state: Arc::clone(&self.state),
            path: path.to_string(),
        }
    }

    fn any(&mut self, paths: Array) -> AnyFields {
        AnyFields {
            state: Arc::clone(&self.state),
            paths: dynamic_array_to_strings(paths),
        }
    }

    fn result(&mut self) -> Map {
        let state = self.state.lock().expect("validation state lock poisoned");
        validation_result_map(state.errors.first())
    }

    fn first_error(&self) -> Option<RulesValidationError> {
        self.state
            .lock()
            .expect("validation state lock poisoned")
            .errors
            .first()
            .cloned()
    }
}

impl FieldRef {
    fn required(&mut self, field_type: &str) -> FieldRef {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let value = lookup_path(&state.payload, &self.path).cloned();
        if !is_present(value.as_ref()) {
            state.errors.push(rules_error(
                RulesValidationCode::RequiredFieldMissing,
                format!("missing required field `{}`", self.path),
                Some(self.path.clone()),
            ));
            return self.clone();
        }
        validate_type(&mut state, &self.path, field_type, value.as_ref());
        self.clone()
    }

    fn optional(&mut self, field_type: &str) -> FieldRef {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let value = lookup_path(&state.payload, &self.path).cloned();
        if is_present(value.as_ref()) {
            validate_type(&mut state, &self.path, field_type, value.as_ref());
        }
        self.clone()
    }

    fn one_of(&mut self, values: Array) -> FieldRef {
        let enum_values = dynamic_array_to_strings(values);
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = lookup_path(&state.payload, &self.path).filter(|value| !value.is_null())
        else {
            return self.clone();
        };
        let Some(actual) = value.as_str() else {
            state.errors.push(rules_error(
                RulesValidationCode::FieldTypeMismatch,
                format!("field `{}` must be a string to use enum", self.path),
                Some(self.path.clone()),
            ));
            return self.clone();
        };
        if !enum_values
            .iter()
            .any(|expected| expected.eq_ignore_ascii_case(actual))
        {
            state.errors.push(rules_error(
                RulesValidationCode::EnumValueInvalid,
                format!(
                    "field `{}` must be one of [{}]",
                    self.path,
                    enum_values.join(", ")
                ),
                Some(self.path.clone()),
            ));
        }
        self.clone()
    }

    fn gt_int(&mut self, threshold: rhai::INT) -> FieldRef {
        self.gt_number(threshold as f64)
    }

    fn gt_float(&mut self, threshold: rhai::FLOAT) -> FieldRef {
        self.gt_number(threshold)
    }

    fn eq(&mut self, expected: Dynamic) -> bool {
        let state = self.state.lock().expect("validation state lock poisoned");
        let Some(actual) = lookup_path(&state.payload, &self.path) else {
            return false;
        };
        let Ok(expected) = dynamic_to_json(expected) else {
            return false;
        };
        values_equal(actual, &expected)
    }

    fn exists(&mut self) -> bool {
        let state = self.state.lock().expect("validation state lock poisoned");
        is_present(lookup_path(&state.payload, &self.path))
    }

    fn missing(&mut self) -> bool {
        !self.exists()
    }

    fn value(&mut self) -> Dynamic {
        let state = self.state.lock().expect("validation state lock poisoned");
        lookup_path(&state.payload, &self.path)
            .and_then(|value| to_dynamic(value).ok())
            .unwrap_or(Dynamic::UNIT)
    }

    fn gt_number(&mut self, threshold: f64) -> FieldRef {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = lookup_path(&state.payload, &self.path).filter(|value| !value.is_null())
        else {
            return self.clone();
        };
        let Some(number) = value.as_f64() else {
            state.errors.push(rules_error(
                RulesValidationCode::NumberParseFailed,
                format!("field `{}` could not be represented as f64", self.path),
                Some(self.path.clone()),
            ));
            return self.clone();
        };
        if number <= threshold {
            state.errors.push(rules_error(
                RulesValidationCode::NumberConstraintFailed,
                format!("field `{}` must be greater than {threshold}", self.path),
                Some(self.path.clone()),
            ));
        }
        self.clone()
    }
}

impl AnyFields {
    fn required(&mut self) -> AnyFields {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        if !self
            .paths
            .iter()
            .any(|path| is_present(lookup_path(&state.payload, path)))
        {
            state.errors.push(rules_error(
                RulesValidationCode::ConditionalRequiredMissing,
                format!("at least one field is required: {}", self.paths.join(", ")),
                None::<String>,
            ));
        }
        self.clone()
    }
}

fn register_validation_api(engine: &mut Engine) {
    engine.register_type_with_name::<ValidationEvent>("ValidationEvent");
    engine.register_type_with_name::<FieldRef>("FieldRef");
    engine.register_type_with_name::<AnyFields>("AnyFields");

    engine.register_fn("field", ValidationEvent::field);
    engine.register_fn("any", ValidationEvent::any);
    engine.register_fn("result", ValidationEvent::result);

    engine.register_fn("required", FieldRef::required);
    engine.register_fn("optional", FieldRef::optional);
    engine.register_fn("one_of", FieldRef::one_of);
    engine.register_fn("gt", FieldRef::gt_int);
    engine.register_fn("gt", FieldRef::gt_float);
    engine.register_fn("eq", FieldRef::eq);
    engine.register_fn("exists", FieldRef::exists);
    engine.register_fn("missing", FieldRef::missing);
    engine.register_fn("value", FieldRef::value);

    engine.register_fn("required", AnyFields::required);
}

fn validate_type(state: &mut ValidationState, path: &str, field_type: &str, value: Option<&Value>) {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return;
    };

    let valid = match field_type {
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        _ => {
            state.errors.push(rules_error(
                RulesValidationCode::ScriptExecutionFailed,
                format!("unknown validation type `{field_type}`"),
                Some(path.to_string()),
            ));
            return;
        }
    };

    if !valid {
        state.errors.push(rules_error(
            RulesValidationCode::FieldTypeMismatch,
            format!(
                "field `{path}` expected type `{}`",
                display_type(field_type)
            ),
            Some(path.to_string()),
        ));
    }
}

fn display_type(field_type: &str) -> &'static str {
    match field_type {
        "string" => "String",
        "number" => "Number",
        "integer" => "Integer",
        "boolean" => "Boolean",
        "object" => "Object",
        "array" => "Array",
        _ => "Unknown",
    }
}

fn validation_result_map(error: Option<&RulesValidationError>) -> Map {
    let mut output = Map::new();
    match error {
        Some(error) => {
            output.insert("ok".into(), false.into());
            output.insert("code".into(), error.code().into());
            output.insert("error".into(), error.message().into());
            output.insert("message".into(), error.message().into());
            if let Some(path) = error.path() {
                output.insert("path".into(), path.into());
            }
        }
        None => {
            output.insert("ok".into(), true.into());
        }
    }
    output
}

fn lookup_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = payload;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn is_present(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Null) | None => false,
        Some(Value::String(text)) => !text.is_empty(),
        Some(_) => true,
    }
}

fn dynamic_array_to_strings(values: Array) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| value.try_cast::<ImmutableString>())
        .map(|value| value.to_string())
        .collect()
}

fn dynamic_to_json(value: Dynamic) -> std::result::Result<Value, Box<EvalAltResult>> {
    rhai::serde::from_dynamic(&value).map_err(|error| {
        Box::new(EvalAltResult::ErrorRuntime(
            error.to_string().into(),
            rhai::Position::NONE,
        ))
    })
}

fn values_equal(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::String(actual), Value::String(expected)) => actual.eq_ignore_ascii_case(expected),
        _ => actual == expected,
    }
}

fn rules_error(
    code: RulesValidationCode,
    message: String,
    path: Option<impl Into<String>>,
) -> RulesValidationError {
    RulesValidationError::new(code, message, path)
}
