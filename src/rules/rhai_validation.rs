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
    presence: FieldPresence,
    field_type: Option<FieldType>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldPresence {
    Inspect,
    Required,
    Optional,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumberComparison {
    Gt,
    Gte,
    Lt,
    Lte,
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
            presence: FieldPresence::Inspect,
            field_type: None,
        }
    }

    fn required(&mut self, path: &str) -> FieldRef {
        let field = FieldRef {
            state: Arc::clone(&self.state),
            path: path.to_string(),
            presence: FieldPresence::Required,
            field_type: None,
        };
        field.validate_required_presence();
        field
    }

    fn optional(&mut self, path: &str) -> FieldRef {
        FieldRef {
            state: Arc::clone(&self.state),
            path: path.to_string(),
            presence: FieldPresence::Optional,
            field_type: None,
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

    fn string(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::String)
    }

    fn number(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::Number)
    }

    fn integer(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::Integer)
    }

    fn boolean(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::Boolean)
    }

    fn object(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::Object)
    }

    fn array(&mut self) -> FieldRef {
        self.validate_chained_type(FieldType::Array)
    }

    fn min_int(&mut self, threshold: rhai::INT) -> FieldRef {
        self.min_length(threshold)
    }

    fn enum_strings(&mut self, values: Array) -> std::result::Result<FieldRef, Box<EvalAltResult>> {
        let enum_values = dynamic_array_to_string_enum(values, &self.path)?;
        if enum_values.is_empty() {
            return Err(script_error(format!(
                "enum values for field `{}` must not be empty",
                self.path
            )));
        }
        if self.field_type != Some(FieldType::String) {
            return Err(script_error(format!(
                "enum for field `{}` must follow string()",
                self.path
            )));
        }

        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = self.value_for_chained_validation(&state) else {
            return Ok(self.clone());
        };
        let Some(actual) = value.as_str() else {
            state
                .errors
                .push(type_mismatch_error(&self.path, FieldType::String));
            return Ok(self.clone());
        };
        if !enum_values.iter().any(|expected| expected == actual) {
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
        Ok(self.clone())
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
        self.compare_number(NumberComparison::Gt, threshold as f64)
    }

    fn gt_float(&mut self, threshold: rhai::FLOAT) -> FieldRef {
        self.compare_number(NumberComparison::Gt, threshold)
    }

    fn gte_int(&mut self, threshold: rhai::INT) -> FieldRef {
        self.compare_number(NumberComparison::Gte, threshold as f64)
    }

    fn gte_float(&mut self, threshold: rhai::FLOAT) -> FieldRef {
        self.compare_number(NumberComparison::Gte, threshold)
    }

    fn lt_int(&mut self, threshold: rhai::INT) -> FieldRef {
        self.compare_number(NumberComparison::Lt, threshold as f64)
    }

    fn lt_float(&mut self, threshold: rhai::FLOAT) -> FieldRef {
        self.compare_number(NumberComparison::Lt, threshold)
    }

    fn lte_int(&mut self, threshold: rhai::INT) -> FieldRef {
        self.compare_number(NumberComparison::Lte, threshold as f64)
    }

    fn lte_float(&mut self, threshold: rhai::FLOAT) -> FieldRef {
        self.compare_number(NumberComparison::Lte, threshold)
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

    fn compare_number(&mut self, comparison: NumberComparison, threshold: f64) -> FieldRef {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = self.value_for_chained_validation(&state) else {
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
        if !comparison.matches(number, threshold) {
            state.errors.push(rules_error(
                RulesValidationCode::NumberConstraintFailed,
                format!(
                    "field `{}` must be {} {threshold}",
                    self.path,
                    comparison.description()
                ),
                Some(self.path.clone()),
            ));
        }
        self.clone()
    }

    fn validate_required_presence(&self) {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let value = lookup_path(&state.payload, &self.path);
        if !exists_non_null(value) {
            state.errors.push(rules_error(
                RulesValidationCode::RequiredFieldMissing,
                format!("missing required field `{}`", self.path),
                Some(self.path.clone()),
            ));
        }
    }

    fn validate_chained_type(&mut self, field_type: FieldType) -> FieldRef {
        self.field_type = Some(field_type);
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = self.value_for_chained_validation(&state) else {
            return self.clone();
        };
        if !field_type.matches(value) {
            state
                .errors
                .push(type_mismatch_error(&self.path, field_type));
        }
        self.clone()
    }

    fn min_length(&mut self, threshold: rhai::INT) -> FieldRef {
        let mut state = self.state.lock().expect("validation state lock poisoned");
        let Some(value) = self.value_for_chained_validation(&state) else {
            return self.clone();
        };
        let Some(text) = value.as_str() else {
            return self.clone();
        };
        if text.chars().count() < threshold.max(0) as usize {
            state.errors.push(rules_error(
                RulesValidationCode::FieldTypeMismatch,
                format!(
                    "field `{}` length must be at least {}",
                    self.path, threshold
                ),
                Some(self.path.clone()),
            ));
        }
        self.clone()
    }

    fn value_for_chained_validation<'a>(&self, state: &'a ValidationState) -> Option<&'a Value> {
        let value = lookup_path(&state.payload, &self.path);
        match self.presence {
            FieldPresence::Optional if !exists_non_null(value) => None,
            _ => value.filter(|value| !value.is_null()),
        }
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
    engine.register_fn("required", ValidationEvent::required);
    engine.register_fn("optional", ValidationEvent::optional);
    engine.register_fn("any", ValidationEvent::any);
    engine.register_fn("result", ValidationEvent::result);

    engine.register_fn("required", FieldRef::required);
    engine.register_fn("optional", FieldRef::optional);
    engine.register_fn("string", FieldRef::string);
    engine.register_fn("number", FieldRef::number);
    engine.register_fn("integer", FieldRef::integer);
    engine.register_fn("boolean", FieldRef::boolean);
    engine.register_fn("object", FieldRef::object);
    engine.register_fn("array", FieldRef::array);
    engine.register_fn("min", FieldRef::min_int);
    engine.register_fn("enum", FieldRef::enum_strings);
    engine.register_fn("one_of", FieldRef::one_of);
    engine.register_fn("gt", FieldRef::gt_int);
    engine.register_fn("gt", FieldRef::gt_float);
    engine.register_fn("gte", FieldRef::gte_int);
    engine.register_fn("gte", FieldRef::gte_float);
    engine.register_fn("lt", FieldRef::lt_int);
    engine.register_fn("lt", FieldRef::lt_float);
    engine.register_fn("lte", FieldRef::lte_int);
    engine.register_fn("lte", FieldRef::lte_float);
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

impl FieldType {
    fn matches(self, value: &Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Boolean => value.is_boolean(),
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
        }
    }

    const fn display(self) -> &'static str {
        match self {
            Self::String => "String",
            Self::Number => "Number",
            Self::Integer => "Integer",
            Self::Boolean => "Boolean",
            Self::Object => "Object",
            Self::Array => "Array",
        }
    }
}

impl NumberComparison {
    fn matches(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Gte => value >= threshold,
            Self::Lt => value < threshold,
            Self::Lte => value <= threshold,
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Gt => "greater than",
            Self::Gte => "greater than or equal to",
            Self::Lt => "less than",
            Self::Lte => "less than or equal to",
        }
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

fn exists_non_null(value: Option<&Value>) -> bool {
    !matches!(value, Some(Value::Null) | None)
}

fn dynamic_array_to_strings(values: Array) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| value.try_cast::<ImmutableString>())
        .map(|value| value.to_string())
        .collect()
}

fn dynamic_array_to_string_enum(
    values: Array,
    path: &str,
) -> std::result::Result<Vec<String>, Box<EvalAltResult>> {
    values
        .into_iter()
        .map(|value| {
            value
                .try_cast::<ImmutableString>()
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    script_error(format!(
                        "enum values for field `{path}` must all be strings"
                    ))
                })
        })
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

fn type_mismatch_error(path: &str, field_type: FieldType) -> RulesValidationError {
    rules_error(
        RulesValidationCode::FieldTypeMismatch,
        format!("field `{path}` expected type `{}`", field_type.display()),
        Some(path.to_string()),
    )
}

fn script_error(message: String) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorRuntime(
        message.into(),
        rhai::Position::NONE,
    ))
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
