use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use regex::RegexBuilder;
use rhai::serde::{from_dynamic, to_dynamic};
use rhai::{Array, Dynamic, Engine, EvalAltResult, ImmutableString, Map};
use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct FieldRef {
    payload: Value,
    path: String,
    presence: FieldPresence,
    field_type: Option<FieldType>,
    ignore_case: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AnyFields {
    payload: Value,
    paths: Vec<String>,
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

#[derive(Clone, Copy, Debug, PartialEq)]
enum NumberComparison {
    Gt,
    Gte,
    Lt,
    Lte,
}

pub(crate) fn register_event_api(engine: &mut Engine) {
    engine.register_type_with_name::<FieldRef>("FieldRef");
    engine.register_type_with_name::<AnyFields>("AnyFields");

    engine.register_fn("field", map_field);
    engine.register_fn("required", map_required);
    engine.register_fn("optional", map_optional);
    engine.register_fn("any", map_any);
    engine.register_fn("string", FieldRef::string);
    engine.register_fn("number", FieldRef::number);
    engine.register_fn("integer", FieldRef::integer);
    engine.register_fn("boolean", FieldRef::boolean);
    engine.register_fn("object", FieldRef::object);
    engine.register_fn("array", FieldRef::array);
    engine.register_fn("min", FieldRef::min_int);
    engine.register_fn("ignore_case", FieldRef::ignore_case);
    engine.register_fn("enum", FieldRef::enum_strings);
    engine.register_fn("matches", FieldRef::matches_pattern);
    engine.register_fn("date", FieldRef::date);
    engine.register_fn("time", FieldRef::time);
    engine.register_fn("datetime", FieldRef::datetime);
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

fn map_field(event: &mut Map, path: &str) -> Result<FieldRef, Box<EvalAltResult>> {
    Ok(FieldRef::new(
        map_to_json(event)?,
        path,
        FieldPresence::Inspect,
    ))
}

fn map_required(event: &mut Map, path: &str) -> Result<FieldRef, Box<EvalAltResult>> {
    let payload = map_to_json(event)?;
    let field = FieldRef::new(payload, path, FieldPresence::Required);
    if !exists_non_null(lookup_path(&field.payload, &field.path)) {
        return Err(script_error(format!(
            "missing required field `{}`",
            field.path
        )));
    }
    Ok(field)
}

fn map_optional(event: &mut Map, path: &str) -> Result<FieldRef, Box<EvalAltResult>> {
    Ok(FieldRef::new(
        map_to_json(event)?,
        path,
        FieldPresence::Optional,
    ))
}

fn map_any(event: &mut Map, paths: Array) -> Result<AnyFields, Box<EvalAltResult>> {
    Ok(AnyFields {
        payload: map_to_json(event)?,
        paths: dynamic_array_to_strings(paths),
    })
}

impl FieldRef {
    fn new(payload: Value, path: &str, presence: FieldPresence) -> Self {
        Self {
            payload,
            path: path.to_string(),
            presence,
            field_type: None,
            ignore_case: false,
        }
    }

    fn string(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::String)
    }

    fn number(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::Number)
    }

    fn integer(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::Integer)
    }

    fn boolean(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::Boolean)
    }

    fn object(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::Object)
    }

    fn array(&mut self) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_chained_type(FieldType::Array)
    }

    fn min_int(&mut self, threshold: rhai::INT) -> Result<FieldRef, Box<EvalAltResult>> {
        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(text) = value.as_str() else {
            return Ok(self.clone());
        };
        if text.chars().count() < threshold.max(0) as usize {
            return Err(script_error(format!(
                "field `{}` length must be at least {}",
                self.path, threshold
            )));
        }
        Ok(self.clone())
    }

    fn ignore_case(&mut self) -> FieldRef {
        self.ignore_case = true;
        self.clone()
    }

    fn enum_strings(&mut self, values: Array) -> Result<FieldRef, Box<EvalAltResult>> {
        if self.field_type != Some(FieldType::String) {
            return Err(script_error(format!(
                "enum for field `{}` must follow string()",
                self.path
            )));
        }
        let enum_values = dynamic_array_to_string_enum(values, &self.path)?;
        if enum_values.is_empty() {
            return Err(script_error(format!(
                "enum values for field `{}` must not be empty",
                self.path
            )));
        }

        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(actual) = value.as_str() else {
            return Err(type_mismatch_error(&self.path, FieldType::String));
        };
        if !enum_values
            .iter()
            .any(|expected| strings_equal(actual, expected, self.ignore_case))
        {
            return Err(script_error(format!(
                "field `{}` must be one of [{}]",
                self.path,
                enum_values.join(", ")
            )));
        }
        Ok(self.clone())
    }

    fn matches_pattern(&mut self, pattern: &str) -> Result<FieldRef, Box<EvalAltResult>> {
        if self.field_type != Some(FieldType::String) {
            return Err(script_error(format!(
                "regex for field `{}` must follow string()",
                self.path
            )));
        }
        let regex = RegexBuilder::new(pattern)
            .case_insensitive(self.ignore_case)
            .build()
            .map_err(|error| {
                script_error(format!("invalid regex for field `{}`: {error}", self.path))
            })?;

        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(actual) = value.as_str() else {
            return Err(type_mismatch_error(&self.path, FieldType::String));
        };
        if !regex.is_match(actual) {
            return Err(script_error(format!(
                "field `{}` must match regex `{pattern}`",
                self.path
            )));
        }
        Ok(self.clone())
    }

    fn date(&mut self, formatter: &str) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_temporal_string(formatter, "date", |value, format| {
            NaiveDate::parse_from_str(value, format)
                .map(|date| date.format(format).to_string() == value)
                .unwrap_or(false)
        })
    }

    fn time(&mut self, formatter: &str) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_temporal_string(formatter, "time", |value, format| {
            NaiveTime::parse_from_str(value, format)
                .map(|time| time.format(format).to_string() == value)
                .unwrap_or(false)
        })
    }

    fn datetime(&mut self, formatter: &str) -> Result<FieldRef, Box<EvalAltResult>> {
        self.validate_temporal_string(formatter, "datetime", |value, format| {
            NaiveDateTime::parse_from_str(value, format)
                .map(|time| time.format(format).to_string() == value)
                .unwrap_or(false)
        })
    }

    fn validate_temporal_string(
        &mut self,
        formatter: &str,
        kind: &str,
        is_valid: impl FnOnce(&str, &str) -> bool,
    ) -> Result<FieldRef, Box<EvalAltResult>> {
        if self.field_type != Some(FieldType::String) {
            return Err(script_error(format!(
                "{kind} for field `{}` must follow string()",
                self.path
            )));
        }
        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(actual) = value.as_str() else {
            return Err(type_mismatch_error(&self.path, FieldType::String));
        };
        if !is_valid(actual, formatter) {
            return Err(script_error(format!(
                "field `{}` must be a valid {kind} matching format `{formatter}`",
                self.path
            )));
        }
        Ok(self.clone())
    }

    fn one_of(&mut self, values: Array) -> Result<FieldRef, Box<EvalAltResult>> {
        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(actual) = value.as_str() else {
            return Err(type_mismatch_error(&self.path, FieldType::String));
        };
        let enum_values = dynamic_array_to_strings(values);
        if !enum_values
            .iter()
            .any(|expected| expected.eq_ignore_ascii_case(actual))
        {
            return Err(script_error(format!(
                "field `{}` must be one of [{}]",
                self.path,
                enum_values.join(", ")
            )));
        }
        Ok(self.clone())
    }

    fn gt_int(&mut self, threshold: rhai::INT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Gt, threshold as f64)
    }

    fn gt_float(&mut self, threshold: rhai::FLOAT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Gt, threshold)
    }

    fn gte_int(&mut self, threshold: rhai::INT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Gte, threshold as f64)
    }

    fn gte_float(&mut self, threshold: rhai::FLOAT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Gte, threshold)
    }

    fn lt_int(&mut self, threshold: rhai::INT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Lt, threshold as f64)
    }

    fn lt_float(&mut self, threshold: rhai::FLOAT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Lt, threshold)
    }

    fn lte_int(&mut self, threshold: rhai::INT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Lte, threshold as f64)
    }

    fn lte_float(&mut self, threshold: rhai::FLOAT) -> Result<FieldRef, Box<EvalAltResult>> {
        self.compare_number(NumberComparison::Lte, threshold)
    }

    fn eq(&mut self, expected: Dynamic) -> bool {
        let Some(actual) = lookup_path(&self.payload, &self.path) else {
            return false;
        };
        let Ok(expected) = dynamic_to_json(expected) else {
            return false;
        };
        values_equal(actual, &expected, self.ignore_case)
    }

    fn exists(&mut self) -> bool {
        is_present(lookup_path(&self.payload, &self.path))
    }

    fn missing(&mut self) -> bool {
        !self.exists()
    }

    fn value(&mut self) -> Dynamic {
        lookup_path(&self.payload, &self.path)
            .and_then(|value| to_dynamic(value).ok())
            .unwrap_or(Dynamic::UNIT)
    }

    fn compare_number(
        &mut self,
        comparison: NumberComparison,
        threshold: f64,
    ) -> Result<FieldRef, Box<EvalAltResult>> {
        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        let Some(number) = value.as_f64() else {
            return Err(script_error(format!(
                "field `{}` could not be represented as f64",
                self.path
            )));
        };
        if !comparison.matches(number, threshold) {
            return Err(script_error(format!(
                "field `{}` must be {} {threshold}",
                self.path,
                comparison.description()
            )));
        }
        Ok(self.clone())
    }

    fn validate_chained_type(
        &mut self,
        field_type: FieldType,
    ) -> Result<FieldRef, Box<EvalAltResult>> {
        self.field_type = Some(field_type);
        let Some(value) = self.value_for_chained_validation() else {
            return Ok(self.clone());
        };
        if !field_type.matches(value) {
            return Err(type_mismatch_error(&self.path, field_type));
        }
        Ok(self.clone())
    }

    fn value_for_chained_validation(&self) -> Option<&Value> {
        let value = lookup_path(&self.payload, &self.path);
        match self.presence {
            FieldPresence::Optional if !exists_non_null(value) => None,
            _ => value.filter(|value| !value.is_null()),
        }
    }
}

impl AnyFields {
    fn required(&mut self) -> Result<AnyFields, Box<EvalAltResult>> {
        if !self
            .paths
            .iter()
            .any(|path| is_present(lookup_path(&self.payload, path)))
        {
            return Err(script_error(format!(
                "at least one field is required: {}",
                self.paths.join(", ")
            )));
        }
        Ok(self.clone())
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

fn map_to_json(event: &Map) -> Result<Value, Box<EvalAltResult>> {
    from_dynamic::<Value>(&Dynamic::from(event.clone()))
        .map_err(|error| script_error(format!("failed to read processor event as json: {error}")))
}

fn type_mismatch_error(path: &str, field_type: FieldType) -> Box<EvalAltResult> {
    script_error(format!(
        "field `{path}` expected type `{}`",
        field_type.display()
    ))
}

fn script_error(message: String) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorRuntime(
        anyhow!(message).to_string().into(),
        rhai::Position::NONE,
    ))
}

fn lookup_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = payload;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn strings_equal(left: &str, right: &str, ignore_case: bool) -> bool {
    if ignore_case {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn values_equal(left: &Value, right: &Value, ignore_case: bool) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => strings_equal(left, right, ignore_case),
        _ => left == right,
    }
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
) -> Result<Vec<String>, Box<EvalAltResult>> {
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

fn dynamic_to_json(value: Dynamic) -> Result<Value, Box<EvalAltResult>> {
    from_dynamic(&value).map_err(|error| script_error(error.to_string()))
}
