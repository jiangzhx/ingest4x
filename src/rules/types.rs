use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::rhai_validation::RhaiValidationRules;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

#[derive(Clone, Debug, Default)]
pub struct CompiledFieldRule {
    pub(crate) required: bool,
    pub(crate) constraints: Option<FieldConstraints>,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldConstraints {
    String(StringConstraints),
    Number(NumericConstraints),
    Integer(NumericConstraints),
    Boolean,
    Object,
    Array,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StringConstraints {
    pub(crate) enum_values: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NumericConstraints {
    pub(crate) gt: Option<f64>,
    pub(crate) gte: Option<f64>,
    pub(crate) lt: Option<f64>,
    pub(crate) lte: Option<f64>,
}

impl CompiledFieldRule {
    pub fn required(&self) -> bool {
        self.required
    }

    pub fn field_type(&self) -> Option<FieldType> {
        match self.constraints.as_ref() {
            Some(FieldConstraints::String(_)) => Some(FieldType::String),
            Some(FieldConstraints::Number(_)) => Some(FieldType::Number),
            Some(FieldConstraints::Integer(_)) => Some(FieldType::Integer),
            Some(FieldConstraints::Boolean) => Some(FieldType::Boolean),
            Some(FieldConstraints::Object) => Some(FieldType::Object),
            Some(FieldConstraints::Array) => Some(FieldType::Array),
            None => None,
        }
    }

    pub fn enum_values(&self) -> Option<&Vec<String>> {
        match self.constraints.as_ref() {
            Some(FieldConstraints::String(constraints)) => constraints.enum_values.as_ref(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CompiledEventRule {
    pub(crate) fields: BTreeMap<String, CompiledFieldRule>,
    pub(crate) rules: CompiledConditionalRules,
}

impl CompiledEventRule {
    pub fn field(&self, path: &str) -> Option<&CompiledFieldRule> {
        self.fields.get(path)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Rules {
    pub(crate) events: HashMap<String, CompiledEventRule>,
    pub(crate) rhai: Option<Arc<RhaiValidationRules>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct RuleFragment {
    pub(crate) extends: Option<String>,
    #[serde(default)]
    pub(crate) fields: BTreeMap<String, FieldRule>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct FieldRule {
    #[serde(default)]
    pub(crate) required: bool,
    #[serde(rename = "type")]
    pub(crate) field_type: Option<FieldType>,
    #[serde(default, rename = "enum")]
    pub(crate) enum_values: Vec<String>,
    pub(crate) gt: Option<f64>,
    pub(crate) gte: Option<f64>,
    pub(crate) lt: Option<f64>,
    pub(crate) lte: Option<f64>,
    #[serde(default)]
    pub(crate) required_when: Vec<FieldConditionalRequirement>,
    #[serde(default)]
    pub(crate) required_any_when: Vec<FieldConditionalRequirement>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FieldConditionalRequirement {
    pub(crate) equals: serde_yaml::Value,
    pub(crate) fields: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CompiledConditionalRules {
    pub(crate) required_if: Vec<CompiledConditionalRequirement>,
    pub(crate) required_any_if: Vec<CompiledConditionalRequirement>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledConditionalRequirement {
    pub(crate) path: String,
    pub(crate) equals: Value,
    pub(crate) fields: Vec<String>,
}
