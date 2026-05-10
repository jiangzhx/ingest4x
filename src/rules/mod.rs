mod error;
pub(crate) mod loader;
pub(crate) mod merge;
mod rhai_validation;
pub(crate) mod types;
mod validate;

use anyhow::Result;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub use error::RulesValidationError;
pub use types::{CompiledEventRule, CompiledFieldRule, FieldType, Rules};

#[derive(Clone)]
pub struct RuleSets {
    pub ingest: Rules,
}

impl RuleSets {
    pub fn load_from_root(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref();
        Ok(Self {
            ingest: Rules::load_from_dir(root.join("ingest"))?,
        })
    }

    pub fn scope_dir(path: impl AsRef<Path>, scope: &str) -> PathBuf {
        path.as_ref().join(scope)
    }
}

impl Rules {
    pub fn from_rhai_script(script: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            events: Default::default(),
            rhai: Some(rhai_validation::RhaiValidationRules::compile(
                script.as_ref(),
            )?),
        })
    }

    pub fn load_from_dir(path: impl AsRef<Path>) -> Result<Self> {
        loader::load_rules_from_dir(path)
    }

    pub fn event(&self, name: &str) -> Option<&CompiledEventRule> {
        self.events.get(name)
    }

    pub fn can_validate(&self, event_name: &str) -> bool {
        if self.rhai.is_some() {
            return true;
        }
        self.events.contains_key(event_name) || self.events.contains_key("default")
    }

    pub fn validate(
        &self,
        event_name: &str,
        payload: &Value,
    ) -> std::result::Result<(), RulesValidationError> {
        if let Some(rhai) = self.rhai.as_ref() {
            return rhai.validate(payload);
        }
        validate::validate_event(self, event_name, payload)
    }
}
