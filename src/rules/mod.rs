mod loader;
mod merge;
mod repository;
mod types;
mod validate;

use anyhow::Result;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub use repository::{
    CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput, ProjectRuleSet, Rule,
    RuleRepository, RuleRepositoryError, RuleSet, UpdateRuleInput, UpdateRuleSetInput,
};
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
    pub fn load_from_dir(path: impl AsRef<Path>) -> Result<Self> {
        loader::load_rules_from_dir(path)
    }

    pub fn event(&self, name: &str) -> Option<&CompiledEventRule> {
        self.events.get(name)
    }

    pub fn can_validate(&self, event_name: &str) -> bool {
        self.events.contains_key(event_name) || self.events.contains_key("default")
    }

    pub fn validate(&self, event_name: &str, payload: &Value) -> Result<()> {
        validate::validate_event(self, event_name, payload)
    }
}
