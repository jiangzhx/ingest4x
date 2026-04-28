use super::loader::compile_event_rule;
use super::merge::merge_fragments;
use super::types::{RuleFragment, Rules};
use crate::current_timestamp_as_u64;
use crate::db::entities::{project_rule_sets, projects, rule_sets, rules};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Set, SqlErr,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utoipa::ToSchema;

#[derive(Clone)]
pub struct RuleRepository {
    db: DatabaseConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct RuleSet {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub wildcard_rule_id: Option<i32>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct Rule {
    pub id: i32,
    pub rule_set_id: i32,
    pub parent_id: Option<i32>,
    pub name: String,
    pub xwhat: Option<String>,
    pub content: String,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ProjectRuleSet {
    pub id: i32,
    pub project_id: i32,
    pub rule_set_id: i32,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateRuleSetInput {
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct UpdateRuleSetInput {
    pub name: Option<String>,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub wildcard_rule_id: Option<Option<i32>>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateRuleInput {
    pub rule_set_id: i32,
    pub parent_id: Option<i32>,
    pub name: String,
    pub xwhat: Option<String>,
    pub content: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct UpdateRuleInput {
    pub parent_id: Option<Option<i32>>,
    pub name: Option<String>,
    pub xwhat: Option<Option<String>>,
    pub content: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateProjectRuleSetInput {
    pub rule_set_id: i32,
    pub enabled: bool,
}

pub type RuleRepositoryResult<T> = std::result::Result<T, RuleRepositoryError>;

#[derive(Debug, PartialEq, Eq)]
pub enum RuleRepositoryError {
    ProjectNotFound { appid: String },
    RuleSetNotFound { id: i32 },
    RuleNotFound { id: i32 },
    ParentNotFound { id: i32 },
    ParentMustBeCommonRule { id: i32 },
    RuleWithChildrenCannotHaveXwhat { id: i32 },
    WildcardRuleMustNotHaveXwhat,
    DuplicateName,
    DuplicateXwhat,
    Cycle,
    InvalidRuleContent { message: String },
    DuplicateRuntimeRule { xwhat: String },
    Database(DbErr),
}

impl RuleRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create_rule_set(
        &self,
        input: CreateRuleSetInput,
    ) -> RuleRepositoryResult<RuleSet> {
        let now = current_timestamp();
        rule_sets::ActiveModel {
            name: Set(input.name),
            description: Set(input.description),
            enabled: Set(input.enabled),
            wildcard_rule_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&self.db)
        .await
        .map(Into::into)
        .map_err(map_rule_set_write_error)
    }

    pub async fn list_rule_sets(&self) -> RuleRepositoryResult<Vec<RuleSet>> {
        Ok(rule_sets::Entity::find()
            .order_by_asc(rule_sets::Column::Id)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get_rule_set(&self, id: i32) -> RuleRepositoryResult<Option<RuleSet>> {
        Ok(rule_sets::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .map(Into::into))
    }

    pub async fn update_rule_set(
        &self,
        id: i32,
        input: UpdateRuleSetInput,
    ) -> RuleRepositoryResult<RuleSet> {
        let existing = rule_sets::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or(RuleRepositoryError::RuleSetNotFound { id })?;
        let mut active = existing.into_active_model();
        if let Some(name) = input.name {
            active.name = Set(name);
        }
        if let Some(description) = input.description {
            active.description = Set(Some(description));
        }
        if let Some(enabled) = input.enabled {
            active.enabled = Set(enabled);
        }
        if let Some(wildcard_rule_id) = input.wildcard_rule_id {
            ensure_wildcard_rule_id(&self.db, id, wildcard_rule_id).await?;
            active.wildcard_rule_id = Set(wildcard_rule_id);
        }
        active.updated_at = Set(current_timestamp());

        active
            .update(&self.db)
            .await
            .map(Into::into)
            .map_err(map_rule_set_write_error)
    }

    pub async fn delete_rule_set(&self, id: i32) -> RuleRepositoryResult<()> {
        let result = rule_sets::Entity::delete_by_id(id).exec(&self.db).await?;
        if result.rows_affected == 0 {
            return Err(RuleRepositoryError::RuleSetNotFound { id });
        }
        Ok(())
    }

    pub async fn create_rule(&self, input: CreateRuleInput) -> RuleRepositoryResult<Rule> {
        ensure_rule_set_exists(&self.db, input.rule_set_id).await?;
        if let Some(parent_id) = input.parent_id {
            ensure_parent_in_rule_set(&self.db, input.rule_set_id, parent_id).await?;
        }
        parse_rule_content(&input.content)?;

        let now = current_timestamp();
        rules::ActiveModel {
            rule_set_id: Set(input.rule_set_id),
            parent_id: Set(input.parent_id),
            name: Set(input.name),
            xwhat: Set(input.xwhat),
            content: Set(input.content),
            enabled: Set(input.enabled),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&self.db)
        .await
        .map(Into::into)
        .map_err(map_rule_write_error)
    }

    pub async fn list_rules(&self, rule_set_id: i32) -> RuleRepositoryResult<Vec<Rule>> {
        ensure_rule_set_exists(&self.db, rule_set_id).await?;
        Ok(rules::Entity::find()
            .filter(rules::Column::RuleSetId.eq(rule_set_id))
            .order_by_asc(rules::Column::ParentId)
            .order_by_asc(rules::Column::Id)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn get_rule(&self, rule_set_id: i32, id: i32) -> RuleRepositoryResult<Option<Rule>> {
        Ok(rules::Entity::find()
            .filter(rules::Column::RuleSetId.eq(rule_set_id))
            .filter(rules::Column::Id.eq(id))
            .one(&self.db)
            .await?
            .map(Into::into))
    }

    pub async fn update_rule(
        &self,
        rule_set_id: i32,
        id: i32,
        input: UpdateRuleInput,
    ) -> RuleRepositoryResult<Rule> {
        let existing = rules::Entity::find()
            .filter(rules::Column::RuleSetId.eq(rule_set_id))
            .filter(rules::Column::Id.eq(id))
            .one(&self.db)
            .await?
            .ok_or(RuleRepositoryError::RuleNotFound { id })?;

        let next_parent_id = input.parent_id.unwrap_or(existing.parent_id);
        if let Some(parent_id) = next_parent_id {
            if parent_id == id {
                return Err(RuleRepositoryError::Cycle);
            }
            ensure_parent_in_rule_set(&self.db, rule_set_id, parent_id).await?;
            ensure_no_cycle(&self.db, id, parent_id).await?;
        }

        if let Some(content) = input.content.as_ref() {
            parse_rule_content(content)?;
        }

        let next_xwhat = input.xwhat.as_ref().unwrap_or(&existing.xwhat);
        if has_event_xwhat(next_xwhat) {
            ensure_rule_set_does_not_use_wildcard_rule(&self.db, rule_set_id, id).await?;
            ensure_rule_has_no_children(&self.db, id).await?;
        }

        let mut active = existing.into_active_model();
        if input.parent_id.is_some() {
            active.parent_id = Set(next_parent_id);
        }
        if let Some(name) = input.name {
            active.name = Set(name);
        }
        if let Some(xwhat) = input.xwhat {
            active.xwhat = Set(xwhat);
        }
        if let Some(content) = input.content {
            active.content = Set(content);
        }
        if let Some(enabled) = input.enabled {
            active.enabled = Set(enabled);
        }
        active.updated_at = Set(current_timestamp());

        active
            .update(&self.db)
            .await
            .map(Into::into)
            .map_err(map_rule_write_error)
    }

    pub async fn delete_rule(&self, rule_set_id: i32, id: i32) -> RuleRepositoryResult<()> {
        clear_rule_set_wildcard_if_matches(&self.db, rule_set_id, id).await?;
        let result = rules::Entity::delete_many()
            .filter(rules::Column::RuleSetId.eq(rule_set_id))
            .filter(rules::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(RuleRepositoryError::RuleNotFound { id });
        }
        Ok(())
    }

    pub async fn assign_rule_set_to_project(
        &self,
        appid: &str,
        input: CreateProjectRuleSetInput,
    ) -> RuleRepositoryResult<ProjectRuleSet> {
        let project = find_project_by_appid(&self.db, appid).await?;
        ensure_rule_set_exists(&self.db, input.rule_set_id).await?;
        let now = current_timestamp();

        let existing = project_rule_sets::Entity::find()
            .filter(project_rule_sets::Column::ProjectId.eq(project.id))
            .one(&self.db)
            .await?;

        if let Some(existing) = existing {
            let mut active = existing.into_active_model();
            active.rule_set_id = Set(input.rule_set_id);
            active.enabled = Set(input.enabled);
            active.updated_at = Set(now);
            return active
                .update(&self.db)
                .await
                .map(Into::into)
                .map_err(Into::into);
        }

        project_rule_sets::ActiveModel {
            project_id: Set(project.id),
            rule_set_id: Set(input.rule_set_id),
            enabled: Set(input.enabled),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&self.db)
        .await
        .map(Into::into)
        .map_err(Into::into)
    }

    pub async fn list_project_rule_sets(
        &self,
        appid: &str,
    ) -> RuleRepositoryResult<Vec<ProjectRuleSet>> {
        let project = find_project_by_appid(&self.db, appid).await?;
        Ok(project_rule_sets::Entity::find()
            .filter(project_rule_sets::Column::ProjectId.eq(project.id))
            .order_by_asc(project_rule_sets::Column::Id)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn delete_project_rule_set(
        &self,
        appid: &str,
        rule_set_id: i32,
    ) -> RuleRepositoryResult<()> {
        let project = find_project_by_appid(&self.db, appid).await?;
        let result = project_rule_sets::Entity::delete_many()
            .filter(project_rule_sets::Column::ProjectId.eq(project.id))
            .filter(project_rule_sets::Column::RuleSetId.eq(rule_set_id))
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(RuleRepositoryError::RuleSetNotFound { id: rule_set_id });
        }
        Ok(())
    }

    pub async fn compile_project_rules(&self, appid: &str) -> RuleRepositoryResult<Rules> {
        let project = find_project_by_appid(&self.db, appid).await?;
        let assignments = project_rule_sets::Entity::find()
            .filter(project_rule_sets::Column::ProjectId.eq(project.id))
            .filter(project_rule_sets::Column::Enabled.eq(true))
            .one(&self.db)
            .await?;

        let mut events = HashMap::new();
        if let Some(assignment) = assignments {
            let rule_set_id = assignment.rule_set_id;
            let Some(rule_set) = rule_sets::Entity::find_by_id(rule_set_id)
                .one(&self.db)
                .await?
            else {
                return Ok(Rules { events });
            };
            if rule_set.enabled {
                let rule_rows = rules::Entity::find()
                    .filter(rules::Column::RuleSetId.eq(rule_set_id))
                    .all(&self.db)
                    .await?;
                let by_id = rule_rows
                    .iter()
                    .cloned()
                    .map(|rule| (rule.id, rule))
                    .collect::<HashMap<_, _>>();

                for rule in rule_rows.iter().filter(|rule| rule.enabled) {
                    let Some(event_name) = event_name_for_runtime(rule, rule_set.wildcard_rule_id)
                    else {
                        continue;
                    };
                    if events.contains_key(&event_name) {
                        return Err(RuleRepositoryError::DuplicateRuntimeRule {
                            xwhat: event_name,
                        });
                    }
                    let fragment = merged_fragment_for_rule(rule, &by_id)?;
                    events.insert(
                        event_name,
                        compile_event_rule(&fragment).map_err(|error| {
                            RuleRepositoryError::InvalidRuleContent {
                                message: error.to_string(),
                            }
                        })?,
                    );
                }
            }
        }

        Ok(Rules { events })
    }

    pub async fn enabled_rule_exists_for_xwhat(&self, xwhat: &str) -> RuleRepositoryResult<bool> {
        let exists = rules::Entity::find()
            .filter(rules::Column::Xwhat.eq(xwhat))
            .filter(rules::Column::Enabled.eq(true))
            .one(&self.db)
            .await?
            .is_some();
        Ok(exists)
    }
}

fn event_name_for_runtime(rule: &rules::Model, wildcard_rule_id: Option<i32>) -> Option<String> {
    match rule.xwhat.as_deref() {
        Some(xwhat) if !xwhat.trim().is_empty() => Some(xwhat.to_string()),
        None if wildcard_rule_id == Some(rule.id) => Some("default".to_string()),
        _ => None,
    }
}

fn merged_fragment_for_rule(
    rule: &rules::Model,
    by_id: &HashMap<i32, rules::Model>,
) -> RuleRepositoryResult<RuleFragment> {
    let mut chain = Vec::new();
    let mut current = Some(rule);
    let mut visited = HashSet::new();

    while let Some(rule) = current {
        if !visited.insert(rule.id) {
            return Err(RuleRepositoryError::Cycle);
        }
        chain.push(rule);
        current = rule.parent_id.and_then(|parent_id| by_id.get(&parent_id));
    }

    let mut merged = RuleFragment::default();
    for rule in chain.into_iter().rev() {
        if rule.enabled {
            merged = merge_fragments(merged, parse_rule_content(&rule.content)?);
        }
    }
    Ok(merged)
}

fn parse_rule_content(content: &str) -> RuleRepositoryResult<RuleFragment> {
    serde_yaml::from_str::<RuleFragment>(content).map_err(|error| {
        RuleRepositoryError::InvalidRuleContent {
            message: error.to_string(),
        }
    })
}

async fn find_project_by_appid(
    db: &DatabaseConnection,
    appid: &str,
) -> RuleRepositoryResult<projects::Model> {
    projects::Entity::find()
        .filter(projects::Column::Appid.eq(appid))
        .one(db)
        .await?
        .ok_or_else(|| RuleRepositoryError::ProjectNotFound {
            appid: appid.to_string(),
        })
}

async fn ensure_rule_set_exists(db: &DatabaseConnection, id: i32) -> RuleRepositoryResult<()> {
    if rule_sets::Entity::find_by_id(id).one(db).await?.is_none() {
        return Err(RuleRepositoryError::RuleSetNotFound { id });
    }
    Ok(())
}

async fn ensure_parent_in_rule_set(
    db: &DatabaseConnection,
    rule_set_id: i32,
    parent_id: i32,
) -> RuleRepositoryResult<()> {
    let parent = rules::Entity::find_by_id(parent_id).one(db).await?;
    match parent {
        Some(parent) if parent.rule_set_id == rule_set_id && !has_event_xwhat(&parent.xwhat) => {
            Ok(())
        }
        Some(parent) if parent.rule_set_id == rule_set_id => {
            Err(RuleRepositoryError::ParentMustBeCommonRule { id: parent_id })
        }
        Some(_) | None => Err(RuleRepositoryError::ParentNotFound { id: parent_id }),
    }
}

async fn ensure_rule_has_no_children(db: &DatabaseConnection, id: i32) -> RuleRepositoryResult<()> {
    let has_children = rules::Entity::find()
        .filter(rules::Column::ParentId.eq(id))
        .one(db)
        .await?
        .is_some();
    if has_children {
        return Err(RuleRepositoryError::RuleWithChildrenCannotHaveXwhat { id });
    }
    Ok(())
}

fn has_event_xwhat(xwhat: &Option<String>) -> bool {
    xwhat
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

async fn ensure_wildcard_rule_id(
    db: &DatabaseConnection,
    rule_set_id: i32,
    wildcard_rule_id: Option<i32>,
) -> RuleRepositoryResult<()> {
    let Some(wildcard_rule_id) = wildcard_rule_id else {
        return Ok(());
    };

    let rule = rules::Entity::find()
        .filter(rules::Column::RuleSetId.eq(rule_set_id))
        .filter(rules::Column::Id.eq(wildcard_rule_id))
        .one(db)
        .await?
        .ok_or(RuleRepositoryError::RuleNotFound {
            id: wildcard_rule_id,
        })?;

    if has_event_xwhat(&rule.xwhat) {
        return Err(RuleRepositoryError::WildcardRuleMustNotHaveXwhat);
    }

    Ok(())
}

async fn ensure_rule_set_does_not_use_wildcard_rule(
    db: &DatabaseConnection,
    rule_set_id: i32,
    rule_id: i32,
) -> RuleRepositoryResult<()> {
    let rule_set = rule_sets::Entity::find_by_id(rule_set_id)
        .one(db)
        .await?
        .ok_or(RuleRepositoryError::RuleSetNotFound { id: rule_set_id })?;
    if rule_set.wildcard_rule_id == Some(rule_id) {
        return Err(RuleRepositoryError::WildcardRuleMustNotHaveXwhat);
    }
    Ok(())
}

async fn clear_rule_set_wildcard_if_matches(
    db: &DatabaseConnection,
    rule_set_id: i32,
    rule_id: i32,
) -> RuleRepositoryResult<()> {
    let Some(rule_set) = rule_sets::Entity::find_by_id(rule_set_id).one(db).await? else {
        return Ok(());
    };
    if rule_set.wildcard_rule_id != Some(rule_id) {
        return Ok(());
    }

    let mut active = rule_set.into_active_model();
    active.wildcard_rule_id = Set(None);
    active.updated_at = Set(current_timestamp());
    active.update(db).await?;
    Ok(())
}

async fn ensure_no_cycle(
    db: &DatabaseConnection,
    rule_id: i32,
    next_parent_id: i32,
) -> RuleRepositoryResult<()> {
    let mut current = Some(next_parent_id);
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if id == rule_id || !visited.insert(id) {
            return Err(RuleRepositoryError::Cycle);
        }
        current = rules::Entity::find_by_id(id)
            .one(db)
            .await?
            .and_then(|rule| rule.parent_id);
    }
    Ok(())
}

fn map_rule_set_write_error(error: DbErr) -> RuleRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => RuleRepositoryError::DuplicateName,
        _ => RuleRepositoryError::Database(error),
    }
}

fn map_rule_write_error(error: DbErr) -> RuleRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(message)) if message.contains("xwhat") => {
            RuleRepositoryError::DuplicateXwhat
        }
        Some(SqlErr::UniqueConstraintViolation(_)) => RuleRepositoryError::DuplicateName,
        _ => RuleRepositoryError::Database(error),
    }
}

fn current_timestamp() -> i64 {
    current_timestamp_as_u64() as i64
}

impl From<rule_sets::Model> for RuleSet {
    fn from(value: rule_sets::Model) -> Self {
        Self {
            id: value.id,
            name: value.name,
            description: value.description,
            enabled: value.enabled,
            wildcard_rule_id: value.wildcard_rule_id,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<rules::Model> for Rule {
    fn from(value: rules::Model) -> Self {
        Self {
            id: value.id,
            rule_set_id: value.rule_set_id,
            parent_id: value.parent_id,
            name: value.name,
            xwhat: value.xwhat,
            content: value.content,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<project_rule_sets::Model> for ProjectRuleSet {
    fn from(value: project_rule_sets::Model) -> Self {
        Self {
            id: value.id,
            project_id: value.project_id,
            rule_set_id: value.rule_set_id,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<DbErr> for RuleRepositoryError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}

impl Display for RuleRepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectNotFound { appid } => write!(f, "project '{appid}' not found"),
            Self::RuleSetNotFound { id } => write!(f, "rule set '{id}' not found"),
            Self::RuleNotFound { id } => write!(f, "rule '{id}' not found"),
            Self::ParentNotFound { id } => write!(f, "parent rule '{id}' not found"),
            Self::ParentMustBeCommonRule { id } => {
                write!(f, "parent rule '{id}' must have xwhat=null")
            }
            Self::RuleWithChildrenCannotHaveXwhat { id } => {
                write!(f, "rule '{id}' has child rules and must keep xwhat=null")
            }
            Self::WildcardRuleMustNotHaveXwhat => {
                write!(f, "wildcard rule must have xwhat=null")
            }
            Self::DuplicateName => write!(f, "rule name already exists under the same parent"),
            Self::DuplicateXwhat => write!(f, "rule xwhat already exists in this rule set"),
            Self::Cycle => write!(f, "rule parent would create a cycle"),
            Self::InvalidRuleContent { message } => write!(f, "invalid rule content: {message}"),
            Self::DuplicateRuntimeRule { xwhat } => {
                write!(f, "multiple enabled rules matched xwhat '{xwhat}'")
            }
            Self::Database(error) => write!(f, "{error}"),
        }
    }
}

impl Error for RuleRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}
