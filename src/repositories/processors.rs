use crate::current_timestamp_as_u64;
use crate::entities::{
    app_meta, event_sinks, processor_script_modules, processor_scripts, project_processors,
    projects,
};
use crate::ingest::processor::ProcessorState;
use crate::rhai_ctx::sink_target_constant_name;
use rhai::{ASTNode, Expr, FnCallExpr, Stmt};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set, SqlErr, TransactionTrait,
};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

const PROCESSOR_SCRIPTS_VERSION_KEY: &str = "processor_scripts_version";
const DEFAULT_PROCESSOR_SCRIPT_KEY: &str = "default";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorScriptStatus {
    Draft,
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProcessorScriptModuleInput {
    pub module_name: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProcessorScriptInput {
    pub script_key: String,
    pub name: String,
    pub entry_module: String,
    pub status: ProcessorScriptStatus,
    pub modules: Vec<CreateProcessorScriptModuleInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProcessorScriptModuleInput {
    pub module_name: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProcessorScriptInput {
    pub name: String,
    pub entry_module: String,
    pub status: ProcessorScriptStatus,
    pub modules: Vec<UpdateProcessorScriptModuleInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateProcessorScriptModuleInput {
    pub module_name: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateProcessorScriptInput {
    pub entry_module: String,
    pub modules: Vec<ValidateProcessorScriptModuleInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateProcessorScriptStatusInput {
    pub status: ProcessorScriptStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorScript {
    pub id: i32,
    pub script_key: String,
    pub name: String,
    pub entry_module: String,
    pub version: i32,
    pub status: ProcessorScriptStatus,
    pub checksum: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub activated_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorScriptModule {
    pub id: i32,
    pub processor_script_id: i32,
    pub module_name: String,
    pub source: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectProcessor {
    pub id: i32,
    pub project_id: i32,
    pub processor_script_id: i32,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProcessorScript {
    pub id: i32,
    pub script_key: String,
    pub name: String,
    pub entry_module: String,
    pub version: i32,
    pub entry_source: String,
    pub modules: Vec<ProcessorScriptModule>,
}

impl RuntimeProcessorScript {
    pub fn resolver_modules(&self) -> Vec<(String, String)> {
        self.modules
            .iter()
            .filter(|module| module.module_name != self.entry_module)
            .map(|module| (module.module_name.clone(), module.source.clone()))
            .collect()
    }
}

pub type ProcessorRepositoryResult<T> = Result<T, ProcessorRepositoryError>;

#[derive(Debug)]
pub enum ProcessorRepositoryError {
    ProjectNotFound { id: i32 },
    ProcessorScriptNotFound { id: i32 },
    DuplicateProcessorScriptKey { script_key: String },
    ProcessorScriptNotActive { id: i32 },
    ProcessorScriptInUse { id: i32 },
    DefaultProcessorScriptMissing,
    EntryModuleMissing { module_name: String },
    InvalidModuleName { module_name: String },
    InvalidScript { message: String },
    VersionMetadataMissing,
    CorruptedVersion { value: String },
    Database(DbErr),
}

impl Display for ProcessorRepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectNotFound { id } => write!(f, "project '{id}' not found"),
            Self::ProcessorScriptNotFound { id } => write!(f, "processor script '{id}' not found"),
            Self::DuplicateProcessorScriptKey { script_key } => {
                write!(f, "processor script_key '{script_key}' already exists")
            }
            Self::ProcessorScriptNotActive { id } => {
                write!(f, "processor script '{id}' is not active")
            }
            Self::ProcessorScriptInUse { id } => {
                write!(f, "processor script '{id}' is still used by projects")
            }
            Self::DefaultProcessorScriptMissing => write!(f, "default processor script is missing"),
            Self::EntryModuleMissing { module_name } => {
                write!(f, "processor entry module '{module_name}' is missing")
            }
            Self::InvalidModuleName { module_name } => {
                write!(f, "invalid Rhai module name '{module_name}'")
            }
            Self::InvalidScript { message } => write!(f, "{message}"),
            Self::VersionMetadataMissing => {
                write!(f, "processor_scripts_version metadata is missing")
            }
            Self::CorruptedVersion { value } => {
                write!(
                    f,
                    "processor_scripts_version contains invalid value '{value}'"
                )
            }
            Self::Database(error) => write!(f, "{error}"),
        }
    }
}

impl Error for ProcessorRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DbErr> for ProcessorRepositoryError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}

#[derive(Clone)]
pub struct ProcessorRepository {
    db: DatabaseConnection,
}

impl ProcessorRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn validate_script(
        &self,
        input: ValidateProcessorScriptInput,
    ) -> ProcessorRepositoryResult<()> {
        let sink_targets = self.enabled_sink_ids().await?;
        validate_script_input(
            &CreateProcessorScriptInput {
                script_key: "validation".to_string(),
                name: "Validation".to_string(),
                entry_module: input.entry_module,
                status: ProcessorScriptStatus::Draft,
                modules: input
                    .modules
                    .into_iter()
                    .map(|module| CreateProcessorScriptModuleInput {
                        module_name: module.module_name,
                        source: module.source,
                    })
                    .collect(),
            },
            &sink_targets,
        )
    }

    pub async fn create_script(
        &self,
        input: CreateProcessorScriptInput,
    ) -> ProcessorRepositoryResult<ProcessorScript> {
        let sink_targets = self.enabled_sink_ids().await?;
        validate_script_input(&input, &sink_targets)?;
        let txn = self.db.begin().await?;
        let result = async {
            let now = current_timestamp();
            let script_key = input.script_key.clone();
            let checksum = script_checksum(&input);

            let script = processor_scripts::ActiveModel {
                script_key: Set(input.script_key),
                name: Set(input.name),
                entry_module: Set(input.entry_module),
                version: Set(1),
                status: Set(input.status.as_str().to_string()),
                checksum: Set(checksum),
                created_at: Set(now),
                updated_at: Set(now),
                activated_at: Set((input.status == ProcessorScriptStatus::Active).then_some(now)),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(|error| map_processor_script_write_error(error, &script_key))?;

            for module in input.modules {
                processor_script_modules::ActiveModel {
                    processor_script_id: Set(script.id),
                    module_name: Set(module.module_name),
                    source: Set(module.source),
                    created_at: Set(now),
                    updated_at: Set(now),
                    ..Default::default()
                }
                .insert(&txn)
                .await?;
            }

            bump_processor_scripts_version(&txn).await?;

            script.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn list_scripts(&self) -> ProcessorRepositoryResult<Vec<ProcessorScript>> {
        let scripts = processor_scripts::Entity::find()
            .order_by_asc(processor_scripts::Column::ScriptKey)
            .order_by_desc(processor_scripts::Column::Version)
            .all(&self.db)
            .await?;

        scripts.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn get_script(
        &self,
        id: i32,
    ) -> ProcessorRepositoryResult<Option<(ProcessorScript, Vec<ProcessorScriptModule>)>> {
        let Some(script) = processor_scripts::Entity::find_by_id(id)
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };
        let modules = processor_script_modules::Entity::find()
            .filter(processor_script_modules::Column::ProcessorScriptId.eq(id))
            .order_by_asc(processor_script_modules::Column::ModuleName)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        Ok(Some((script.try_into()?, modules)))
    }

    pub async fn update_script(
        &self,
        id: i32,
        input: UpdateProcessorScriptInput,
    ) -> ProcessorRepositoryResult<ProcessorScript> {
        let sink_targets = self.enabled_sink_ids().await?;
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_processor_script_by_id(&txn, id).await?;
            if input.status != ProcessorScriptStatus::Active {
                ensure_processor_script_can_be_disabled(&txn, &existing).await?;
            }

            let validation_input = CreateProcessorScriptInput {
                script_key: existing.script_key.clone(),
                name: input.name.clone(),
                entry_module: input.entry_module.clone(),
                status: input.status,
                modules: input
                    .modules
                    .iter()
                    .map(|module| CreateProcessorScriptModuleInput {
                        module_name: module.module_name.clone(),
                        source: module.source.clone(),
                    })
                    .collect(),
            };
            validate_script_input(&validation_input, &sink_targets)?;

            let now = current_timestamp();
            let checksum = script_checksum(&validation_input);
            let mut active_model = existing.into_active_model();
            active_model.name = Set(input.name);
            active_model.entry_module = Set(input.entry_module);
            active_model.status = Set(input.status.as_str().to_string());
            active_model.version = Set(active_model.version.unwrap() + 1);
            active_model.checksum = Set(checksum);
            active_model.updated_at = Set(now);
            if input.status == ProcessorScriptStatus::Active {
                active_model.activated_at = Set(Some(now));
            }

            let script = active_model.update(&txn).await?;
            processor_script_modules::Entity::delete_many()
                .filter(processor_script_modules::Column::ProcessorScriptId.eq(script.id))
                .exec(&txn)
                .await?;
            for module in input.modules {
                processor_script_modules::ActiveModel {
                    processor_script_id: Set(script.id),
                    module_name: Set(module.module_name),
                    source: Set(module.source),
                    created_at: Set(now),
                    updated_at: Set(now),
                    ..Default::default()
                }
                .insert(&txn)
                .await?;
            }

            bump_processor_scripts_version(&txn).await?;
            script.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_script_status(
        &self,
        id: i32,
        input: UpdateProcessorScriptStatusInput,
    ) -> ProcessorRepositoryResult<ProcessorScript> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_processor_script_by_id(&txn, id).await?;
            if input.status != ProcessorScriptStatus::Active {
                ensure_processor_script_can_be_disabled(&txn, &existing).await?;
            }

            let now = current_timestamp();
            let mut active_model = existing.into_active_model();
            active_model.status = Set(input.status.as_str().to_string());
            active_model.updated_at = Set(now);
            if input.status == ProcessorScriptStatus::Active {
                active_model.activated_at = Set(Some(now));
            }

            let script = active_model.update(&txn).await?;
            bump_processor_scripts_version(&txn).await?;
            script.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn assign_default_processor(&self, project_id: i32) -> ProcessorRepositoryResult<()> {
        let default_script = self.default_processor_script_model().await?;
        self.assign_project_processor(project_id, default_script.id, true)
            .await
    }

    pub async fn ensure_default_project_processors(&self) -> ProcessorRepositoryResult<usize> {
        let txn = self.db.begin().await?;
        let result = async {
            let default_script = find_default_processor_script(&txn).await?;
            let projects = projects::Entity::find()
                .order_by_asc(projects::Column::Id)
                .all(&txn)
                .await?;
            let now = current_timestamp();
            let mut inserted = 0_usize;

            for project in projects {
                let existing = project_processors::Entity::find()
                    .filter(project_processors::Column::ProjectId.eq(project.id))
                    .one(&txn)
                    .await?;
                if existing.is_some() {
                    continue;
                }

                project_processors::ActiveModel {
                    project_id: Set(project.id),
                    processor_script_id: Set(default_script.id),
                    enabled: Set(true),
                    created_at: Set(now),
                    updated_at: Set(now),
                    ..Default::default()
                }
                .insert(&txn)
                .await?;
                inserted += 1;
            }

            if inserted > 0 {
                bump_processor_scripts_version(&txn).await?;
            }

            Ok(inserted)
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn assign_project_processor(
        &self,
        project_id: i32,
        processor_script_id: i32,
        enabled: bool,
    ) -> ProcessorRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            let project = find_project_by_id(&txn, project_id).await?;
            let script = find_processor_script_by_id(&txn, processor_script_id).await?;
            if enabled
                && ProcessorScriptStatus::parse(&script.status)? != ProcessorScriptStatus::Active
            {
                return Err(ProcessorRepositoryError::ProcessorScriptNotActive {
                    id: processor_script_id,
                });
            }
            let now = current_timestamp();
            let existing = project_processors::Entity::find()
                .filter(project_processors::Column::ProjectId.eq(project.id))
                .one(&txn)
                .await?;

            match existing {
                Some(existing) => {
                    let mut active_model = existing.into_active_model();
                    active_model.processor_script_id = Set(processor_script_id);
                    active_model.enabled = Set(enabled);
                    active_model.updated_at = Set(now);
                    active_model.update(&txn).await?;
                }
                None => {
                    project_processors::ActiveModel {
                        project_id: Set(project.id),
                        processor_script_id: Set(processor_script_id),
                        enabled: Set(enabled),
                        created_at: Set(now),
                        updated_at: Set(now),
                        ..Default::default()
                    }
                    .insert(&txn)
                    .await?;
                }
            }

            bump_processor_scripts_version(&txn).await?;
            Ok(())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn delete_project_processor(&self, project_id: i32) -> ProcessorRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            let project = find_project_by_id(&txn, project_id).await?;
            project_processors::Entity::delete_many()
                .filter(project_processors::Column::ProjectId.eq(project.id))
                .exec(&txn)
                .await?;
            bump_processor_scripts_version(&txn).await?;
            Ok(())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn list_project_processors(
        &self,
    ) -> ProcessorRepositoryResult<Vec<ProjectProcessor>> {
        let bindings = project_processors::Entity::find()
            .order_by_asc(project_processors::Column::Id)
            .all(&self.db)
            .await?;

        let mut result = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let Some(project) = projects::Entity::find_by_id(binding.project_id)
                .one(&self.db)
                .await?
            else {
                continue;
            };
            result.push(ProjectProcessor {
                id: binding.id,
                project_id: project.id,
                processor_script_id: binding.processor_script_id,
                enabled: binding.enabled,
                created_at: binding.created_at,
                updated_at: binding.updated_at,
            });
        }

        Ok(result)
    }

    pub async fn default_runtime_script(
        &self,
    ) -> ProcessorRepositoryResult<RuntimeProcessorScript> {
        let script = self.default_processor_script_model().await?;
        self.runtime_script_from_model(script).await
    }

    pub async fn runtime_script_for_project(
        &self,
        project_id: i32,
    ) -> ProcessorRepositoryResult<RuntimeProcessorScript> {
        if projects::Entity::find_by_id(project_id)
            .one(&self.db)
            .await?
            .is_none()
        {
            return self.default_runtime_script().await;
        }

        let binding = project_processors::Entity::find()
            .filter(project_processors::Column::ProjectId.eq(project_id))
            .filter(project_processors::Column::Enabled.eq(true))
            .one(&self.db)
            .await?;

        let Some(binding) = binding else {
            return self.default_runtime_script().await;
        };

        let script = processor_scripts::Entity::find_by_id(binding.processor_script_id)
            .filter(processor_scripts::Column::Status.eq(ProcessorScriptStatus::Active.as_str()))
            .one(&self.db)
            .await?;

        match script {
            Some(script) => self.runtime_script_from_model(script).await,
            None => self.default_runtime_script().await,
        }
    }

    pub async fn list_enabled_runtime_project_processors(
        &self,
    ) -> ProcessorRepositoryResult<Vec<(i32, RuntimeProcessorScript)>> {
        let bindings = project_processors::Entity::find()
            .filter(project_processors::Column::Enabled.eq(true))
            .order_by_asc(project_processors::Column::Id)
            .all(&self.db)
            .await?;

        let mut processors = Vec::new();
        for binding in bindings {
            let Some(project) = projects::Entity::find_by_id(binding.project_id)
                .filter(projects::Column::Enabled.eq(true))
                .one(&self.db)
                .await?
            else {
                continue;
            };
            let Some(script) = processor_scripts::Entity::find_by_id(binding.processor_script_id)
                .filter(
                    processor_scripts::Column::Status.eq(ProcessorScriptStatus::Active.as_str()),
                )
                .one(&self.db)
                .await?
            else {
                continue;
            };

            processors.push((project.id, self.runtime_script_from_model(script).await?));
        }

        Ok(processors)
    }

    pub async fn processor_scripts_version(&self) -> ProcessorRepositoryResult<u64> {
        read_processor_scripts_version(&self.db).await
    }

    pub async fn enabled_sink_ids(&self) -> ProcessorRepositoryResult<Vec<String>> {
        let sinks = event_sinks::Entity::find()
            .filter(event_sinks::Column::Enabled.eq(true))
            .order_by_asc(event_sinks::Column::SinkId)
            .all(&self.db)
            .await?;
        Ok(sinks.into_iter().map(|sink| sink.sink_id).collect())
    }

    async fn default_processor_script_model(
        &self,
    ) -> ProcessorRepositoryResult<processor_scripts::Model> {
        find_default_processor_script(&self.db).await
    }

    async fn runtime_script_from_model(
        &self,
        script: processor_scripts::Model,
    ) -> ProcessorRepositoryResult<RuntimeProcessorScript> {
        let modules = processor_script_modules::Entity::find()
            .filter(processor_script_modules::Column::ProcessorScriptId.eq(script.id))
            .order_by_asc(processor_script_modules::Column::ModuleName)
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect::<Vec<ProcessorScriptModule>>();
        runtime_script_from_parts(script, modules)
    }
}

fn validate_script_input(
    input: &CreateProcessorScriptInput,
    sink_targets: &[String],
) -> ProcessorRepositoryResult<()> {
    validate_module_name(input.entry_module.as_str())?;
    for module in &input.modules {
        validate_module_name(module.module_name.as_str())?;
    }
    let Some(entry) = input
        .modules
        .iter()
        .find(|module| module.module_name == input.entry_module)
    else {
        return Err(ProcessorRepositoryError::EntryModuleMissing {
            module_name: input.entry_module.clone(),
        });
    };
    let modules = input
        .modules
        .iter()
        .filter(|module| module.module_name != input.entry_module)
        .map(|module| (module.module_name.clone(), module.source.clone()))
        .collect::<Vec<_>>();
    validate_emit_targets(input, sink_targets)?;
    ProcessorState::new_with_sink_targets(
        entry.source.clone(),
        modules,
        sink_targets.to_vec(),
        10_000,
    )
    .map_err(|error| ProcessorRepositoryError::InvalidScript {
        message: label_entry_compile_error(&input.entry_module, error.to_string()),
    })?;
    Ok(())
}

fn validate_emit_targets(
    input: &CreateProcessorScriptInput,
    sink_targets: &[String],
) -> ProcessorRepositoryResult<()> {
    let constants = sink_target_constants(sink_targets)?;
    let allowed_constants = constants.keys().cloned().collect::<HashSet<_>>();
    for module in &input.modules {
        let ast = compile_lint_ast(module.source.as_str()).map_err(|error| {
            ProcessorRepositoryError::InvalidScript {
                message: format!(
                    "failed to compile Rhai module `{}`: {error}",
                    module.module_name
                ),
            }
        })?;
        lint_emit_targets_in_ast(&module.module_name, &ast, &constants, &allowed_constants)?;
    }
    Ok(())
}

fn compile_lint_ast(source: &str) -> Result<rhai::AST, rhai::ParseError> {
    let mut engine = rhai::Engine::new();
    engine.set_max_expr_depths(0, 0);
    engine.compile(source)
}

fn sink_target_constants(
    sink_targets: &[String],
) -> ProcessorRepositoryResult<HashMap<String, String>> {
    let mut constants = HashMap::new();
    for sink_target in sink_targets {
        let constant_name = sink_target_constant_name(sink_target);
        if constant_name == "SINK" {
            return Err(ProcessorRepositoryError::InvalidScript {
                message: format!("invalid event sink id '{sink_target}' for Rhai constant"),
            });
        }
        if let Some(existing) = constants.insert(constant_name.clone(), sink_target.clone()) {
            return Err(ProcessorRepositoryError::InvalidScript {
                message: format!(
                    "event sink ids '{existing}' and '{sink_target}' both map to Rhai constant `{constant_name}`"
                ),
            });
        }
    }
    Ok(constants)
}

fn lint_emit_targets_in_ast(
    module_name: &str,
    ast: &rhai::AST,
    constants: &HashMap<String, String>,
    allowed_constants: &HashSet<String>,
) -> ProcessorRepositoryResult<()> {
    let mut error = None;
    ast.walk(&mut |path| {
        let Some(node) = path.last() else {
            return true;
        };
        match node {
            ASTNode::Stmt(Stmt::FnCall(call, position))
            | ASTNode::Expr(Expr::FnCall(call, position))
                if call.name == "emit" =>
            {
                error = lint_emit_call(module_name, call, *position, constants, allowed_constants);
                error.is_none()
            }
            _ => true,
        }
    });
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn lint_emit_call(
    module_name: &str,
    call: &FnCallExpr,
    position: rhai::Position,
    constants: &HashMap<String, String>,
    allowed_constants: &HashSet<String>,
) -> Option<ProcessorRepositoryError> {
    let Some(target) = call.args.first() else {
        return Some(invalid_emit_target(
            module_name,
            "emit(target, event) requires a sink target constant",
            position,
        ));
    };

    match target {
        Expr::Variable(variable, _, position) => {
            let constant_name = variable.1.as_str();
            if allowed_constants.contains(constant_name) {
                return None;
            }
            Some(invalid_emit_target(
                module_name,
                format!("unknown sink target constant `{constant_name}`").as_str(),
                *position,
            ))
        }
        Expr::StringConstant(sink_target, position) => {
            let constant_name = sink_target_constant_name(sink_target);
            let message = if constants.contains_key(&constant_name) {
                format!(
                    "emit target must use sink constant `{constant_name}` instead of string literal"
                )
            } else {
                format!("unknown sink target `{sink_target}`")
            };
            Some(invalid_emit_target(
                module_name,
                message.as_str(),
                *position,
            ))
        }
        other => Some(invalid_emit_target(
            module_name,
            "emit target must be a configured sink constant",
            other.position(),
        )),
    }
}

fn invalid_emit_target(
    module_name: &str,
    message: &str,
    position: rhai::Position,
) -> ProcessorRepositoryError {
    ProcessorRepositoryError::InvalidScript {
        message: format!("failed to lint Rhai module `{module_name}`: {message} {position}"),
    }
}

fn label_entry_compile_error(entry_module: &str, message: String) -> String {
    if message.contains("Rhai module `") {
        return message;
    }

    format!("failed to compile Rhai module `{entry_module}`: {message}")
}

fn validate_module_name(module_name: &str) -> ProcessorRepositoryResult<()> {
    let mut chars = module_name.chars();
    let Some(first) = chars.next() else {
        return Err(ProcessorRepositoryError::InvalidModuleName {
            module_name: module_name.to_string(),
        });
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(ProcessorRepositoryError::InvalidModuleName {
            module_name: module_name.to_string(),
        });
    }
    Ok(())
}

fn runtime_script_from_parts(
    script: processor_scripts::Model,
    modules: Vec<ProcessorScriptModule>,
) -> ProcessorRepositoryResult<RuntimeProcessorScript> {
    let entry_source = modules
        .iter()
        .find(|module| module.module_name == script.entry_module)
        .map(|module| module.source.clone())
        .ok_or_else(|| ProcessorRepositoryError::EntryModuleMissing {
            module_name: script.entry_module.clone(),
        })?;

    Ok(RuntimeProcessorScript {
        id: script.id,
        script_key: script.script_key,
        name: script.name,
        entry_module: script.entry_module,
        version: script.version,
        entry_source,
        modules,
    })
}

async fn find_project_by_id<C>(db: &C, id: i32) -> ProcessorRepositoryResult<projects::Model>
where
    C: ConnectionTrait,
{
    projects::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(ProcessorRepositoryError::ProjectNotFound { id })
}

async fn find_processor_script_by_id<C>(
    db: &C,
    id: i32,
) -> ProcessorRepositoryResult<processor_scripts::Model>
where
    C: ConnectionTrait,
{
    processor_scripts::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(ProcessorRepositoryError::ProcessorScriptNotFound { id })
}

async fn find_default_processor_script<C>(
    db: &C,
) -> ProcessorRepositoryResult<processor_scripts::Model>
where
    C: ConnectionTrait,
{
    processor_scripts::Entity::find()
        .filter(processor_scripts::Column::ScriptKey.eq(DEFAULT_PROCESSOR_SCRIPT_KEY))
        .filter(processor_scripts::Column::Status.eq(ProcessorScriptStatus::Active.as_str()))
        .one(db)
        .await?
        .ok_or(ProcessorRepositoryError::DefaultProcessorScriptMissing)
}

async fn ensure_processor_script_can_be_disabled<C>(
    db: &C,
    script: &processor_scripts::Model,
) -> ProcessorRepositoryResult<()>
where
    C: ConnectionTrait,
{
    if script.script_key == DEFAULT_PROCESSOR_SCRIPT_KEY {
        return Err(ProcessorRepositoryError::ProcessorScriptInUse { id: script.id });
    }

    let in_use = project_processors::Entity::find()
        .filter(project_processors::Column::ProcessorScriptId.eq(script.id))
        .filter(project_processors::Column::Enabled.eq(true))
        .one(db)
        .await?
        .is_some();
    if in_use {
        return Err(ProcessorRepositoryError::ProcessorScriptInUse { id: script.id });
    }

    Ok(())
}

async fn read_processor_scripts_version<C>(db: &C) -> ProcessorRepositoryResult<u64>
where
    C: ConnectionTrait,
{
    let meta = load_processor_scripts_version_metadata(db).await?;

    meta.value
        .parse::<u64>()
        .map_err(|_| ProcessorRepositoryError::CorruptedVersion { value: meta.value })
}

async fn bump_processor_scripts_version<C>(db: &C) -> ProcessorRepositoryResult<()>
where
    C: ConnectionTrait,
{
    let meta = load_processor_scripts_version_metadata(db).await?;
    let next_version =
        meta.value
            .parse::<u64>()
            .map_err(|_| ProcessorRepositoryError::CorruptedVersion {
                value: meta.value.clone(),
            })?
            + 1;
    let mut active_model: app_meta::ActiveModel = meta.into();
    active_model.value = Set(next_version.to_string());
    active_model.update(db).await?;
    Ok(())
}

async fn load_processor_scripts_version_metadata<C>(
    db: &C,
) -> ProcessorRepositoryResult<app_meta::Model>
where
    C: ConnectionTrait,
{
    app_meta::Entity::find_by_id(PROCESSOR_SCRIPTS_VERSION_KEY.to_string())
        .one(db)
        .await?
        .ok_or(ProcessorRepositoryError::VersionMetadataMissing)
}

async fn finish_transaction<T>(
    txn: sea_orm::DatabaseTransaction,
    result: ProcessorRepositoryResult<T>,
) -> ProcessorRepositoryResult<T> {
    match result {
        Ok(value) => {
            txn.commit().await?;
            Ok(value)
        }
        Err(error) => {
            txn.rollback().await?;
            Err(error)
        }
    }
}

fn script_checksum(input: &CreateProcessorScriptInput) -> String {
    let mut modules = input.modules.clone();
    modules.sort_by(|left, right| left.module_name.cmp(&right.module_name));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(input.script_key.as_bytes());
    bytes.extend_from_slice(input.entry_module.as_bytes());
    for module in modules {
        bytes.extend_from_slice(module.module_name.as_bytes());
        bytes.extend_from_slice(module.source.as_bytes());
    }
    format!("{:08x}", crc32fast::hash(&bytes))
}

fn current_timestamp() -> i64 {
    current_timestamp_as_u64() as i64
}

fn map_processor_script_write_error(error: DbErr, script_key: &str) -> ProcessorRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => {
            ProcessorRepositoryError::DuplicateProcessorScriptKey {
                script_key: script_key.to_string(),
            }
        }
        _ => ProcessorRepositoryError::Database(error),
    }
}

impl ProcessorScriptStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    fn parse(value: &str) -> ProcessorRepositoryResult<Self> {
        match value {
            "draft" => Ok(Self::Draft),
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            _ => Err(ProcessorRepositoryError::InvalidScript {
                message: format!("unknown processor script status `{value}`"),
            }),
        }
    }
}

impl TryFrom<processor_scripts::Model> for ProcessorScript {
    type Error = ProcessorRepositoryError;

    fn try_from(value: processor_scripts::Model) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            script_key: value.script_key,
            name: value.name,
            entry_module: value.entry_module,
            version: value.version,
            status: ProcessorScriptStatus::parse(&value.status)?,
            checksum: value.checksum,
            created_at: value.created_at,
            updated_at: value.updated_at,
            activated_at: value.activated_at,
        })
    }
}

impl From<processor_script_modules::Model> for ProcessorScriptModule {
    fn from(value: processor_script_modules::Model) -> Self {
        Self {
            id: value.id,
            processor_script_id: value.processor_script_id,
            module_name: value.module_name,
            source: value.source,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}
