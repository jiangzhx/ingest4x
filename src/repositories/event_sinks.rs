use crate::current_timestamp_as_u64;
use crate::entities::{app_meta, delivery_targets, event_sinks};
use crate::settings::{
    default_kafka_batch_num_messages, default_kafka_delivery_timeout_ms, default_kafka_linger_ms,
    default_kafka_queue_buffering_max_messages, default_kafka_queue_buffering_max_ms,
    AutoOffsetReset,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set, SqlErr, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::{Display, Formatter};

const EVENT_SINKS_VERSION_KEY: &str = "event_sinks_version";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTargetType {
    Kafka,
    Stdout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTarget {
    pub id: i32,
    pub target_id: String,
    pub name: String,
    pub target_type: DeliveryTargetType,
    pub config_json: Value,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSink {
    pub id: i32,
    pub sink_id: String,
    pub name: String,
    pub delivery_target_id: i32,
    pub destination_json: Value,
    pub auto_offset_reset: AutoOffsetReset,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEventSink {
    pub sink_id: String,
    pub name: String,
    pub destination_json: Value,
    pub auto_offset_reset: AutoOffsetReset,
    pub target: DeliveryTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDeliveryTargetInput {
    pub target_id: String,
    pub name: String,
    pub target_type: DeliveryTargetType,
    pub config_json: Value,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateDeliveryTargetInput {
    pub name: Option<String>,
    pub config_json: Option<Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateEventSinkInput {
    pub sink_id: String,
    pub name: String,
    pub delivery_target_id: i32,
    pub destination_json: Value,
    pub auto_offset_reset: AutoOffsetReset,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateEventSinkInput {
    pub name: Option<String>,
    pub delivery_target_id: Option<i32>,
    pub destination_json: Option<Value>,
    pub auto_offset_reset: Option<AutoOffsetReset>,
    pub enabled: Option<bool>,
}

pub type EventSinkRepositoryResult<T> = Result<T, EventSinkRepositoryError>;

#[derive(Debug, PartialEq, Eq)]
pub enum EventSinkRepositoryError {
    DeliveryTargetNotFound { id: i32 },
    EventSinkNotFound { id: i32 },
    DeliveryTargetInUse { id: i32 },
    DuplicateDeliveryTarget { target_id: String },
    DuplicateEventSink { sink_id: String },
    VersionMetadataMissing,
    CorruptedVersion { value: String },
    InvalidConfig { message: String },
    Database(DbErr),
}

impl Display for EventSinkRepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeliveryTargetNotFound { id } => write!(f, "delivery target '{id}' not found"),
            Self::EventSinkNotFound { id } => write!(f, "event sink '{id}' not found"),
            Self::DeliveryTargetInUse { id } => {
                write!(f, "delivery target '{id}' is still used by event sinks")
            }
            Self::DuplicateDeliveryTarget { target_id } => {
                write!(f, "delivery target '{target_id}' already exists")
            }
            Self::DuplicateEventSink { sink_id } => {
                write!(f, "event sink '{sink_id}' already exists")
            }
            Self::VersionMetadataMissing => write!(f, "event_sinks_version metadata is missing"),
            Self::CorruptedVersion { value } => {
                write!(f, "event_sinks_version contains invalid value '{value}'")
            }
            Self::InvalidConfig { message } => write!(f, "{message}"),
            Self::Database(error) => write!(f, "{error}"),
        }
    }
}

impl Error for EventSinkRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DbErr> for EventSinkRepositoryError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}

#[derive(Clone)]
pub struct EventSinkRepository {
    db: DatabaseConnection,
}

impl EventSinkRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create_delivery_target(
        &self,
        input: CreateDeliveryTargetInput,
    ) -> EventSinkRepositoryResult<DeliveryTarget> {
        let txn = self.db.begin().await?;
        let result = async {
            let now = current_timestamp();
            let target_id = input.target_id.clone();
            let config_json = normalize_target_config(input.target_type, input.config_json)?;

            let target = delivery_targets::ActiveModel {
                target_id: Set(input.target_id),
                name: Set(input.name),
                target_type: Set(input.target_type.as_str().to_string()),
                config_json: Set(config_json),
                enabled: Set(input.enabled),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(|error| map_delivery_target_write_error(error, &target_id))?;

            bump_event_sinks_version(&txn).await?;

            target.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_delivery_target(
        &self,
        id: i32,
        input: UpdateDeliveryTargetInput,
    ) -> EventSinkRepositoryResult<DeliveryTarget> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_delivery_target_by_id(&txn, id).await?;
            let target_type = DeliveryTargetType::parse(&existing.target_type)?;
            let mut active_model = existing.into_active_model();

            if let Some(name) = input.name {
                active_model.name = Set(name);
            }
            if let Some(config_json) = input.config_json {
                active_model.config_json = Set(normalize_target_config(target_type, config_json)?);
            }
            if let Some(enabled) = input.enabled {
                active_model.enabled = Set(enabled);
            }
            active_model.updated_at = Set(current_timestamp());

            let target = active_model.update(&txn).await?;
            bump_event_sinks_version(&txn).await?;

            target.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn delete_delivery_target(&self, id: i32) -> EventSinkRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            find_delivery_target_by_id(&txn, id).await?;
            if event_sinks::Entity::find()
                .filter(event_sinks::Column::DeliveryTargetId.eq(id))
                .one(&txn)
                .await?
                .is_some()
            {
                return Err(EventSinkRepositoryError::DeliveryTargetInUse { id });
            }

            let delete_result = delivery_targets::Entity::delete_by_id(id)
                .exec(&txn)
                .await?;
            debug_assert_eq!(delete_result.rows_affected, 1);

            bump_event_sinks_version(&txn).await?;

            Ok(())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn create_event_sink(
        &self,
        input: CreateEventSinkInput,
    ) -> EventSinkRepositoryResult<EventSink> {
        let txn = self.db.begin().await?;
        let result = async {
            let target = find_delivery_target_by_id(&txn, input.delivery_target_id).await?;
            let target_type = DeliveryTargetType::parse(&target.target_type)?;
            let destination_json = normalize_sink_destination(target_type, input.destination_json)?;
            let now = current_timestamp();
            let sink_id = input.sink_id.clone();

            let sink = event_sinks::ActiveModel {
                sink_id: Set(input.sink_id),
                name: Set(input.name),
                delivery_target_id: Set(input.delivery_target_id),
                destination_json: Set(destination_json),
                auto_offset_reset: Set(
                    auto_offset_reset_as_str(input.auto_offset_reset).to_string()
                ),
                enabled: Set(input.enabled),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(|error| map_event_sink_write_error(error, &sink_id))?;

            bump_event_sinks_version(&txn).await?;

            sink.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn update_event_sink(
        &self,
        id: i32,
        input: UpdateEventSinkInput,
    ) -> EventSinkRepositoryResult<EventSink> {
        let txn = self.db.begin().await?;
        let result = async {
            let existing = find_event_sink_by_id(&txn, id).await?;
            let delivery_target_id = input
                .delivery_target_id
                .unwrap_or(existing.delivery_target_id);
            let target = find_delivery_target_by_id(&txn, delivery_target_id).await?;
            let target_type = DeliveryTargetType::parse(&target.target_type)?;
            let destination_json = match input.destination_json {
                Some(destination_json) => destination_json,
                None => serde_json::from_str(&existing.destination_json).map_err(|error| {
                    EventSinkRepositoryError::InvalidConfig {
                        message: error.to_string(),
                    }
                })?,
            };
            let mut active_model = existing.into_active_model();

            if let Some(name) = input.name {
                active_model.name = Set(name);
            }
            active_model.delivery_target_id = Set(delivery_target_id);
            active_model.destination_json =
                Set(normalize_sink_destination(target_type, destination_json)?);
            if let Some(auto_offset_reset) = input.auto_offset_reset {
                active_model.auto_offset_reset =
                    Set(auto_offset_reset_as_str(auto_offset_reset).to_string());
            }
            if let Some(enabled) = input.enabled {
                active_model.enabled = Set(enabled);
            }
            active_model.updated_at = Set(current_timestamp());

            let sink = active_model.update(&txn).await?;
            bump_event_sinks_version(&txn).await?;

            sink.try_into()
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn delete_event_sink(&self, id: i32) -> EventSinkRepositoryResult<()> {
        let txn = self.db.begin().await?;
        let result = async {
            find_event_sink_by_id(&txn, id).await?;
            let delete_result = event_sinks::Entity::delete_by_id(id).exec(&txn).await?;
            debug_assert_eq!(delete_result.rows_affected, 1);

            bump_event_sinks_version(&txn).await?;

            Ok(())
        }
        .await;

        finish_transaction(txn, result).await
    }

    pub async fn event_sinks_version(&self) -> EventSinkRepositoryResult<u64> {
        read_event_sinks_version(&self.db).await
    }

    pub async fn list_delivery_targets(&self) -> EventSinkRepositoryResult<Vec<DeliveryTarget>> {
        let targets = delivery_targets::Entity::find()
            .order_by_asc(delivery_targets::Column::Id)
            .all(&self.db)
            .await?;

        targets.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_event_sinks(&self) -> EventSinkRepositoryResult<Vec<EventSink>> {
        let sinks = event_sinks::Entity::find()
            .order_by_asc(event_sinks::Column::Id)
            .all(&self.db)
            .await?;

        sinks.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_enabled_runtime_sinks(
        &self,
    ) -> EventSinkRepositoryResult<Vec<RuntimeEventSink>> {
        let sinks = event_sinks::Entity::find()
            .filter(event_sinks::Column::Enabled.eq(true))
            .order_by_asc(event_sinks::Column::Id)
            .all(&self.db)
            .await?;

        let mut runtime_sinks = Vec::with_capacity(sinks.len());
        for sink in sinks {
            let target = delivery_targets::Entity::find_by_id(sink.delivery_target_id)
                .filter(delivery_targets::Column::Enabled.eq(true))
                .one(&self.db)
                .await?;

            let Some(target) = target else {
                continue;
            };

            let sink = EventSink::try_from(sink)?;
            runtime_sinks.push(RuntimeEventSink {
                sink_id: sink.sink_id,
                name: sink.name,
                destination_json: sink.destination_json,
                auto_offset_reset: sink.auto_offset_reset,
                target: target.try_into()?,
            });
        }

        Ok(runtime_sinks)
    }
}

async fn find_event_sink_by_id<C>(db: &C, id: i32) -> EventSinkRepositoryResult<event_sinks::Model>
where
    C: ConnectionTrait,
{
    event_sinks::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(EventSinkRepositoryError::EventSinkNotFound { id })
}

async fn find_delivery_target_by_id<C>(
    db: &C,
    id: i32,
) -> EventSinkRepositoryResult<delivery_targets::Model>
where
    C: ConnectionTrait,
{
    delivery_targets::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(EventSinkRepositoryError::DeliveryTargetNotFound { id })
}

async fn read_event_sinks_version<C>(db: &C) -> EventSinkRepositoryResult<u64>
where
    C: ConnectionTrait,
{
    let meta = load_event_sinks_version_metadata(db).await?;

    meta.value
        .parse::<u64>()
        .map_err(|_| EventSinkRepositoryError::CorruptedVersion { value: meta.value })
}

async fn bump_event_sinks_version<C>(db: &C) -> EventSinkRepositoryResult<()>
where
    C: ConnectionTrait,
{
    let meta = load_event_sinks_version_metadata(db).await?;

    let next_version =
        meta.value
            .parse::<u64>()
            .map_err(|_| EventSinkRepositoryError::CorruptedVersion {
                value: meta.value.clone(),
            })?
            + 1;

    let mut active_model: app_meta::ActiveModel = meta.into();
    active_model.value = Set(next_version.to_string());
    active_model.update(db).await?;

    Ok(())
}

async fn load_event_sinks_version_metadata<C>(db: &C) -> EventSinkRepositoryResult<app_meta::Model>
where
    C: ConnectionTrait,
{
    app_meta::Entity::find_by_id(EVENT_SINKS_VERSION_KEY.to_string())
        .one(db)
        .await?
        .ok_or(EventSinkRepositoryError::VersionMetadataMissing)
}

fn normalize_target_config(
    target_type: DeliveryTargetType,
    config_json: Value,
) -> EventSinkRepositoryResult<String> {
    match target_type {
        DeliveryTargetType::Kafka => normalize_json::<KafkaDeliveryTargetConfig>(config_json),
        DeliveryTargetType::Stdout => normalize_json::<StdoutDeliveryTargetConfig>(config_json),
    }
}

fn normalize_sink_destination(
    target_type: DeliveryTargetType,
    destination_json: Value,
) -> EventSinkRepositoryResult<String> {
    match target_type {
        DeliveryTargetType::Kafka => {
            require_json_field(&destination_json, "topic")?;
            normalize_json::<KafkaSinkDestination>(destination_json)
        }
        DeliveryTargetType::Stdout => normalize_json::<StdoutSinkDestination>(destination_json),
    }
}

fn normalize_json<T>(value: Value) -> EventSinkRepositoryResult<String>
where
    T: for<'de> Deserialize<'de> + Serialize + ValidateConfig,
{
    let config = serde_json::from_value::<T>(value).map_err(|error| {
        EventSinkRepositoryError::InvalidConfig {
            message: error.to_string(),
        }
    })?;
    config.validate()?;
    serde_json::to_string(&config).map_err(|error| EventSinkRepositoryError::InvalidConfig {
        message: error.to_string(),
    })
}

fn current_timestamp() -> i64 {
    current_timestamp_as_u64() as i64
}

fn map_delivery_target_write_error(error: DbErr, target_id: &str) -> EventSinkRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => {
            EventSinkRepositoryError::DuplicateDeliveryTarget {
                target_id: target_id.to_string(),
            }
        }
        _ => EventSinkRepositoryError::Database(error),
    }
}

fn map_event_sink_write_error(error: DbErr, sink_id: &str) -> EventSinkRepositoryError {
    match error.sql_err() {
        Some(SqlErr::UniqueConstraintViolation(_)) => {
            EventSinkRepositoryError::DuplicateEventSink {
                sink_id: sink_id.to_string(),
            }
        }
        _ => EventSinkRepositoryError::Database(error),
    }
}

async fn finish_transaction<T>(
    txn: sea_orm::DatabaseTransaction,
    result: EventSinkRepositoryResult<T>,
) -> EventSinkRepositoryResult<T> {
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

fn auto_offset_reset_as_str(value: AutoOffsetReset) -> &'static str {
    match value {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
    }
}

fn parse_auto_offset_reset(value: &str) -> EventSinkRepositoryResult<AutoOffsetReset> {
    match value {
        "earliest" => Ok(AutoOffsetReset::Earliest),
        "latest" => Ok(AutoOffsetReset::Latest),
        _ => Err(EventSinkRepositoryError::InvalidConfig {
            message: format!("unknown auto_offset_reset `{value}`"),
        }),
    }
}

impl DeliveryTargetType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Kafka => "kafka",
            Self::Stdout => "stdout",
        }
    }

    fn parse(value: &str) -> EventSinkRepositoryResult<Self> {
        match value {
            "kafka" => Ok(Self::Kafka),
            "stdout" => Ok(Self::Stdout),
            _ => Err(EventSinkRepositoryError::InvalidConfig {
                message: format!("unknown delivery target type `{value}`"),
            }),
        }
    }
}

impl TryFrom<delivery_targets::Model> for DeliveryTarget {
    type Error = EventSinkRepositoryError;

    fn try_from(value: delivery_targets::Model) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            target_id: value.target_id,
            name: value.name,
            target_type: DeliveryTargetType::parse(&value.target_type)?,
            config_json: serde_json::from_str(&value.config_json).map_err(|error| {
                EventSinkRepositoryError::InvalidConfig {
                    message: error.to_string(),
                }
            })?,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

impl TryFrom<event_sinks::Model> for EventSink {
    type Error = EventSinkRepositoryError;

    fn try_from(value: event_sinks::Model) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            sink_id: value.sink_id,
            name: value.name,
            delivery_target_id: value.delivery_target_id,
            destination_json: serde_json::from_str(&value.destination_json).map_err(|error| {
                EventSinkRepositoryError::InvalidConfig {
                    message: error.to_string(),
                }
            })?,
            auto_offset_reset: parse_auto_offset_reset(&value.auto_offset_reset)?,
            enabled: value.enabled,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
    }
}

trait ValidateConfig {
    fn validate(&self) -> EventSinkRepositoryResult<()>;
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct KafkaDeliveryTargetConfig {
    bootstrap_servers: String,
    #[serde(default = "default_kafka_delivery_timeout_ms")]
    delivery_timeout_ms: String,
    #[serde(default = "default_kafka_queue_buffering_max_ms")]
    queue_buffering_max_ms: String,
    #[serde(default = "default_kafka_batch_num_messages")]
    batch_num_messages: String,
    #[serde(default = "default_kafka_queue_buffering_max_messages")]
    queue_buffering_max_messages: String,
    #[serde(default = "default_kafka_linger_ms")]
    linger_ms: String,
}

impl ValidateConfig for KafkaDeliveryTargetConfig {
    fn validate(&self) -> EventSinkRepositoryResult<()> {
        validate_required_string("bootstrap_servers", &self.bootstrap_servers)?;
        validate_required_string("delivery_timeout_ms", &self.delivery_timeout_ms)?;
        validate_required_string("queue_buffering_max_ms", &self.queue_buffering_max_ms)?;
        validate_required_string("batch_num_messages", &self.batch_num_messages)?;
        validate_required_string(
            "queue_buffering_max_messages",
            &self.queue_buffering_max_messages,
        )?;
        validate_required_string("linger_ms", &self.linger_ms)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct KafkaSinkDestination {
    topic: String,
}

impl ValidateConfig for KafkaSinkDestination {
    fn validate(&self) -> EventSinkRepositoryResult<()> {
        validate_required_string("topic", &self.topic)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StdoutDeliveryTargetConfig {}

impl ValidateConfig for StdoutDeliveryTargetConfig {
    fn validate(&self) -> EventSinkRepositoryResult<()> {
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StdoutSinkDestination {}

impl ValidateConfig for StdoutSinkDestination {
    fn validate(&self) -> EventSinkRepositoryResult<()> {
        Ok(())
    }
}

fn validate_required_string(field: &str, value: &str) -> EventSinkRepositoryResult<()> {
    if value.trim().is_empty() {
        return Err(EventSinkRepositoryError::InvalidConfig {
            message: format!("{field} must not be empty"),
        });
    }

    Ok(())
}

fn require_json_field(value: &Value, field: &str) -> EventSinkRepositoryResult<()> {
    if value.get(field).is_none() {
        return Err(EventSinkRepositoryError::InvalidConfig {
            message: format!("missing field `{field}`"),
        });
    }

    Ok(())
}
