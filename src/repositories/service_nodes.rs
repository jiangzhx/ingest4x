use crate::current_timestamp_as_u64;
use crate::entities::service_nodes;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Set,
};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceNodeStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Stale,
}

impl ServiceNodeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Stale => "stale",
        }
    }
}

impl From<String> for ServiceNodeStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "starting" => Self::Starting,
            "running" => Self::Running,
            "stopping" => Self::Stopping,
            "stopped" => Self::Stopped,
            "stale" => Self::Stale,
            _ => Self::Stale,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceNode {
    pub node_id: String,
    pub hostname: Option<String>,
    pub machine_ip: Option<String>,
    pub ingest_bind_address: String,
    pub management_bind_address: String,
    pub version: String,
    pub status: ServiceNodeStatus,
    pub started_at: i64,
    pub last_seen_at: i64,
    pub updated_at: i64,
    pub metadata_json: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegisterServiceNodeInput {
    pub node_id: String,
    pub hostname: Option<String>,
    pub machine_ip: Option<String>,
    pub ingest_bind_address: String,
    pub management_bind_address: String,
    pub version: String,
    pub status: ServiceNodeStatus,
    pub metadata_json: Option<Value>,
}

pub type ServiceNodeRepositoryResult<T> = Result<T, DbErr>;

#[derive(Clone)]
pub struct ServiceNodeRepository {
    db: DatabaseConnection,
}

impl ServiceNodeRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn connection(&self) -> &DatabaseConnection {
        &self.db
    }

    pub async fn register_service_node(
        &self,
        input: RegisterServiceNodeInput,
    ) -> ServiceNodeRepositoryResult<ServiceNode> {
        let now = current_timestamp();
        let node_id = input.node_id.trim().to_string();
        let metadata_json = input
            .metadata_json
            .map(|value| serde_json::to_string(&value).expect("metadata json should serialize"));

        let model = match service_nodes::Entity::find_by_id(node_id.clone())
            .one(&self.db)
            .await?
        {
            Some(existing) => {
                let mut active_model = existing.into_active_model();
                active_model.hostname = Set(input.hostname);
                active_model.machine_ip = Set(input.machine_ip);
                active_model.ingest_bind_address = Set(input.ingest_bind_address);
                active_model.management_bind_address = Set(input.management_bind_address);
                active_model.version = Set(input.version);
                active_model.status = Set(input.status.as_str().to_string());
                active_model.started_at = Set(now);
                active_model.last_seen_at = Set(now);
                active_model.updated_at = Set(now);
                active_model.metadata_json = Set(metadata_json);
                active_model.update(&self.db).await?
            }
            None => {
                service_nodes::ActiveModel {
                    node_id: Set(node_id),
                    hostname: Set(input.hostname),
                    machine_ip: Set(input.machine_ip),
                    ingest_bind_address: Set(input.ingest_bind_address),
                    management_bind_address: Set(input.management_bind_address),
                    version: Set(input.version),
                    status: Set(input.status.as_str().to_string()),
                    started_at: Set(now),
                    last_seen_at: Set(now),
                    updated_at: Set(now),
                    metadata_json: Set(metadata_json),
                }
                .insert(&self.db)
                .await?
            }
        };

        Ok(model.into())
    }

    pub async fn mark_service_node_seen(
        &self,
        node_id: &str,
    ) -> ServiceNodeRepositoryResult<Option<ServiceNode>> {
        let Some(existing) = service_nodes::Entity::find()
            .filter(service_nodes::Column::NodeId.eq(node_id))
            .one(&self.db)
            .await?
        else {
            return Ok(None);
        };

        let now = current_timestamp();
        let mut active_model = existing.into_active_model();
        active_model.status = Set(ServiceNodeStatus::Running.as_str().to_string());
        active_model.last_seen_at = Set(now);
        active_model.updated_at = Set(now);

        Ok(Some(active_model.update(&self.db).await?.into()))
    }

    pub async fn list_service_nodes(&self) -> ServiceNodeRepositoryResult<Vec<ServiceNode>> {
        let nodes = service_nodes::Entity::find()
            .order_by_asc(service_nodes::Column::NodeId)
            .all(&self.db)
            .await?;

        Ok(nodes.into_iter().map(Into::into).collect())
    }
}

fn current_timestamp() -> i64 {
    current_timestamp_as_u64() as i64
}

impl From<service_nodes::Model> for ServiceNode {
    fn from(value: service_nodes::Model) -> Self {
        Self {
            node_id: value.node_id,
            hostname: value.hostname,
            machine_ip: value.machine_ip,
            ingest_bind_address: value.ingest_bind_address,
            management_bind_address: value.management_bind_address,
            version: value.version,
            status: value.status.into(),
            started_at: value.started_at,
            last_seen_at: value.last_seen_at,
            updated_at: value.updated_at,
            metadata_json: value
                .metadata_json
                .map(|raw| serde_json::from_str(&raw).unwrap_or(Value::String(raw))),
        }
    }
}
