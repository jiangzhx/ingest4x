use sea_orm::DbErr;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub id: i32,
    pub appid: String,
    pub name: String,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectInput {
    pub appid: String,
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateProjectInput {
    pub name: Option<String>,
    pub enabled: Option<bool>,
}

pub type ProjectRepositoryResult<T> = Result<T, ProjectRepositoryError>;

#[derive(Debug, PartialEq, Eq)]
pub enum ProjectRepositoryError {
    NotFound { appid: String },
    DuplicateAppid { appid: String },
    VersionMetadataMissing,
    CorruptedVersion { value: String },
    Database(DbErr),
}

impl Display for ProjectRepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { appid } => write!(f, "project '{appid}' not found"),
            Self::DuplicateAppid { appid } => {
                write!(f, "project appid '{appid}' already exists")
            }
            Self::VersionMetadataMissing => write!(f, "projects_version metadata is missing"),
            Self::CorruptedVersion { value } => {
                write!(f, "projects_version contains invalid value '{value}'")
            }
            Self::Database(error) => write!(f, "{error}"),
        }
    }
}

impl Error for ProjectRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DbErr> for ProjectRepositoryError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}
