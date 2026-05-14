use crate::repositories::{Project, ProjectRepository, ProjectRepositoryResult};
use actix_web::rt::{spawn, task::JoinHandle, time::sleep};
use futures::lock::Mutex as AsyncMutex;
use log::error;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

pub struct ProjectRegistryState {
    repository: ProjectRepository,
    projects_by_token: RwLock<HashMap<String, Project>>,
    projects_by_key: RwLock<HashMap<String, Project>>,
    version: AtomicU64,
    refresh_lock: AsyncMutex<()>,
}

impl ProjectRegistryState {
    pub async fn load(repository: ProjectRepository) -> ProjectRepositoryResult<Self> {
        let (projects_by_token, projects_by_key, version) = load_snapshot(&repository).await?;

        Ok(Self {
            repository,
            projects_by_token: RwLock::new(projects_by_token),
            projects_by_key: RwLock::new(projects_by_key),
            version: AtomicU64::new(version),
            refresh_lock: AsyncMutex::new(()),
        })
    }

    pub fn project_by_key(&self, project_key: &str) -> Option<Project> {
        self.projects_by_key
            .read()
            .expect("project registry read lock poisoned")
            .get(project_key.trim())
            .cloned()
    }

    pub fn contains_project_id(&self, project_id: i32) -> bool {
        self.projects_by_token
            .read()
            .expect("project registry read lock poisoned")
            .values()
            .any(|project| project.id == project_id)
    }

    pub async fn refresh_if_needed(&self) -> ProjectRepositoryResult<bool> {
        let _guard = self.refresh_lock.lock().await;
        let current_version = self.version.load(Ordering::Acquire);
        let latest_version = self.repository.projects_version().await?;

        if latest_version == current_version {
            return Ok(false);
        }

        let (projects_by_token, projects_by_key, version) = load_snapshot(&self.repository).await?;

        Ok(self.apply_snapshot_if_newer(projects_by_token, projects_by_key, version))
    }

    fn apply_snapshot_if_newer(
        &self,
        projects_by_token: HashMap<String, Project>,
        projects_by_key: HashMap<String, Project>,
        version: u64,
    ) -> bool {
        if version <= self.version.load(Ordering::Acquire) {
            return false;
        }

        let mut token_guard = self
            .projects_by_token
            .write()
            .expect("project registry write lock poisoned");
        let mut key_guard = self
            .projects_by_key
            .write()
            .expect("project registry write lock poisoned");

        if version <= self.version.load(Ordering::Acquire) {
            return false;
        }

        *token_guard = projects_by_token;
        *key_guard = projects_by_key;
        self.version.store(version, Ordering::Release);
        true
    }
}

pub fn spawn_project_registry_refresh_loop(
    registry: actix_web::web::Data<ProjectRegistryState>,
    interval: Duration,
) -> JoinHandle<()> {
    spawn(async move {
        loop {
            sleep(interval).await;

            if let Err(error) = registry.refresh_if_needed().await {
                error!("refresh project registry snapshot failed: {error}");
            }
        }
    })
}

async fn load_snapshot(
    repository: &ProjectRepository,
) -> ProjectRepositoryResult<(HashMap<String, Project>, HashMap<String, Project>, u64)> {
    loop {
        let version_before = repository.projects_version().await?;
        let projects = repository.list_enabled_projects().await?;
        let version_after = repository.projects_version().await?;

        if version_before == version_after {
            let projects_by_token = projects
                .iter()
                .cloned()
                .map(|project| (project.ingest_token.clone(), project))
                .collect();
            let projects_by_key = projects
                .into_iter()
                .map(|project| (project.project_key.clone(), project))
                .collect();
            return Ok((projects_by_token, projects_by_key, version_after));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_sqlite_database;
    use crate::repositories::CreateProjectInput;
    use std::sync::{Arc, Barrier};

    #[tokio::test]
    async fn concurrent_refresh_completion_does_not_roll_back_to_older_snapshot() {
        let db = init_sqlite_database("sqlite::memory:")
            .await
            .expect("sqlite database should initialize");
        let repository = ProjectRepository::new(db);

        repository
            .create_project(CreateProjectInput {
                name: "App A".to_string(),
                enabled: true,
                ingest_token: "igx_app_a".to_string(),
            })
            .await
            .expect("seed project should be created");

        let registry = Arc::new(
            ProjectRegistryState::load(repository)
                .await
                .expect("registry should load"),
        );

        let older_snapshot = HashMap::from([(
            "igx_app_b".to_string(),
            Project {
                id: 2,
                ingest_token: "igx_app_b".to_string(),
                project_key: "app-b".to_string(),
                auth_mode: crate::repositories::ProjectAuthMode::Token,
                allowed_ips: Vec::new(),
                name: "App B".to_string(),
                enabled: true,
                created_at: 0,
                updated_at: 0,
            },
        )]);
        let newer_snapshot = HashMap::from([(
            "igx_app_c".to_string(),
            Project {
                id: 3,
                ingest_token: "igx_app_c".to_string(),
                project_key: "app-c".to_string(),
                auth_mode: crate::repositories::ProjectAuthMode::Token,
                allowed_ips: Vec::new(),
                name: "App C".to_string(),
                enabled: true,
                created_at: 0,
                updated_at: 0,
            },
        )]);

        let barrier = Arc::new(Barrier::new(3));
        let apply_older = {
            let barrier = barrier.clone();
            let registry = registry.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let older_key_snapshot = older_snapshot
                    .values()
                    .cloned()
                    .map(|project| (project.project_key.clone(), project))
                    .collect();
                registry.apply_snapshot_if_newer(older_snapshot, older_key_snapshot, 2);
            })
        };
        let apply_newer = {
            let barrier = barrier.clone();
            let registry = registry.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let newer_key_snapshot = newer_snapshot
                    .values()
                    .cloned()
                    .map(|project| (project.project_key.clone(), project))
                    .collect();
                registry.apply_snapshot_if_newer(newer_snapshot, newer_key_snapshot, 3);
            })
        };

        barrier.wait();
        apply_older
            .join()
            .expect("older snapshot task should finish");
        apply_newer
            .join()
            .expect("newer snapshot task should finish");

        assert!(registry.project_by_key("App-B").is_none());
        assert!(registry.project_by_key("app-c").is_some());
        assert_eq!(registry.version.load(Ordering::Acquire), 3);
    }
}
