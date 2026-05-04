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
    projects: RwLock<HashMap<String, Project>>,
    version: AtomicU64,
    refresh_lock: AsyncMutex<()>,
}

impl ProjectRegistryState {
    pub async fn load(repository: ProjectRepository) -> ProjectRepositoryResult<Self> {
        let (projects, version) = load_snapshot(&repository).await?;

        Ok(Self {
            repository,
            projects: RwLock::new(projects),
            version: AtomicU64::new(version),
            refresh_lock: AsyncMutex::new(()),
        })
    }

    pub fn contains(&self, appid: &str) -> bool {
        self.projects
            .read()
            .expect("project registry read lock poisoned")
            .contains_key(appid)
    }

    pub async fn refresh_if_needed(&self) -> ProjectRepositoryResult<bool> {
        let _guard = self.refresh_lock.lock().await;
        let current_version = self.version.load(Ordering::Acquire);
        let latest_version = self.repository.projects_version().await?;

        if latest_version == current_version {
            return Ok(false);
        }

        let (projects, version) = load_snapshot(&self.repository).await?;

        Ok(self.apply_snapshot_if_newer(projects, version))
    }

    fn apply_snapshot_if_newer(&self, projects: HashMap<String, Project>, version: u64) -> bool {
        if version <= self.version.load(Ordering::Acquire) {
            return false;
        }

        let mut guard = self
            .projects
            .write()
            .expect("project registry write lock poisoned");

        if version <= self.version.load(Ordering::Acquire) {
            return false;
        }

        *guard = projects;
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
) -> ProjectRepositoryResult<(HashMap<String, Project>, u64)> {
    loop {
        let version_before = repository.projects_version().await?;
        let projects = repository.list_enabled_projects().await?;
        let version_after = repository.projects_version().await?;

        if version_before == version_after {
            return Ok((
                projects
                    .into_iter()
                    .map(|project| (project.appid.clone(), project))
                    .collect(),
                version_after,
            ));
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
                appid: "app-a".to_string(),
                name: "App A".to_string(),
                enabled: true,
            })
            .await
            .expect("seed project should be created");

        let registry = Arc::new(
            ProjectRegistryState::load(repository)
                .await
                .expect("registry should load"),
        );

        let older_snapshot = HashMap::from([(
            "app-b".to_string(),
            Project {
                id: 2,
                appid: "app-b".to_string(),
                name: "App B".to_string(),
                enabled: true,
                created_at: 0,
                updated_at: 0,
            },
        )]);
        let newer_snapshot = HashMap::from([(
            "app-c".to_string(),
            Project {
                id: 3,
                appid: "app-c".to_string(),
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
                registry.apply_snapshot_if_newer(older_snapshot, 2);
            })
        };
        let apply_newer = {
            let barrier = barrier.clone();
            let registry = registry.clone();
            std::thread::spawn(move || {
                barrier.wait();
                registry.apply_snapshot_if_newer(newer_snapshot, 3);
            })
        };

        barrier.wait();
        apply_older
            .join()
            .expect("older snapshot task should finish");
        apply_newer
            .join()
            .expect("newer snapshot task should finish");

        assert!(!registry.contains("app-b"));
        assert!(registry.contains("app-c"));
        assert_eq!(registry.version.load(Ordering::Acquire), 3);
    }
}
