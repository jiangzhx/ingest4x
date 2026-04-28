use ingest4x::db::init_sqlite_database;
use ingest4x::projects::{
    CreateProjectInput, ProjectRepository, ProjectRepositoryError, UpdateProjectInput,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

#[tokio::test]
async fn create_and_list_enabled_projects() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    repo.create_project(CreateProjectInput {
        appid: "disabled-app".to_string(),
        name: "Disabled".to_string(),
        enabled: false,
    })
    .await
    .expect("disabled project should be created");

    repo.create_project(CreateProjectInput {
        appid: "enabled-app".to_string(),
        name: "Enabled".to_string(),
        enabled: true,
    })
    .await
    .expect("enabled project should be created");

    let enabled_projects = repo
        .list_enabled_projects()
        .await
        .expect("enabled projects should list");

    assert_eq!(enabled_projects.len(), 1);
    assert_eq!(enabled_projects[0].appid, "enabled-app");
    assert!(enabled_projects[0].enabled);

    let all_projects = repo.list_projects().await.expect("projects should list");
    assert_eq!(all_projects.len(), 2);

    let loaded = repo
        .get_project("enabled-app")
        .await
        .expect("project lookup should succeed")
        .expect("enabled project should exist");
    assert_eq!(loaded.name, "Enabled");
}

#[tokio::test]
async fn mutating_projects_bumps_projects_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    assert_eq!(
        repo.projects_version()
            .await
            .expect("initial projects version should load"),
        0
    );

    repo.create_project(CreateProjectInput {
        appid: "app-1".to_string(),
        name: "Original".to_string(),
        enabled: true,
    })
    .await
    .expect("project should be created");

    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should load after create"),
        1
    );

    repo.update_project(
        "app-1",
        UpdateProjectInput {
            name: Some("Renamed".to_string()),
            enabled: Some(false),
        },
    )
    .await
    .expect("project should be updated");

    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should load after update"),
        2
    );

    repo.delete_project("app-1")
        .await
        .expect("project should be deleted");

    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should load after delete"),
        3
    );
}

#[tokio::test]
async fn duplicate_appid_returns_stable_error_and_does_not_bump_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    repo.create_project(CreateProjectInput {
        appid: "dup-app".to_string(),
        name: "Original".to_string(),
        enabled: true,
    })
    .await
    .expect("initial project should be created");

    let version_before = repo
        .projects_version()
        .await
        .expect("version should load before duplicate create");

    let error = repo
        .create_project(CreateProjectInput {
            appid: "dup-app".to_string(),
            name: "Duplicate".to_string(),
            enabled: false,
        })
        .await
        .expect_err("duplicate create should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::DuplicateAppid { ref appid } if appid == "dup-app"
    ));
    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should not change after failed create"),
        version_before
    );
}

#[tokio::test]
async fn update_missing_project_returns_not_found_and_does_not_bump_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    let version_before = repo
        .projects_version()
        .await
        .expect("version should load before failed update");

    let error = repo
        .update_project(
            "missing-app",
            UpdateProjectInput {
                name: Some("New Name".to_string()),
                enabled: Some(false),
            },
        )
        .await
        .expect_err("missing project update should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::NotFound { ref appid } if appid == "missing-app"
    ));
    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should not change after failed update"),
        version_before
    );
}

#[tokio::test]
async fn delete_missing_project_returns_not_found_and_does_not_bump_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    let version_before = repo
        .projects_version()
        .await
        .expect("version should load before failed delete");

    let error = repo
        .delete_project("missing-app")
        .await
        .expect_err("missing project delete should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::NotFound { ref appid } if appid == "missing-app"
    ));
    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should not change after failed delete"),
        version_before
    );
}

#[tokio::test]
async fn missing_version_metadata_returns_stable_error_for_reads_and_mutations() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db.clone());

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "DELETE FROM app_meta WHERE key = 'projects_version'",
    ))
    .await
    .expect("projects_version row should be deleted");

    let read_error = repo
        .projects_version()
        .await
        .expect_err("reading missing version metadata should fail");
    assert!(matches!(
        read_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    let create_error = repo
        .create_project(CreateProjectInput {
            appid: "create-missing-version".to_string(),
            name: "Create Missing Version".to_string(),
            enabled: true,
        })
        .await
        .expect_err("create should fail when version metadata is missing");
    assert!(matches!(
        create_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    let created = repo
        .get_project("create-missing-version")
        .await
        .expect("project lookup should succeed");
    assert!(created.is_none(), "failed create should be rolled back");
}

#[tokio::test]
async fn corrupted_version_metadata_returns_stable_error() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db.clone());

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "UPDATE app_meta SET value = 'broken' WHERE key = 'projects_version'",
    ))
    .await
    .expect("projects_version row should be corrupted");

    let error = repo
        .projects_version()
        .await
        .expect_err("corrupted version metadata should fail");
    assert!(matches!(
        error,
        ProjectRepositoryError::CorruptedVersion { ref value } if value == "broken"
    ));
}

#[tokio::test]
async fn update_and_delete_return_stable_error_when_version_metadata_is_missing() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db.clone());

    repo.create_project(CreateProjectInput {
        appid: "versioned-app".to_string(),
        name: "Versioned App".to_string(),
        enabled: true,
    })
    .await
    .expect("project should be created before removing version metadata");

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "DELETE FROM app_meta WHERE key = 'projects_version'",
    ))
    .await
    .expect("projects_version row should be deleted");

    let update_error = repo
        .update_project(
            "versioned-app",
            UpdateProjectInput {
                name: Some("Updated Name".to_string()),
                enabled: Some(false),
            },
        )
        .await
        .expect_err("update should fail when version metadata is missing");
    assert!(matches!(
        update_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    let loaded = repo
        .get_project("versioned-app")
        .await
        .expect("project lookup should succeed")
        .expect("project should still exist");
    assert_eq!(loaded.name, "Versioned App");
    assert!(loaded.enabled);

    let delete_error = repo
        .delete_project("versioned-app")
        .await
        .expect_err("delete should fail when version metadata is missing");
    assert!(matches!(
        delete_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    assert!(
        repo.get_project("versioned-app")
            .await
            .expect("project lookup should succeed")
            .is_some(),
        "failed delete should be rolled back"
    );
}
