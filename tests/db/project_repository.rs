use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{
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
        name: "Disabled".to_string(),
        enabled: false,
        ingest_token: "igx_disabled_token".to_string(),
    })
    .await
    .expect("disabled project should be created");

    repo.create_project(CreateProjectInput {
        name: "Enabled".to_string(),
        enabled: true,
        ingest_token: "igx_enabled_token".to_string(),
    })
    .await
    .expect("enabled project should be created");

    let enabled_projects = repo
        .list_enabled_projects()
        .await
        .expect("enabled projects should list");

    assert_eq!(enabled_projects.len(), 1);
    assert_eq!(enabled_projects[0].name, "Enabled");
    assert!(enabled_projects[0].enabled);
    assert_eq!(enabled_projects[0].ingest_token, "igx_enabled_token");

    let all_projects = repo.list_projects().await.expect("projects should list");
    assert_eq!(all_projects.len(), 2);

    let loaded = repo
        .find_enabled_project_by_ingest_token("igx_enabled_token")
        .await
        .expect("project lookup should succeed")
        .expect("enabled project should exist");
    assert_eq!(loaded.name, "Enabled");

    assert!(repo
        .find_enabled_project_by_ingest_token("igx_disabled_token")
        .await
        .expect("disabled token lookup should succeed")
        .is_none());
    assert!(repo
        .find_enabled_project_by_ingest_token("igx_missing_token")
        .await
        .expect("missing token lookup should succeed")
        .is_none());
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
        name: "Original".to_string(),
        enabled: true,
        ingest_token: "igx_app_1_token".to_string(),
    })
    .await
    .expect("project should be created");

    assert_eq!(
        repo.projects_version()
            .await
            .expect("version should load after create"),
        1
    );

    let project_id = repo
        .find_enabled_project_by_ingest_token("igx_app_1_token")
        .await
        .expect("project lookup should succeed")
        .expect("project should exist")
        .id;

    repo.update_project(
        project_id,
        UpdateProjectInput {
            name: Some("Renamed".to_string()),
            enabled: Some(false),
            ingest_token: None,
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

    repo.delete_project(project_id)
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
async fn update_project_can_replace_ingest_token() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    let created = repo
        .create_project(CreateProjectInput {
            name: "Token Project".to_string(),
            enabled: true,
            ingest_token: "igx_original_token".to_string(),
        })
        .await
        .expect("project should be created");

    let updated = repo
        .update_project(
            created.id,
            UpdateProjectInput {
                name: None,
                enabled: None,
                ingest_token: Some("igx_replaced_token".to_string()),
            },
        )
        .await
        .expect("project token should be replaced");

    assert_eq!(updated.ingest_token, "igx_replaced_token");
    assert!(repo
        .find_enabled_project_by_ingest_token("igx_original_token")
        .await
        .expect("old token lookup should succeed")
        .is_none());
    assert_eq!(
        repo.find_enabled_project_by_ingest_token("igx_replaced_token")
            .await
            .expect("new token lookup should succeed")
            .expect("new token should authenticate")
            .id,
        created.id
    );
}

#[tokio::test]
async fn duplicate_ingest_token_returns_stable_error_and_does_not_bump_version() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let repo = ProjectRepository::new(db);

    repo.create_project(CreateProjectInput {
        name: "Original".to_string(),
        enabled: true,
        ingest_token: "igx_dup_token".to_string(),
    })
    .await
    .expect("initial project should be created");

    let version_before = repo
        .projects_version()
        .await
        .expect("version should load before duplicate create");

    let error = repo
        .create_project(CreateProjectInput {
            name: "Duplicate".to_string(),
            enabled: false,
            ingest_token: "igx_dup_token".to_string(),
        })
        .await
        .expect_err("duplicate create should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::DuplicateIngestToken { ref ingest_token_prefix } if ingest_token_prefix == "igx_dup_toke..."
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
            404,
            UpdateProjectInput {
                name: Some("New Name".to_string()),
                enabled: Some(false),
                ingest_token: None,
            },
        )
        .await
        .expect_err("missing project update should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::NotFound { id } if id == 404
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
        .delete_project(404)
        .await
        .expect_err("missing project delete should fail");

    assert!(matches!(
        error,
        ProjectRepositoryError::NotFound { id } if id == 404
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
            name: "Create Missing Version".to_string(),
            enabled: true,
            ingest_token: "igx_create_missing_version".to_string(),
        })
        .await
        .expect_err("create should fail when version metadata is missing");
    assert!(matches!(
        create_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    let created = repo
        .find_enabled_project_by_ingest_token("igx_create_missing_version")
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
        name: "Versioned App".to_string(),
        enabled: true,
        ingest_token: "igx_versioned_app".to_string(),
    })
    .await
    .expect("project should be created before removing version metadata");

    let project_id = repo
        .find_enabled_project_by_ingest_token("igx_versioned_app")
        .await
        .expect("project lookup should succeed")
        .expect("project should exist")
        .id;

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "DELETE FROM app_meta WHERE key = 'projects_version'",
    ))
    .await
    .expect("projects_version row should be deleted");

    let update_error = repo
        .update_project(
            project_id,
            UpdateProjectInput {
                name: Some("Updated Name".to_string()),
                enabled: Some(false),
                ingest_token: None,
            },
        )
        .await
        .expect_err("update should fail when version metadata is missing");
    assert!(matches!(
        update_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    let loaded = repo
        .get_project(project_id)
        .await
        .expect("project lookup should succeed")
        .expect("project should still exist");
    assert_eq!(loaded.name, "Versioned App");
    assert!(loaded.enabled);

    let delete_error = repo
        .delete_project(project_id)
        .await
        .expect_err("delete should fail when version metadata is missing");
    assert!(matches!(
        delete_error,
        ProjectRepositoryError::VersionMetadataMissing
    ));

    assert!(
        repo.get_project(project_id)
            .await
            .expect("project lookup should succeed")
            .is_some(),
        "failed delete should be rolled back"
    );
}
