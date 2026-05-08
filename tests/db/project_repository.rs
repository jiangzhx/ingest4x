use ingest4x::db::{init_sqlite_database, migrate};
use ingest4x::repositories::{
    hash_ingest_token, CreateProjectInput, ProjectRepository, ProjectRepositoryError,
    UpdateProjectInput,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

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
    assert_eq!(
        enabled_projects[0].ingest_token_hash,
        hash_ingest_token("igx_enabled_token")
    );
    assert_ne!(enabled_projects[0].ingest_token_hash, "igx_enabled_token");

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
async fn migration_backfills_ingest_tokens_for_legacy_appid_projects() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite database should connect");

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE seaql_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    applied_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("migration table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE app_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
)
"#,
    ))
    .await
    .expect("app_meta table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT INTO app_meta (key, value) VALUES ('projects_version', '0')",
    ))
    .await
    .expect("projects_version metadata should be inserted");

    for version in [
        "m20260425_000001_create_initial_schema",
        "m20260427_000002_add_rule_wildcard_flag",
        "m20260427_000003_move_wildcard_to_rule_set",
        "m20260508_000004_create_event_sinks",
        "m20260508_000005_create_processor_scripts",
    ] {
        db.execute(Statement::from_string(
            DbBackend::Sqlite,
            format!("INSERT INTO seaql_migrations (version, applied_at) VALUES ('{version}', 0)"),
        ))
        .await
        .expect("migration marker should be inserted");
    }

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    appid TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("legacy projects table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE project_rule_sets (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    project_id INTEGER NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
)
"#,
    ))
    .await
    .expect("project_rule_sets table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
INSERT INTO projects (appid, name, enabled, created_at, updated_at)
VALUES ('legacy-app', 'Legacy App', TRUE, 1, 1)
"#,
    ))
    .await
    .expect("legacy project should be inserted");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT INTO project_rule_sets (project_id) VALUES (1)",
    ))
    .await
    .expect("project binding should be inserted");

    migrate::run(&db)
        .await
        .expect("pending token migration should run");

    let repo = ProjectRepository::new(db.clone());
    let project = repo
        .find_enabled_project_by_ingest_token("igx_legacy-app")
        .await
        .expect("legacy token lookup should succeed")
        .expect("legacy project should be found by generated token");

    assert_eq!(project.name, "Legacy App");
    assert_eq!(project.ingest_token_prefix, "igx_legacy-a...");
    assert_project_rule_sets_references_projects(&db).await;

    repo.create_project(CreateProjectInput {
        name: "Created After Migration".to_string(),
        enabled: true,
        ingest_token: "igx_created_after_migration".to_string(),
    })
    .await
    .expect("projects created after migration should not require legacy appid");
}

async fn assert_project_rule_sets_references_projects(db: &DatabaseConnection) {
    let rows = db
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_key_list(project_rule_sets)",
        ))
        .await
        .expect("project_rule_sets foreign keys should be queryable");

    let referenced_tables = rows
        .into_iter()
        .map(|row| {
            row.try_get::<String>("", "table")
                .expect("foreign key table should be present")
        })
        .collect::<Vec<_>>();

    assert!(
        referenced_tables.iter().any(|table| table == "projects"),
        "project_rule_sets should keep referencing projects"
    );
    assert!(
        referenced_tables
            .iter()
            .all(|table| table != "projects_old"),
        "project_rule_sets should not reference temporary projects_old"
    );
}

#[tokio::test]
async fn migration_drops_legacy_appid_when_token_backfill_already_ran() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite database should connect");

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE seaql_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    applied_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("migration table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE app_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
)
"#,
    ))
    .await
    .expect("app_meta table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT INTO app_meta (key, value) VALUES ('projects_version', '0')",
    ))
    .await
    .expect("projects_version metadata should be inserted");

    for version in [
        "m20260425_000001_create_initial_schema",
        "m20260427_000002_add_rule_wildcard_flag",
        "m20260427_000003_move_wildcard_to_rule_set",
        "m20260508_000004_create_event_sinks",
        "m20260508_000005_create_processor_scripts",
        "m20260508_000006_add_project_ingest_tokens",
    ] {
        db.execute(Statement::from_string(
            DbBackend::Sqlite,
            format!("INSERT INTO seaql_migrations (version, applied_at) VALUES ('{version}', 0)"),
        ))
        .await
        .expect("migration marker should be inserted");
    }

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    appid TEXT NOT NULL UNIQUE,
    ingest_token_hash TEXT NOT NULL UNIQUE,
    ingest_token_prefix TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("legacy projects table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!(
            r#"
INSERT INTO projects (
    appid,
    ingest_token_hash,
    ingest_token_prefix,
    name,
    enabled,
    created_at,
    updated_at
)
VALUES (
    'legacy-app',
    '{}',
    'igx_legacy-a...',
    'Legacy App',
    TRUE,
    1,
    1
)
"#,
            hash_ingest_token("igx_legacy-app")
        ),
    ))
    .await
    .expect("legacy project should be inserted");

    migrate::run(&db)
        .await
        .expect("pending appid cleanup migration should run");

    let repo = ProjectRepository::new(db);
    repo.create_project(CreateProjectInput {
        name: "Created After Partial Migration".to_string(),
        enabled: true,
        ingest_token: "igx_created_after_partial_migration".to_string(),
    })
    .await
    .expect("projects created after cleanup should not require legacy appid");
}

#[tokio::test]
async fn migration_repairs_sqlite_projects_table_left_as_projects_new() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite database should connect");

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE seaql_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    applied_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("migration table should be created");

    for version in [
        "m20260425_000001_create_initial_schema",
        "m20260427_000002_add_rule_wildcard_flag",
        "m20260427_000003_move_wildcard_to_rule_set",
        "m20260508_000004_create_event_sinks",
        "m20260508_000005_create_processor_scripts",
        "m20260508_000006_add_project_ingest_tokens",
        "m20260508_000007_drop_legacy_project_appid",
    ] {
        db.execute(Statement::from_string(
            DbBackend::Sqlite,
            format!("INSERT INTO seaql_migrations (version, applied_at) VALUES ('{version}', 0)"),
        ))
        .await
        .expect("migration marker should be inserted");
    }

    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE app_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
)
"#,
    ))
    .await
    .expect("app_meta table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT INTO app_meta (key, value) VALUES ('projects_version', '0')",
    ))
    .await
    .expect("projects_version metadata should be inserted");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
CREATE TABLE projects_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ingest_token_hash TEXT NOT NULL UNIQUE,
    ingest_token_prefix TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
)
"#,
    ))
    .await
    .expect("projects_new table should be created");
    db.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!(
            r#"
INSERT INTO projects_new (
    ingest_token_hash,
    ingest_token_prefix,
    name,
    enabled,
    created_at,
    updated_at
)
VALUES (
    '{}',
    'igx_legacy-a...',
    'Legacy App',
    TRUE,
    1,
    1
)
"#,
            hash_ingest_token("igx_legacy-app")
        ),
    ))
    .await
    .expect("projects_new row should be inserted");

    migrate::run(&db)
        .await
        .expect("pending projects repair migration should run");

    let repo = ProjectRepository::new(db);
    let project = repo
        .find_enabled_project_by_ingest_token("igx_legacy-app")
        .await
        .expect("legacy token lookup should succeed")
        .expect("legacy project should be restored under projects table");

    assert_eq!(project.name, "Legacy App");
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
