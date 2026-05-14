use ingest4x::db::migrate::Migrator;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::prelude::MigratorTrait;

#[tokio::test]
async fn migrator_creates_current_sqlite_schema() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite database should connect");

    Migrator::up(&db, None)
        .await
        .expect("migrations should run");

    let tables = db
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        ))
        .await
        .expect("tables should query");

    let table_names = tables
        .into_iter()
        .map(|row| row.try_get::<String>("", "name").expect("table name"))
        .collect::<Vec<_>>();

    for expected in [
        "app_meta",
        "delivery_targets",
        "event_sinks",
        "processor_script_modules",
        "processor_scripts",
        "project_processors",
        "project_rule_sets",
        "projects",
        "rule_sets",
        "rules",
        "seaql_migrations",
        "service_nodes",
    ] {
        assert!(
            table_names.iter().any(|table| table == expected),
            "missing table {expected}; found {table_names:?}"
        );
    }

    let version = db
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT value FROM app_meta WHERE key = 'projects_version'",
        ))
        .await
        .expect("metadata should query")
        .expect("projects version metadata should exist")
        .try_get::<String>("", "value")
        .expect("projects version value");

    assert_eq!(version, "0");

    let event_sinks_version = db
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT value FROM app_meta WHERE key = 'event_sinks_version'",
        ))
        .await
        .expect("event sinks metadata should query")
        .expect("event sinks version metadata should exist")
        .try_get::<String>("", "value")
        .expect("event sinks version value");

    assert_eq!(event_sinks_version, "0");

    let processor_scripts_version = db
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT value FROM app_meta WHERE key = 'processor_scripts_version'",
        ))
        .await
        .expect("processor scripts metadata should query")
        .expect("processor scripts version metadata should exist")
        .try_get::<String>("", "value")
        .expect("processor scripts version value");

    assert_eq!(processor_scripts_version, "0");

    for expected_column in ["project_key", "auth_mode", "allowed_ips"] {
        assert!(
            sqlite_column_exists(&db, "projects", expected_column).await,
            "missing projects.{expected_column}"
        );
    }
}

#[tokio::test]
async fn migrator_repairs_legacy_project_auth_strategy_column() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite database should connect");

    Migrator::up(&db, Some(2))
        .await
        .expect("base migrations should run");

    for sql in [
        "INSERT INTO projects (ingest_token, name, enabled, created_at, updated_at) VALUES ('legacy-token', 'Legacy Project', 1, 1, 1)",
        "ALTER TABLE projects ADD COLUMN project_key TEXT",
        "UPDATE projects SET project_key = 'legacy-app'",
        "ALTER TABLE projects ADD COLUMN auth_strategy TEXT NOT NULL DEFAULT 'token'",
        "UPDATE projects SET auth_strategy = 'ip'",
        "ALTER TABLE projects ADD COLUMN allowed_ips TEXT NOT NULL DEFAULT '[]'",
        "UPDATE projects SET allowed_ips = '[\"127.0.0.1\"]'",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_project_key ON projects(project_key)",
        "INSERT INTO seaql_migrations (version, applied_at) VALUES ('m20260513_000003_add_project_ingest_auth', 1)",
    ] {
        db.execute(Statement::from_string(DbBackend::Sqlite, sql))
            .await
            .expect("legacy schema setup should run");
    }

    Migrator::up(&db, None)
        .await
        .expect("repair migration should run");

    assert!(sqlite_column_exists(&db, "projects", "auth_mode").await);

    let auth_mode = db
        .query_one(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT auth_mode FROM projects WHERE project_key = 'legacy-app'",
        ))
        .await
        .expect("project should query")
        .expect("project should exist")
        .try_get::<String>("", "auth_mode")
        .expect("auth_mode should read");

    assert_eq!(auth_mode, "public");
}

async fn sqlite_column_exists(db: &sea_orm::DatabaseConnection, table: &str, column: &str) -> bool {
    db.query_one(Statement::from_string(
        DbBackend::Sqlite,
        format!(
            "SELECT COUNT(*) AS count FROM pragma_table_info('{table}') WHERE name = '{column}'"
        ),
    ))
    .await
    .expect("column metadata should query")
    .and_then(|row| row.try_get::<i64>("", "count").ok())
    .unwrap_or_default()
        > 0
}
