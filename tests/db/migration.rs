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
}
