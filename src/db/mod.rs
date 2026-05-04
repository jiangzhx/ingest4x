pub mod migrate;
pub mod seed;

use sea_orm::sqlx::sqlite::SqliteJournalMode;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};
use std::path::Path;
use std::time::Duration;

pub async fn init_database(url: &str) -> Result<DatabaseConnection, DbErr> {
    ensure_sqlite_parent_dir(url)?;

    let mut options = ConnectOptions::new(url);
    options.sqlx_logging(false);
    options.connect_timeout(Duration::from_secs(5));
    options.acquire_timeout(Duration::from_secs(5));
    options.max_connections(default_max_connections(url));

    if is_sqlite_url(url) {
        let in_memory = is_in_memory_sqlite_url(url);
        options.map_sqlx_sqlite_opts(move |options| {
            let options = options.busy_timeout(Duration::from_secs(5));

            if in_memory {
                options
            } else {
                options
                    .journal_mode(SqliteJournalMode::Wal)
                    .pragma("synchronous", "NORMAL")
            }
        });
    }

    let db = Database::connect(options).await?;
    migrate::run(&db).await?;
    Ok(db)
}

pub async fn init_sqlite_database(url: &str) -> Result<DatabaseConnection, DbErr> {
    init_database(url).await
}

fn default_max_connections(url: &str) -> u32 {
    if is_in_memory_sqlite_url(url) {
        1
    } else {
        10
    }
}

fn is_in_memory_sqlite_url(url: &str) -> bool {
    url == "sqlite::memory:" || url.contains("mode=memory")
}

fn is_sqlite_url(url: &str) -> bool {
    url == "sqlite::memory:" || url.starts_with("sqlite:")
}

fn ensure_sqlite_parent_dir(url: &str) -> Result<(), DbErr> {
    let Some(path) = sqlite_file_path(url) else {
        return Ok(());
    };

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| DbErr::Custom(error.to_string()))?;
    }

    Ok(())
}

fn sqlite_file_path(url: &str) -> Option<&Path> {
    if is_in_memory_sqlite_url(url) {
        return None;
    }

    let path = url.strip_prefix("sqlite://")?.split('?').next()?;
    if path.is_empty() {
        return None;
    }

    Some(Path::new(path))
}

#[cfg(test)]
mod tests {
    use super::{init_sqlite_database, sqlite_file_path};
    use std::path::Path;
    use tempfile::tempdir;

    #[tokio::test]
    async fn init_sqlite_database_creates_missing_parent_directory() {
        let temp = tempdir().expect("temp dir");
        let db_path = temp.path().join("db").join("ingest4x.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        init_sqlite_database(&url)
            .await
            .expect("database should initialize");

        assert!(db_path.exists());
    }

    #[test]
    fn sqlite_file_path_extracts_relative_and_absolute_paths() {
        assert_eq!(
            sqlite_file_path("sqlite://db/ingest4x.db?mode=rwc"),
            Some(Path::new("db/ingest4x.db"))
        );
        assert_eq!(
            sqlite_file_path("sqlite:///tmp/ingest4x.db?mode=rwc"),
            Some(Path::new("/tmp/ingest4x.db"))
        );
        assert_eq!(sqlite_file_path("sqlite::memory:"), None);
    }
}
