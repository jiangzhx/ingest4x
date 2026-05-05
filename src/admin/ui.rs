use actix_files::{Files, NamedFile};
use actix_web::error::{ErrorInternalServerError, ErrorNotFound};
use actix_web::web::{self, ServiceConfig};
use actix_web::{HttpRequest, Result};
use std::env;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

pub fn configure(cfg: &mut ServiceConfig) {
    configure_with_dist_dir(cfg, admin_dist_dir());
}

pub fn configure_with_dist_dir(cfg: &mut ServiceConfig, dist_dir: impl AsRef<Path>) {
    let dist_dir = dist_dir.as_ref().to_path_buf();

    cfg.service(
        web::scope("/admin")
            .route(
                "",
                web::get().to({
                    let dist_dir = dist_dir.clone();
                    move || index(dist_dir.clone())
                }),
            )
            .route(
                "/",
                web::get().to({
                    let dist_dir = dist_dir.clone();
                    move || index(dist_dir.clone())
                }),
            )
            .service(Files::new("/assets", dist_dir.join("assets")))
            .default_service(web::get().to({
                let dist_dir = dist_dir.clone();
                move |request| spa_route(request, dist_dir.clone())
            })),
    );
}

async fn index(dist_dir: PathBuf) -> Result<NamedFile> {
    NamedFile::open_async(dist_dir.join("index.html"))
        .await
        .map_err(map_file_error)
}

async fn spa_route(request: HttpRequest, dist_dir: PathBuf) -> Result<NamedFile> {
    if is_static_file_path(admin_relative_path(request.path())) {
        return Err(ErrorNotFound("admin ui asset not found"));
    }

    index(dist_dir).await
}

fn admin_relative_path(path: &str) -> &str {
    path.strip_prefix("/admin/")
        .or_else(|| path.strip_prefix("/admin"))
        .unwrap_or(path)
}

fn is_static_file_path(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'))
}

fn admin_dist_dir() -> PathBuf {
    env::var_os("INGEST4X_ADMIN_UI_DIST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web/admin/dist"))
}

fn map_file_error(error: std::io::Error) -> actix_web::Error {
    match error.kind() {
        ErrorKind::NotFound => ErrorNotFound(error),
        _ => ErrorInternalServerError(error),
    }
}
