use crate::settings::Settings;
use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::{self, Data, Json, ServiceConfig};
use actix_web::{HttpResponse, Result};
use serde::Deserialize;
use std::sync::Arc;
use utoipa::ToSchema;

pub const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";

#[derive(Debug, Deserialize, ToSchema)]
struct LoginRequest {
    password: String,
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(web::scope("/auth").route("/login", web::post().to(login)));
}

#[utoipa::path(
    post,
    path = "/api/admin/auth/login",
    request_body = LoginRequest,
    tag = "admin.auth",
    responses(
        (status = 204, description = "Password accepted"),
        (status = 401, description = "Invalid password")
    )
)]
async fn login(request: Json<LoginRequest>, settings: Data<Arc<Settings>>) -> HttpResponse {
    if verify_admin_password(&request.password, settings.as_ref().as_ref()) {
        HttpResponse::NoContent().finish()
    } else {
        unauthorized_response()
    }
}

pub async fn require_admin_password<B>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<EitherBody<B>>>
where
    B: MessageBody + 'static,
{
    let settings = req
        .app_data::<Data<Arc<Settings>>>()
        .map(|value| value.as_ref().clone());

    let provided_password = req
        .headers()
        .get(ADMIN_PASSWORD_HEADER)
        .and_then(|value| value.to_str().ok());

    if provided_password.is_some_and(|candidate| {
        settings
            .as_ref()
            .is_some_and(|settings| verify_admin_password(candidate, settings.as_ref()))
    }) {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_left_body);
    }

    Ok(req
        .into_response(unauthorized_response())
        .map_into_right_body())
}

pub fn verify_admin_password(candidate: &str, settings: &Settings) -> bool {
    configured_admin_password(settings).is_some_and(|password| candidate == password)
}

fn configured_admin_password(settings: &Settings) -> Option<String> {
    std::env::var("INGEST4X_ADMIN_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| settings.management.admin_password.clone())
        .filter(|value| !value.is_empty())
}

fn unauthorized_response() -> HttpResponse {
    HttpResponse::Unauthorized().finish()
}

#[cfg(test)]
mod tests {
    use super::verify_admin_password;
    use crate::settings::Settings;

    struct EnvVarGuard {
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn remove(key: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var("INGEST4X_ADMIN_PASSWORD", value),
                None => std::env::remove_var("INGEST4X_ADMIN_PASSWORD"),
            }
        }
    }

    fn settings_with_admin_password(admin_password: Option<&str>) -> Settings {
        Settings {
            ingest: crate::settings::IngestSettings {
                bind_address: "127.0.0.1:0".to_string(),
                max_event_bytes: crate::settings::default_max_event_bytes(),
            },
            logging: Default::default(),
            management: crate::settings::ManagementSettings {
                bind_address: "127.0.0.1:0".to_string(),
                admin_password: admin_password.map(str::to_string),
            },
            database: None,
            wal: crate::settings::WalSettings {
                dir: "./wal".to_string(),
                node_id: None,
                flush_max_interval: crate::settings::default_wal_flush_max_interval(),
                flush_max_records: crate::settings::default_wal_flush_max_records(),
                no_sync: false,
                wal_segment_max_bytes: crate::settings::default_wal_segment_max_bytes(),
                min_free_bytes: 0,
                checkpoint: Default::default(),
                replay: Default::default(),
            },
        }
    }

    #[test]
    fn configured_admin_password_accepts_matching_candidate() {
        let _guard = EnvVarGuard::remove("INGEST4X_ADMIN_PASSWORD");

        let accepted = verify_admin_password(
            "configured-password",
            &settings_with_admin_password(Some("configured-password")),
        );

        assert!(accepted);
    }

    #[test]
    fn missing_admin_password_rejects_candidates() {
        let _guard = EnvVarGuard::remove("INGEST4X_ADMIN_PASSWORD");

        let accepted = verify_admin_password("ingest4x", &settings_with_admin_password(None));

        assert!(!accepted);
    }
}
