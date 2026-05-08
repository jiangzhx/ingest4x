use crate::repositories::RuleRepositoryError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

pub(crate) const QUARANTINE_LOG_TARGET: &str = "ingest4x::wal::quarantine";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayAction {
    // WAL/replay state is not trustworthy enough to continue.
    StopReplay,
    // This record cannot be delivered under current business config; isolate it.
    QuarantineRecord,
    // Delivery intent is valid, but the downstream sink failed.
    BlockSink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayIssue {
    // WAL frame, segment, LSN, or checkpoint state problems.
    Wal {
        code: &'static str,
        message: String,
    },
    // The trusted WAL record body cannot be interpreted as a valid event.
    EventPayload {
        code: &'static str,
        message: String,
        appid: Option<String>,
        xwhat: Option<String>,
    },
    // Project metadata or rules make this project impossible to process now.
    Project {
        code: &'static str,
        message: String,
        project_id: i32,
    },
    // Rhai processor execution failed after payload and project checks passed.
    Processor {
        code: &'static str,
        message: String,
    },
    // Rhai emitted an invalid delivery plan, such as an unknown sink target.
    DeliveryPlan {
        code: &'static str,
        message: String,
        target: Option<String>,
    },
    // Sink exists, but the actual downstream write failed.
    Sink {
        code: &'static str,
        message: String,
        sink: String,
    },
    // Local replay side effects such as quarantine/checkpoint writes failed.
    Runtime {
        code: &'static str,
        message: String,
    },
}

impl ReplayAction {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StopReplay => "stop_replay",
            Self::QuarantineRecord => "quarantine_record",
            Self::BlockSink => "block_sink",
        }
    }
}

impl ReplayIssue {
    pub(crate) fn wal_lsn_gap(expected: u64, actual: u64) -> Self {
        Self::Wal {
            code: "replay_wal_lsn_gap",
            message: format!("non-contiguous wal lsn: expected {expected}, got {actual}"),
        }
    }

    pub(crate) fn wal_read_failed(error: io::Error) -> Self {
        let message = error.to_string();
        let code = if error.kind() == io::ErrorKind::InvalidData {
            if message.contains("crc mismatch") {
                "replay_wal_crc_mismatch"
            } else if message.contains("segment header") {
                "replay_segment_header_corrupt"
            } else {
                "replay_wal_frame_corrupt"
            }
        } else {
            "replay_wal_read_failed"
        };
        Self::Wal { code, message }
    }

    pub(crate) fn checkpoint_corrupt(error: impl Display) -> Self {
        Self::Wal {
            code: "replay_checkpoint_corrupt",
            message: error.to_string(),
        }
    }

    pub(crate) fn checkpoint_config_invalid(error: impl Display) -> Self {
        Self::Runtime {
            code: "replay_checkpoint_config_invalid",
            message: error.to_string(),
        }
    }

    pub(crate) fn checkpoint_write_failed(error: impl Display) -> Self {
        Self::Runtime {
            code: "replay_checkpoint_write_failed",
            message: error.to_string(),
        }
    }

    pub(crate) fn invalid_json_body(error: impl Display) -> Self {
        Self::EventPayload {
            code: "replay_invalid_json_body",
            message: format!("invalid wal record json body: {error}"),
            appid: None,
            xwhat: None,
        }
    }

    pub(crate) fn missing_appid(xwhat: Option<String>) -> Self {
        Self::EventPayload {
            code: "replay_missing_appid",
            message: "missing or invalid appid".to_string(),
            appid: None,
            xwhat,
        }
    }

    pub(crate) fn unknown_project_id(project_id: i32, _xwhat: Option<String>) -> Self {
        Self::EventPayload {
            code: "replay_unknown_project",
            message: format!("wal record references unknown project `{project_id}`"),
            appid: None,
            xwhat: _xwhat,
        }
    }

    pub(crate) fn processor_runtime_failed(error: impl Display) -> Self {
        Self::Processor {
            code: "replay_processor_runtime_failed",
            message: error.to_string(),
        }
    }

    pub(crate) fn unknown_sink_target(target: String) -> Self {
        Self::DeliveryPlan {
            code: "replay_unknown_sink_target",
            message: format!("processor emitted unknown sink target `{target}`"),
            target: Some(target),
        }
    }

    pub(crate) fn empty_sink_target() -> Self {
        Self::DeliveryPlan {
            code: "replay_empty_sink_target",
            message: "processor emitted empty sink target".to_string(),
            target: None,
        }
    }

    pub(crate) fn sink_send_failed(sink: String, lsn: u64, error: impl Display) -> Self {
        Self::Sink {
            code: "replay_sink_send_failed",
            message: format!("sink `{sink}` failed at lsn {lsn}: {error}"),
            sink,
        }
    }

    pub(crate) fn from_rule_repository(project_id: i32, error: RuleRepositoryError) -> Self {
        match error {
            RuleRepositoryError::ProjectNotFound { id } => Self::Project {
                code: "replay_project_not_found",
                message: format!("project `{id}` not found while compiling rules"),
                project_id: id,
            },
            RuleRepositoryError::InvalidRuleContent { message } => Self::Project {
                code: "replay_rules_invalid",
                message,
                project_id,
            },
            RuleRepositoryError::DuplicateRuntimeRule { xwhat } => Self::Project {
                code: "replay_rules_duplicate_runtime_rule",
                message: format!("multiple enabled rules matched xwhat `{xwhat}`"),
                project_id,
            },
            RuleRepositoryError::Database(error) => Self::Project {
                code: "replay_control_plane_unavailable",
                message: error.to_string(),
                project_id,
            },
            other => Self::Project {
                code: "replay_rules_invalid",
                message: other.to_string(),
                project_id,
            },
        }
    }

    pub(crate) fn action(&self) -> ReplayAction {
        match self {
            Self::Wal { .. } | Self::Runtime { .. } => ReplayAction::StopReplay,
            Self::EventPayload { .. } | Self::Processor { .. } | Self::DeliveryPlan { .. } => {
                ReplayAction::QuarantineRecord
            }
            Self::Project { code, .. } if *code == "replay_control_plane_unavailable" => {
                ReplayAction::StopReplay
            }
            Self::Project { .. } => ReplayAction::QuarantineRecord,
            Self::Sink { .. } => ReplayAction::BlockSink,
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Wal { code, .. }
            | Self::EventPayload { code, .. }
            | Self::Project { code, .. }
            | Self::Processor { code, .. }
            | Self::DeliveryPlan { code, .. }
            | Self::Sink { code, .. }
            | Self::Runtime { code, .. } => code,
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Wal { message, .. }
            | Self::EventPayload { message, .. }
            | Self::Project { message, .. }
            | Self::Processor { message, .. }
            | Self::DeliveryPlan { message, .. }
            | Self::Sink { message, .. }
            | Self::Runtime { message, .. } => message,
        }
    }

    pub(crate) fn appid(&self) -> Option<&str> {
        match self {
            Self::EventPayload { appid, .. } => appid.as_deref(),
            _ => None,
        }
    }

    pub(crate) fn xwhat(&self) -> Option<&str> {
        match self {
            Self::EventPayload { xwhat, .. } => xwhat.as_deref(),
            _ => None,
        }
    }

    pub(crate) fn target(&self) -> Option<&str> {
        match self {
            Self::DeliveryPlan { target, .. } => target.as_deref(),
            Self::Sink { sink, .. } => Some(sink.as_str()),
            _ => None,
        }
    }
}

impl Display for ReplayIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl Error for ReplayIssue {}

#[cfg(test)]
mod tests {
    use super::{ReplayAction, ReplayIssue};
    use crate::repositories::RuleRepositoryError;
    use sea_orm::DbErr;

    #[test]
    fn rule_repository_errors_map_to_project_issue_actions() {
        let invalid_rules = ReplayIssue::from_rule_repository(
            42,
            RuleRepositoryError::InvalidRuleContent {
                message: "bad yaml".to_string(),
            },
        );
        assert_eq!(invalid_rules.code(), "replay_rules_invalid");
        assert_eq!(invalid_rules.action(), ReplayAction::QuarantineRecord);

        let database_error = ReplayIssue::from_rule_repository(
            42,
            RuleRepositoryError::Database(DbErr::Custom("db down".to_string())),
        );
        assert_eq!(database_error.code(), "replay_control_plane_unavailable");
        assert_eq!(database_error.action(), ReplayAction::StopReplay);
    }
}
