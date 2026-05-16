use crate::repositories::{
    CreateDeliveryTargetInput, CreateEventSinkInput, CreateProcessorScriptInput,
    CreateProcessorScriptModuleInput, CreateProjectInput, DeliveryTargetType, EventSinkRepository,
    ProcessorRepository, ProcessorScriptStatus, Project, ProjectRepository,
};
use crate::settings::AutoOffsetReset;
use crate::settings::{default_replay_sink_batch_max_bytes, default_replay_sink_batch_max_events};
use crate::sinks::kafka::{
    default_kafka_batch_num_messages, default_kafka_delivery_timeout_ms, default_kafka_linger_ms,
    default_kafka_queue_buffering_max_messages, default_kafka_queue_buffering_max_ms,
};
use serde_json::json;

const DEFAULT_PARQUET_BATCH_TIMEOUT: &str = "60s";

const DEFAULT_PROCESSOR_SCRIPT: &str = r#"
fn process(event, request) {
    try {
        let xwhat = event.required("xwhat").string().min(1);
        let os = event.required("xcontext.os").string().ignore_case().enum([
            "ios", "android", "harmony", "wechat", "toutiao", "tiktok",
        ]);

        event.required("appid").string().min(1);
        event.required("xcontext").object();
        event.required("xcontext.installid").string().min(1);

        if os.eq("ios") {
            event.any(["xcontext.idfa", "xcontext.caid"]).required();
        }

        if os.eq("android") || os.eq("harmony") {
            event.any(["xcontext.oaid", "xcontext.androidid"]).required();
        }

        if os.eq("wechat") {
            event.any(["xcontext.openid", "xcontext.unionid"]).required();
        }

        if os.eq("toutiao") || os.eq("tiktok") {
            event.required("xcontext.openid").string().min(1);
        }

        if xwhat.eq("register") {
            event.required("xwho").string().min(1);
        }

        if xwhat.eq("payment") {
            event.required("xwho").string().min(1);
            event.required("xcontext.transactionid").string().min(1);
            event.required("xcontext.paymenttype").string().min(1);
            event.required("xcontext.currencytype")
                .string()
                .enum(currencies());
            event.required("xcontext.currencyamount").number();
            event.optional("xcontext.paymentstatus").boolean();
        }

        if xwhat.eq("levelup") {
            event.required("xwho").string().min(1);
            event.required("xcontext.level").integer().gt(0);
        }

        emit(SINK_EVENTS, event);
    } catch (err) {
        emit(SINK_EVENTS_ERROR, event);
    }
}

fn currencies() {
    [
        "JPY", "EUR", "BRL", "HKD", "TWD", "COP", "MXN", "CHF",
        "CAD", "CLP", "AUD", "PEN", "GBP", "CRC", "PLN", "PYG",
        "BOB", "QAR", "ILS", "SEK", "RUB", "CNY", "USD", "ZAR", "SGD",
        "NZD", "BGN", "LKR", "IQD", "TRY", "AED", "DZD", "EGP",
        "IDR", "INR", "NGN", "NOK", "PHP", "PKR", "THB", "UAH",
        "VND",
    ]
}
"#;

const LOADTEST_PROCESSOR_SCRIPT: &str = r#"
fn process(event, request) {
    try {
        let xwhat = event.required("xwhat").string().min(1);
        let os = event.required("xcontext.os").string().ignore_case().enum([
            "ios", "android", "harmony", "wechat", "toutiao", "tiktok",
        ]);

        event.required("appid").string().min(1);
        event.required("xcontext").object();
        event.required("xcontext.installid").string().min(1);

        if os.eq("ios") {
            event.any(["xcontext.idfa", "xcontext.caid"]).required();
        }

        if os.eq("android") || os.eq("harmony") {
            event.any(["xcontext.oaid", "xcontext.androidid"]).required();
        }

        if os.eq("wechat") {
            event.any(["xcontext.openid", "xcontext.unionid"]).required();
        }

        if os.eq("toutiao") || os.eq("tiktok") {
            event.required("xcontext.openid").string().min(1);
        }

        if xwhat.eq("register") {
            event.required("xwho").string().min(1);
        }

        if xwhat.eq("payment") {
            event.required("xwho").string().min(1);
            event.required("xcontext.transactionid").string().min(1);
            event.required("xcontext.paymenttype").string().min(1);
            event.required("xcontext.currencytype")
                .string()
                .enum(currencies());
            event.required("xcontext.currencyamount").number();
            event.optional("xcontext.paymentstatus").boolean();
        }

        if xwhat.eq("levelup") {
            event.required("xwho").string().min(1);
            event.required("xcontext.level").integer().gt(0);
        }
    } catch (err) {
        if !event.contains("xcontext") || event["xcontext"] == () {
            event["xcontext"] = #{};
        }
        let xcontext = event["xcontext"];
        xcontext["loadtest_validation_code"] = loadtest_validation_code_from_error(err);
        event["xcontext"] = xcontext;
    }
    emit(SINK_LOADTEST_EVENTS, event);
}

fn loadtest_validation_code_from_error(err) {
    let message = `${err}`;

    if message.starts_with("missing required field `") {
        return "rules_required_field_missing";
    }
    if message.starts_with("at least one field is required: ") {
        return "rules_conditional_required_missing";
    }
    if message.contains("expected type `") {
        return "rules_field_type_mismatch";
    }
    if message.contains("must be one of [") {
        return "rules_enum_value_invalid";
    }
    if message.contains("could not be represented as f64") {
        return "rules_number_parse_failed";
    }
    if message.contains("must be greater than ")
        || message.contains("must be greater than or equal to ")
        || message.contains("must be less than ")
        || message.contains("must be less than or equal to ")
    {
        return "rules_number_constraint_failed";
    }

    "rules_script_execution_failed"
}

fn currencies() {
    [
        "JPY", "EUR", "BRL", "HKD", "TWD", "COP", "MXN", "CHF",
        "CAD", "CLP", "AUD", "PEN", "GBP", "CRC", "PLN", "PYG",
        "BOB", "QAR", "ILS", "SEK", "RUB", "CNY", "USD", "ZAR", "SGD",
        "NZD", "BGN", "LKR", "IQD", "TRY", "AED", "DZD", "EGP",
        "IDR", "INR", "NGN", "NOK", "PHP", "PKR", "THB", "UAH",
        "VND",
    ]
}
"#;

pub async fn run(
    project_repository: &ProjectRepository,
    event_sink_repository: &EventSinkRepository,
    processor_repository: &ProcessorRepository,
) -> std::io::Result<()> {
    ensure_local_kafka_delivery_target(event_sink_repository).await?;
    ensure_local_parquet_delivery_target(event_sink_repository).await?;
    ensure_default_event_sinks(event_sink_repository).await?;
    ensure_parquet_event_sink(event_sink_repository).await?;
    ensure_loadtest_event_sink(event_sink_repository).await?;
    ensure_test_project(project_repository).await?;
    ensure_loadtest_project(project_repository).await?;
    ensure_default_processor_script(processor_repository).await?;
    ensure_loadtest_processor_script(processor_repository).await?;
    ensure_default_processor_bindings(processor_repository).await?;
    ensure_loadtest_processor_binding(project_repository, processor_repository).await?;
    Ok(())
}

async fn ensure_local_kafka_delivery_target(
    repository: &EventSinkRepository,
) -> std::io::Result<()> {
    let existing = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if existing
        .iter()
        .any(|target| target.target_id == "local_kafka")
    {
        return Ok(());
    }

    repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "local_kafka".to_string(),
            name: "Local Kafka".to_string(),
            target_type: DeliveryTargetType::kafka(),
            config_json: json!({
                "bootstrap_servers": "127.0.0.1:9092",
                "delivery_timeout_ms": default_kafka_delivery_timeout_ms(),
                "queue_buffering_max_ms": default_kafka_queue_buffering_max_ms(),
                "batch_num_messages": default_kafka_batch_num_messages(),
                "queue_buffering_max_messages": default_kafka_queue_buffering_max_messages(),
                "linger_ms": default_kafka_linger_ms()
            }),
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_local_parquet_delivery_target(
    repository: &EventSinkRepository,
) -> std::io::Result<()> {
    let existing = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if existing
        .iter()
        .any(|target| target.target_id == "local_parquet")
    {
        return Ok(());
    }

    repository
        .create_delivery_target(CreateDeliveryTargetInput {
            target_id: "local_parquet".to_string(),
            name: "Local Parquet".to_string(),
            target_type: DeliveryTargetType::parse("parquet")
                .expect("parquet target type should be registered"),
            config_json: json!({
                "scheme": "fs",
                "options": {
                    "root": "data"
                }
            }),
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_default_event_sinks(repository: &EventSinkRepository) -> std::io::Result<()> {
    let existing = repository
        .list_event_sinks()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let missing_sink_ids = ["events", "events_error"]
        .into_iter()
        .filter(|sink_id| !existing.iter().any(|sink| sink.sink_id == *sink_id))
        .collect::<Vec<_>>();
    if missing_sink_ids.is_empty() {
        return Ok(());
    }

    let targets = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let target = match targets
        .into_iter()
        .find(|target| target.target_id == "default_stdout")
    {
        Some(target) => target,
        None => repository
            .create_delivery_target(CreateDeliveryTargetInput {
                target_id: "default_stdout".to_string(),
                name: "Default Stdout".to_string(),
                target_type: DeliveryTargetType::stdout(),
                config_json: json!({}),
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    };

    for sink_id in missing_sink_ids {
        repository
            .create_event_sink(CreateEventSinkInput {
                sink_id: sink_id.to_string(),
                name: sink_id.to_string(),
                delivery_target_id: target.id,
                destination_json: json!({}),
                auto_offset_reset: AutoOffsetReset::Latest,
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}

async fn ensure_loadtest_event_sink(repository: &EventSinkRepository) -> std::io::Result<()> {
    let targets = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let target = match targets
        .into_iter()
        .find(|target| target.target_id == "loadtest_blackhole")
    {
        Some(target) => target,
        None => repository
            .create_delivery_target(CreateDeliveryTargetInput {
                target_id: "loadtest_blackhole".to_string(),
                name: "Loadtest Blackhole".to_string(),
                target_type: DeliveryTargetType::blackhole(),
                config_json: json!({}),
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    };

    let existing = repository
        .list_event_sinks()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if existing
        .iter()
        .any(|sink| sink.sink_id == "loadtest_events")
    {
        return Ok(());
    }

    repository
        .create_event_sink(CreateEventSinkInput {
            sink_id: "loadtest_events".to_string(),
            name: "Loadtest Events".to_string(),
            delivery_target_id: target.id,
            destination_json: json!({}),
            auto_offset_reset: AutoOffsetReset::Latest,
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_parquet_event_sink(repository: &EventSinkRepository) -> std::io::Result<()> {
    let targets = repository
        .list_delivery_targets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let target = targets
        .into_iter()
        .find(|target| target.target_id == "local_parquet")
        .ok_or_else(|| std::io::Error::other("local parquet delivery target is missing"))?;

    let existing = repository
        .list_event_sinks()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if existing.iter().any(|sink| sink.sink_id == "parquet_events") {
        return Ok(());
    }

    repository
        .create_event_sink(CreateEventSinkInput {
            sink_id: "parquet_events".to_string(),
            name: "Parquet Events".to_string(),
            delivery_target_id: target.id,
            destination_json: json!({
                "path_prefix": "events",
                "batch": {
                    "max_events": default_replay_sink_batch_max_events(),
                    "max_bytes": default_replay_sink_batch_max_bytes(),
                    "timeout": DEFAULT_PARQUET_BATCH_TIMEOUT
                }
            }),
            auto_offset_reset: AutoOffsetReset::Latest,
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_test_project(repository: &ProjectRepository) -> std::io::Result<()> {
    const TEST_PROJECT_NAME: &str = "test_app";
    const TEST_INGEST_TOKEN: &str = "igx_local_test_token";

    if find_project_by_ingest_token(repository, TEST_INGEST_TOKEN)
        .await?
        .is_some()
    {
        return Ok(());
    }

    repository
        .create_project(CreateProjectInput {
            name: TEST_PROJECT_NAME.to_string(),
            enabled: true,
            ingest_token: TEST_INGEST_TOKEN.to_string(),
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_loadtest_project(repository: &ProjectRepository) -> std::io::Result<()> {
    const LOADTEST_PROJECT_NAME: &str = "loadtest_app";
    const LOADTEST_INGEST_TOKEN: &str = "igx_loadtest_token";

    if find_project_by_ingest_token(repository, LOADTEST_INGEST_TOKEN)
        .await?
        .is_some()
    {
        return Ok(());
    }

    repository
        .create_project(CreateProjectInput {
            name: LOADTEST_PROJECT_NAME.to_string(),
            enabled: true,
            ingest_token: LOADTEST_INGEST_TOKEN.to_string(),
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn find_project_by_ingest_token(
    repository: &ProjectRepository,
    ingest_token: &str,
) -> std::io::Result<Option<Project>> {
    Ok(repository
        .list_projects()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .into_iter()
        .find(|project| project.ingest_token == ingest_token))
}

async fn ensure_default_processor_script(repository: &ProcessorRepository) -> std::io::Result<()> {
    if repository.default_runtime_script().await.is_ok() {
        return Ok(());
    }

    repository
        .create_script(CreateProcessorScriptInput {
            script_key: "default".to_string(),
            name: "Default processor".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: DEFAULT_PROCESSOR_SCRIPT.to_string(),
            }],
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_loadtest_processor_script(repository: &ProcessorRepository) -> std::io::Result<()> {
    if repository
        .list_scripts()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .into_iter()
        .any(|script| script.script_key == "loadtest_blackhole_processor")
    {
        return Ok(());
    }

    repository
        .create_script(CreateProcessorScriptInput {
            script_key: "loadtest_blackhole_processor".to_string(),
            name: "Loadtest blackhole processor".to_string(),
            entry_module: "main".to_string(),
            status: ProcessorScriptStatus::Active,
            modules: vec![CreateProcessorScriptModuleInput {
                module_name: "main".to_string(),
                source: LOADTEST_PROCESSOR_SCRIPT.to_string(),
            }],
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_default_processor_bindings(
    repository: &ProcessorRepository,
) -> std::io::Result<()> {
    repository
        .ensure_default_project_processors()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_loadtest_processor_binding(
    project_repository: &ProjectRepository,
    processor_repository: &ProcessorRepository,
) -> std::io::Result<()> {
    let Some(project) =
        find_project_by_ingest_token(project_repository, "igx_loadtest_token").await?
    else {
        return Ok(());
    };
    let script = processor_repository
        .list_scripts()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .into_iter()
        .find(|script| script.script_key == "loadtest_blackhole_processor")
        .ok_or_else(|| std::io::Error::other("loadtest processor seed is missing"))?;

    processor_repository
        .assign_project_processor(project.id, script.id, true)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}
