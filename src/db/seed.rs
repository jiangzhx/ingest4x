use crate::repositories::{
    CreateProcessorScriptInput, CreateProcessorScriptModuleInput, CreateProjectInput,
    CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput, ProcessorRepository,
    ProcessorScriptStatus, ProjectRepository, RuleRepository, UpdateRuleSetInput,
};

const DEFAULT_RULE_CONTENT: &str = r#"fn validate(event) {
    event.required("appid").string().min(1);
    let xwhat = event.required("xwhat").string().min(1);

    event.required("xcontext").object();
    event.required("xcontext.installid").string().min(1);

    let os = event.required("xcontext.os").string().enum([
        "ios", "iOS", "android", "harmony", "wechat", "toutiao", "tiktok",
    ]);

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

    event.result()
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

const DEFAULT_PROCESSOR_SCRIPT: &str = r#"
fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit(SINK_EVENTS, event);
    } else {
        emit(SINK_EVENTS_ERROR, event);
    }
}
"#;

pub async fn run(
    project_repository: &ProjectRepository,
    rule_repository: &RuleRepository,
    processor_repository: &ProcessorRepository,
) -> std::io::Result<()> {
    ensure_test_project(project_repository).await?;
    ensure_default_rule_set_imported(rule_repository, project_repository).await?;
    ensure_default_processor_script(processor_repository).await?;
    ensure_default_processor_bindings(processor_repository).await
}

async fn ensure_test_project(repository: &ProjectRepository) -> std::io::Result<()> {
    const TEST_PROJECT_NAME: &str = "test_app";
    const TEST_INGEST_TOKEN: &str = "igx_local_test_token";

    if repository
        .find_enabled_project_by_ingest_token(TEST_INGEST_TOKEN)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
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

async fn ensure_default_processor_bindings(
    repository: &ProcessorRepository,
) -> std::io::Result<()> {
    repository
        .ensure_default_project_processors()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}

async fn ensure_default_rule_set_imported(
    rule_repository: &RuleRepository,
    project_repository: &ProjectRepository,
) -> std::io::Result<()> {
    const DEFAULT_RULE_SET_NAME: &str = "Default ingest rules";

    let rule_set = match rule_repository
        .list_rule_sets()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .into_iter()
        .find(|rule_set| rule_set.name == DEFAULT_RULE_SET_NAME)
    {
        Some(rule_set) => rule_set,
        None => {
            let rule_set = rule_repository
                .create_rule_set(CreateRuleSetInput {
                    name: DEFAULT_RULE_SET_NAME.to_string(),
                    description: Some("Built-in ingest seed rules".to_string()),
                    enabled: true,
                })
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;

            import_default_ingest_rules(rule_repository, rule_set.id).await?;
            rule_set
        }
    };

    for project in project_repository
        .list_projects()
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
    {
        let existing = rule_repository
            .list_project_rule_sets(project.id)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if !existing.is_empty() {
            continue;
        }

        rule_repository
            .assign_rule_set_to_project(
                project.id,
                CreateProjectRuleSetInput {
                    rule_set_id: rule_set.id,
                    enabled: true,
                },
            )
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}

async fn import_default_ingest_rules(
    repository: &RuleRepository,
    rule_set_id: i32,
) -> std::io::Result<()> {
    let base = repository
        .create_rule(CreateRuleInput {
            rule_set_id,
            parent_id: None,
            name: "Validation rule".to_string(),
            xwhat: None,
            content: DEFAULT_RULE_CONTENT.to_string(),
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    repository
        .update_rule_set(
            rule_set_id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(base.id)),
            },
        )
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    Ok(())
}
