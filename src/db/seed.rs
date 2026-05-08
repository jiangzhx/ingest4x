use crate::repositories::{
    CreateProcessorScriptInput, CreateProcessorScriptModuleInput, CreateProjectInput,
    CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput, ProcessorRepository,
    ProcessorScriptStatus, ProjectRepository, RuleRepository, UpdateRuleSetInput,
};

const DEFAULT_RULE_CONTENT: &str = r#"fields:
  appid:
    required: true
    type: string
  xwhat:
    required: true
    type: string
  xcontext:
    required: true
    type: object
  xcontext.installid:
    required: true
    type: string
  xcontext.os:
    required: true
    type: string
    enum:
      - ios
      - android
      - harmony
      - wechat
      - toutiao
      - tiktok
    required_any_when:
      - equals: ios
        fields: [xcontext.idfa, xcontext.caid]
      - equals: android
        fields: [xcontext.oaid, xcontext.androidid]
      - equals: harmony
        fields: [xcontext.oaid, xcontext.androidid]
      - equals: wechat
        fields: [xcontext.openid, xcontext.unionid]
    required_when:
      - equals: toutiao
        fields: [xcontext.openid]
      - equals: tiktok
        fields: [xcontext.openid]
"#;

const INSTALL_RULE_CONTENT: &str = r#"fields:
"#;

const STARTUP_RULE_CONTENT: &str = r#"fields:
"#;

const USER_DEFAULT_RULE_CONTENT: &str = r#"fields:
  xwho:
    required: true
    type: string
"#;

const REGISTER_RULE_CONTENT: &str = r#"fields:
"#;

const PAYMENT_RULE_CONTENT: &str = r#"fields:
  xcontext.transactionid:
    required: true
    type: string
  xcontext.paymenttype:
    required: true
    type: string
  xcontext.currencytype:
    required: true
    type: string
    enum:
      - JPY
      - EUR
      - BRL
      - HKD
      - TWD
      - COP
      - MXN
      - CHF
      - CAD
      - CLP
      - AUD
      - PEN
      - GBP
      - CRC
      - PLN
      - PYG
      - BOB
      - QAR
      - ILS
      - SEK
      - RUB
      - CNY
      - ZAR
      - SGD
      - NZD
      - BGN
      - LKR
      - IQD
      - TRY
      - AED
      - DZD
      - EGP
      - IDR
      - INR
      - NGN
      - NOK
      - PHP
      - PKR
      - THB
      - UAH
      - VND
  xcontext.currencyamount:
    required: true
    type: number
  xcontext.paymentstatus:
    type: boolean
"#;

const LEVELUP_RULE_CONTENT: &str = r#"fields:
  xcontext.level:
    required: true
    type: integer
    gt: 0
"#;

const DEFAULT_PROCESSOR_SCRIPT: &str = r#"
fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit("events", event);
    } else {
        emit("events_error", event);
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
    const TEST_APPID: &str = "test_app";

    if repository
        .get_project(TEST_APPID)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .is_some()
    {
        return Ok(());
    }

    repository
        .create_project(CreateProjectInput {
            appid: TEST_APPID.to_string(),
            name: TEST_APPID.to_string(),
            enabled: true,
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
            .list_project_rule_sets(&project.appid)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if !existing.is_empty() {
            continue;
        }

        rule_repository
            .assign_rule_set_to_project(
                &project.appid,
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
            name: "默认规则".to_string(),
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

    for (name, xwhat, content) in [
        ("安装", "install", INSTALL_RULE_CONTENT),
        ("启动", "startup", STARTUP_RULE_CONTENT),
    ] {
        repository
            .create_rule(CreateRuleInput {
                rule_set_id,
                parent_id: Some(base.id),
                name: name.to_string(),
                xwhat: Some(xwhat.to_string()),
                content: content.to_string(),
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    let user = repository
        .create_rule(CreateRuleInput {
            rule_set_id,
            parent_id: Some(base.id),
            name: "用户事件基础规则".to_string(),
            xwhat: None,
            content: USER_DEFAULT_RULE_CONTENT.to_string(),
            enabled: true,
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    for (name, xwhat, content) in [
        ("注册", "register", REGISTER_RULE_CONTENT),
        ("支付", "payment", PAYMENT_RULE_CONTENT),
        ("升级", "levelup", LEVELUP_RULE_CONTENT),
    ] {
        repository
            .create_rule(CreateRuleInput {
                rule_set_id,
                parent_id: Some(user.id),
                name: name.to_string(),
                xwhat: Some(xwhat.to_string()),
                content: content.to_string(),
                enabled: true,
            })
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }

    Ok(())
}
