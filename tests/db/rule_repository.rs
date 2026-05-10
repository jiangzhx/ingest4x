use ingest4x::db::init_sqlite_database;
use ingest4x::repositories::{
    CreateProjectInput, CreateProjectRuleSetInput, CreateRuleInput, CreateRuleSetInput,
    ProjectRepository, RuleRepository, UpdateRuleInput, UpdateRuleSetInput,
};
use serde_json::json;

#[tokio::test]
async fn upsert_rhai_validation_rule_replaces_legacy_rule_tree() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Base".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("base rule should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(base.id),
            name: "Payment".to_string(),
            xwhat: Some("payment".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("child rule should be created");

    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(base.id)),
            },
        )
        .await
        .expect("base rule should become wildcard rule");

    let validation_rule = rules
        .upsert_rhai_validation_rule(
            rule_set.id,
            r#"
fn validate(event) {
    event.field("appid").required("string");
    event.result()
}
"#,
            true,
        )
        .await
        .expect("rhai validation rule should be upserted");

    assert_eq!(validation_rule.parent_id, None);
    assert_eq!(validation_rule.xwhat, None);
    assert!(validation_rule.content.contains("fn validate(event)"));

    let rule_set = rules
        .get_rule_set(rule_set.id)
        .await
        .expect("rule set lookup should succeed")
        .expect("rule set should exist");
    assert_eq!(rule_set.wildcard_rule_id, Some(validation_rule.id));

    let remaining = rules
        .list_rules(rule_set.id)
        .await
        .expect("rules should list");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, validation_rule.id);
}

#[tokio::test]
async fn project_bound_rule_set_can_compile_single_rhai_validation_rule() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Rhai ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let root_rule = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Rhai default".to_string(),
            xwhat: None,
            content: r#"
fn validate(event) {
    event.field("appid").required("string");
    event.field("xcontext.installid").required("string");
    event.result()
}
"#
            .to_string(),
            enabled: true,
        })
        .await
        .expect("rhai rule should be created");

    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(root_rule.id)),
            },
        )
        .await
        .expect("rhai rule should become wildcard rule");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("rule set should be assigned");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rhai rules should compile");

    compiled
        .validate(
            "ignored",
            &json!({
                "appid": "app-1",
                "xcontext": {
                    "installid": "iid-1"
                }
            }),
        )
        .expect("rhai validation should pass");

    let error = compiled
        .validate("ignored", &json!({"appid": "app-1", "xcontext": {}}))
        .expect_err("missing installid should fail");
    assert_eq!(error.path(), Some("xcontext.installid"));
}

#[tokio::test]
async fn project_bound_rule_set_compiles_inherited_rule_for_xwhat() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: Some("Shared ingest rules".to_string()),
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Base".to_string(),
            xwhat: None,
            content: r#"
fields:
  appid:
    required: true
    type: string
  xcontext:
    required: true
    type: object
"#
            .to_string(),
            enabled: true,
        })
        .await
        .expect("base rule should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(base.id),
            name: "Install".to_string(),
            xwhat: Some("install".to_string()),
            content: r#"
fields:
  xcontext.installid:
    required: true
    type: string
"#
            .to_string(),
            enabled: true,
        })
        .await
        .expect("install rule should be created");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("rule set should be assigned");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rules should compile");

    compiled
        .validate(
            "install",
            &json!({
                "appid": "app-1",
                "xwhat": "install",
                "xcontext": {
                    "installid": "iid-1"
                }
            }),
        )
        .expect("inherited install rule should pass");

    let error = compiled
        .validate(
            "install",
            &json!({
                "appid": "app-1",
                "xwhat": "install",
                "xcontext": {}
            }),
        )
        .expect_err("missing inherited event field should fail");
    assert!(error.to_string().contains("xcontext.installid"));
}

#[tokio::test]
async fn duplicate_xwhat_is_rejected_inside_one_rule_set() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Install".to_string(),
            xwhat: Some("install".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("first rule should be created");

    let error = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Install Duplicate".to_string(),
            xwhat: Some("install".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect_err("duplicate xwhat should fail");

    assert!(error.to_string().contains("xwhat"));
}

#[tokio::test]
async fn nested_common_rule_does_not_create_duplicate_default_event() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Default".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("default rule should be created");
    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(base.id)),
            },
        )
        .await
        .expect("default rule should be selected as wildcard");

    let user_base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(base.id),
            name: "User common".to_string(),
            xwhat: None,
            content: r#"
fields:
  xwho:
    required: true
    type: string
"#
            .to_string(),
            enabled: true,
        })
        .await
        .expect("common rule should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(user_base.id),
            name: "Register".to_string(),
            xwhat: Some("register".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("event rule should be created");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("rule set should be assigned");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rules should compile without duplicate default");

    compiled
        .validate(
            "register",
            &json!({
                "appid": "app-1",
                "xwhat": "register",
                "xwho": "user-1",
                "xcontext": {}
            }),
        )
        .expect("event rule should inherit common rule");
}

#[tokio::test]
async fn disabled_common_rule_does_not_contribute_to_child_event() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Default".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("default rule should be created");
    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(base.id)),
            },
        )
        .await
        .expect("default rule should be selected as wildcard");

    let user_base = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(base.id),
            name: "User common".to_string(),
            xwhat: None,
            content: r#"
fields:
  xwho:
    required: true
    type: string
"#
            .to_string(),
            enabled: false,
        })
        .await
        .expect("disabled common rule should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(user_base.id),
            name: "Register".to_string(),
            xwhat: Some("register".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("event rule should be created");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("rule set should be assigned");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rules should compile");

    compiled
        .validate(
            "register",
            &json!({
                "appid": "app-1",
                "xwhat": "register",
                "xcontext": {}
            }),
        )
        .expect("disabled common rule should not require xwho");
}

#[tokio::test]
async fn common_rule_without_wildcard_flag_is_not_runtime_default() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Common".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("common rule should be created");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("rule set should be assigned");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rules should compile");

    assert!(!compiled.can_validate("unknown"));
}

#[tokio::test]
async fn wildcard_rule_must_not_have_xwhat() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let event_rule = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Install".to_string(),
            xwhat: Some("install".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("event rule should be created");

    let error = rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(event_rule.id)),
            },
        )
        .await
        .expect_err("event rule cannot be wildcard");

    assert!(error.to_string().contains("wildcard"));
}

#[tokio::test]
async fn rule_set_points_to_one_wildcard_rule() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let default_rule = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Default".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("wildcard rule should be created");
    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(default_rule.id)),
            },
        )
        .await
        .expect("wildcard rule should be selected by rule set");

    let updated_rule_set = rules
        .get_rule_set(rule_set.id)
        .await
        .expect("rule set lookup should succeed")
        .expect("rule set should exist");
    assert_eq!(updated_rule_set.wildcard_rule_id, Some(default_rule.id));

    let another_default = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Another default".to_string(),
            xwhat: None,
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("another common rule should be created");
    rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: None,
                description: None,
                enabled: None,
                wildcard_rule_id: Some(Some(another_default.id)),
            },
        )
        .await
        .expect("rule set should move wildcard pointer");

    let updated_rule_set = rules
        .get_rule_set(rule_set.id)
        .await
        .expect("rule set lookup should succeed")
        .expect("rule set should exist");
    assert_eq!(updated_rule_set.wildcard_rule_id, Some(another_default.id));
}

#[tokio::test]
async fn event_rule_cannot_be_used_as_parent_rule() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Default ingest".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let event_rule = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Register".to_string(),
            xwhat: Some("register".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("event rule should be created");

    let error = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: Some(event_rule.id),
            name: "Register child".to_string(),
            xwhat: Some("register_child".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect_err("event rule cannot be inherited");

    assert!(error.to_string().contains("xwhat=null"));
}

#[tokio::test]
async fn assigning_second_rule_set_replaces_project_rule_set() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let first_rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "First".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("first rule set should be created");
    let second_rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Second".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("second rule set should be created");

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: first_rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("first rule set should be assigned");
    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: second_rule_set.id,
                enabled: true,
            },
        )
        .await
        .expect("second rule set should replace first");

    let assignments = rules
        .list_project_rule_sets(project.id)
        .await
        .expect("project rule set should list");

    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].rule_set_id, second_rule_set.id);
}

#[tokio::test]
async fn rule_repository_lists_gets_updates_and_deletes_rule_sets_and_rules() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let rules = RuleRepository::new(db);

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "CRUD".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");

    let listed_rule_sets = rules.list_rule_sets().await.expect("rule sets should list");
    assert!(listed_rule_sets.iter().any(|item| item.id == rule_set.id));

    let fetched_rule_set = rules
        .get_rule_set(rule_set.id)
        .await
        .expect("rule set lookup should succeed")
        .expect("rule set should exist");
    assert_eq!(fetched_rule_set.name, "CRUD");

    let updated_rule_set = rules
        .update_rule_set(
            rule_set.id,
            UpdateRuleSetInput {
                name: Some("CRUD Updated".to_string()),
                description: Some("updated".to_string()),
                enabled: Some(false),
                wildcard_rule_id: None,
            },
        )
        .await
        .expect("rule set should update");
    assert_eq!(updated_rule_set.name, "CRUD Updated");
    assert!(!updated_rule_set.enabled);

    let rule = rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Install".to_string(),
            xwhat: Some("install".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("rule should be created");

    let listed_rules = rules
        .list_rules(rule_set.id)
        .await
        .expect("rules should list");
    assert_eq!(listed_rules.len(), 1);
    assert_eq!(listed_rules[0].id, rule.id);

    let fetched_rule = rules
        .get_rule(rule_set.id, rule.id)
        .await
        .expect("rule lookup should succeed")
        .expect("rule should exist");
    assert_eq!(fetched_rule.name, "Install");

    let updated_rule = rules
        .update_rule(
            rule_set.id,
            rule.id,
            UpdateRuleInput {
                parent_id: None,
                name: Some("Install Updated".to_string()),
                xwhat: Some(Some("install_updated".to_string())),
                content: Some(
                    "fields:\n  xcontext.installid:\n    required: true\n    type: string\n"
                        .to_string(),
                ),
                enabled: Some(false),
            },
        )
        .await
        .expect("rule should update");
    assert_eq!(updated_rule.name, "Install Updated");
    assert_eq!(updated_rule.xwhat.as_deref(), Some("install_updated"));
    assert!(!updated_rule.enabled);

    rules
        .delete_rule(rule_set.id, rule.id)
        .await
        .expect("rule should delete");
    assert!(rules
        .get_rule(rule_set.id, rule.id)
        .await
        .expect("rule lookup should succeed")
        .is_none());

    rules
        .delete_rule_set(rule_set.id)
        .await
        .expect("rule set should delete");
    assert!(rules
        .get_rule_set(rule_set.id)
        .await
        .expect("rule set lookup should succeed")
        .is_none());
}

#[tokio::test]
async fn project_rule_set_assignment_delete_and_disabled_assignment_are_respected() {
    let db = init_sqlite_database("sqlite::memory:")
        .await
        .expect("sqlite database should initialize");
    let projects = ProjectRepository::new(db.clone());
    let rules = RuleRepository::new(db);

    let project = projects
        .create_project(CreateProjectInput {
            name: "App 1".to_string(),
            enabled: true,
            ingest_token: "igx_app_1".to_string(),
        })
        .await
        .expect("project should be created");

    let rule_set = rules
        .create_rule_set(CreateRuleSetInput {
            name: "Assignment".to_string(),
            description: None,
            enabled: true,
        })
        .await
        .expect("rule set should be created");
    rules
        .create_rule(CreateRuleInput {
            rule_set_id: rule_set.id,
            parent_id: None,
            name: "Install".to_string(),
            xwhat: Some("install".to_string()),
            content: "fields: {}\n".to_string(),
            enabled: true,
        })
        .await
        .expect("rule should be created");

    assert!(rules
        .enabled_rule_exists_for_xwhat("install")
        .await
        .expect("xwhat lookup should succeed"));

    rules
        .assign_rule_set_to_project(
            project.id,
            CreateProjectRuleSetInput {
                rule_set_id: rule_set.id,
                enabled: false,
            },
        )
        .await
        .expect("disabled assignment should be created");

    let compiled = rules
        .compile_project_rules(project.id)
        .await
        .expect("project rules should compile");
    assert!(!compiled.can_validate("install"));

    rules
        .delete_project_rule_set(project.id, rule_set.id)
        .await
        .expect("assignment should delete");

    let assignments = rules
        .list_project_rule_sets(project.id)
        .await
        .expect("assignments should list");
    assert!(assignments.is_empty());

    let error = rules
        .delete_project_rule_set(project.id, rule_set.id)
        .await
        .expect_err("missing assignment should fail");
    assert!(error.to_string().contains("rule set"));
}
