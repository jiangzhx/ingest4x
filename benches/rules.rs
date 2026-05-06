use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use ingest4x::db::{init_sqlite_database, seed};
use ingest4x::ingest::processor::{ProcessorRequestContext, ProcessorState};
use ingest4x::repositories::{ProjectRepository, RuleRepository};
use ingest4x::rules::Rules;
use serde_json::{json, Value};
use tokio::runtime::Runtime;

const RULES_DIR: &str = "tests/fixtures/rules/ingest";
const RHAI_VALIDATE_SCRIPT: &str = r#"
fn process(event, request) {
    let validation = validate(event);
    if !validation["ok"] {
        emit("events_error", event);
    } else {
        emit("events", event);
    }
}
"#;
const RHAI_ACCEPT_SCRIPT: &str = r#"
fn process(event, request) {
    emit("events", event);
}
"#;

fn load_rules() -> Rules {
    Rules::load_from_dir(RULES_DIR).expect("fixture rules should load")
}

fn rhai_validator_processor() -> ProcessorState {
    ProcessorState::new(RHAI_VALIDATE_SCRIPT.to_string(), 10_000)
        .expect("rhai validator script should compile")
}

fn rhai_accept_processor() -> ProcessorState {
    ProcessorState::new(RHAI_ACCEPT_SCRIPT.to_string(), 10_000)
        .expect("rhai accept script should compile")
}

struct RepositoryBenchState {
    runtime: Runtime,
    rules: RuleRepository,
}

impl RepositoryBenchState {
    fn new() -> Self {
        let runtime = Runtime::new().expect("tokio runtime should initialize");
        let rules = runtime.block_on(async {
            let db = init_sqlite_database("sqlite::memory:")
                .await
                .expect("sqlite database should initialize");
            let projects = ProjectRepository::new(db.clone());
            let rules = RuleRepository::new(db);
            seed::run(&projects, &rules)
                .await
                .expect("seed rules should import");
            rules
        });

        Self { runtime, rules }
    }
}

fn valid_install_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "install",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1"
        }
    })
}

fn valid_payment_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "payment",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1",
            "transactionid": "txn-1",
            "paymenttype": "iap",
            "currencytype": "CNY",
            "currencyamount": 68.0,
            "paymentstatus": true
        }
    })
}

fn valid_levelup_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "levelup",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid-1",
            "os": "android",
            "oaid": "oaid-1",
            "level": 10
        }
    })
}

fn invalid_install_missing_id_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "install",
        "xcontext": {
            "os": "ios"
        }
    })
}

fn invalid_install_unknown_os_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "install",
        "xcontext": {
            "installid": "iid-1",
            "os": "symbian"
        }
    })
}

fn invalid_payment_currency_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "payment",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid-1",
            "os": "ios",
            "idfa": "idfa-1",
            "transactionid": "txn-1",
            "paymenttype": "iap",
            "currencytype": "XYZ",
            "currencyamount": 68.0,
            "paymentstatus": true
        }
    })
}

fn invalid_levelup_zero_payload() -> Value {
    json!({
        "appid": "APPID",
        "xwhat": "levelup",
        "xwho": "user-1",
        "xcontext": {
            "installid": "iid-1",
            "os": "android",
            "oaid": "oaid-1",
            "level": 0
        }
    })
}

fn rules_benchmark(c: &mut Criterion) {
    c.bench_function("rules/load_from_fixture_dir", |b| {
        b.iter(|| black_box(load_rules()))
    });

    let repository = RepositoryBenchState::new();
    c.bench_function("rules/compile_project_rules_from_sqlite", |b| {
        b.iter(|| {
            black_box(
                repository
                    .runtime
                    .block_on(
                        repository
                            .rules
                            .compile_project_rules(black_box("test_app")),
                    )
                    .expect("seeded project rules should compile"),
            )
        })
    });

    let rules = load_rules();
    let install = valid_install_payload();
    let payment = valid_payment_payload();
    let levelup = valid_levelup_payload();
    let invalid_install_missing_id = invalid_install_missing_id_payload();
    let invalid_install_unknown_os = invalid_install_unknown_os_payload();
    let invalid_payment_currency = invalid_payment_currency_payload();
    let invalid_levelup_zero = invalid_levelup_zero_payload();

    c.bench_function("rules/validate_install", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("install"), black_box(&install))
                    .expect("install payload should validate"),
            )
        })
    });

    c.bench_function("rules/validate_payment", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("payment"), black_box(&payment))
                    .expect("payment payload should validate"),
            )
        })
    });

    c.bench_function("rules/validate_levelup", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("levelup"), black_box(&levelup))
                    .expect("levelup payload should validate"),
            )
        })
    });

    c.bench_function("rules/validate_install_fail_missing_required", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("install"), black_box(&invalid_install_missing_id))
                    .expect_err("install payload should fail missing required field"),
            )
        })
    });

    c.bench_function("rules/validate_install_fail_enum", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("install"), black_box(&invalid_install_unknown_os))
                    .expect_err("install payload should fail unknown os"),
            )
        })
    });

    c.bench_function("rules/validate_payment_fail_enum", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("payment"), black_box(&invalid_payment_currency))
                    .expect_err("payment payload should fail unknown currency"),
            )
        })
    });

    c.bench_function("rules/validate_levelup_fail_number_constraint", |b| {
        b.iter(|| {
            black_box(
                rules
                    .validate(black_box("levelup"), black_box(&invalid_levelup_zero))
                    .expect_err("levelup payload should fail level constraint"),
            )
        })
    });

    let processor = rhai_validator_processor();
    let accept_processor = rhai_accept_processor();
    let request = ProcessorRequestContext::default();

    let mut overhead_group = c.benchmark_group("rules_vs_rhai_validator/overhead");
    overhead_group.bench_with_input(
        BenchmarkId::new("value_clone", "payment"),
        &payment,
        |b, payload| b.iter(|| black_box((*payload).clone())),
    );
    overhead_group.bench_function("rules_clone", |b| b.iter(|| black_box(rules.clone())));
    overhead_group.bench_with_input(
        BenchmarkId::new("rhai_accept_only", "payment"),
        &payment,
        |b, payload| {
            b.iter_batched(
                || ((*payload).clone(), rules.clone(), request.clone()),
                |(payload, rules, request)| {
                    let output = accept_processor
                        .process(black_box(payload), black_box(rules), black_box(request))
                        .expect("rhai accept should run");
                    black_box(output.deliveries)
                },
                BatchSize::SmallInput,
            )
        },
    );
    overhead_group.finish();

    let mut valid_group = c.benchmark_group("rules_vs_rhai_validator/valid");
    for (event_name, payload) in [
        ("install", &install),
        ("payment", &payment),
        ("levelup", &levelup),
    ] {
        valid_group.bench_with_input(
            BenchmarkId::new("rules", event_name),
            payload,
            |b, payload| {
                b.iter(|| {
                    black_box(
                        rules
                            .validate(black_box(event_name), black_box(payload))
                            .expect("payload should validate"),
                    )
                })
            },
        );
        valid_group.bench_with_input(
            BenchmarkId::new("rhai_validate_helper", event_name),
            payload,
            |b, payload| {
                b.iter_batched(
                    || ((*payload).clone(), rules.clone(), request.clone()),
                    |(payload, rules, request)| {
                        let output = processor
                            .process(black_box(payload), black_box(rules), black_box(request))
                            .expect("rhai validator should run");
                        black_box(output.deliveries)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    valid_group.finish();

    let mut invalid_group = c.benchmark_group("rules_vs_rhai_validator/invalid");
    for (event_name, payload, case_name) in [
        (
            "install",
            &invalid_install_missing_id,
            "install_missing_required",
        ),
        ("install", &invalid_install_unknown_os, "install_enum"),
        ("payment", &invalid_payment_currency, "payment_enum"),
        (
            "levelup",
            &invalid_levelup_zero,
            "levelup_number_constraint",
        ),
    ] {
        invalid_group.bench_with_input(
            BenchmarkId::new("rules", case_name),
            payload,
            |b, payload| {
                b.iter(|| {
                    black_box(
                        rules
                            .validate(black_box(event_name), black_box(payload))
                            .expect_err("payload should fail validation"),
                    )
                })
            },
        );
        invalid_group.bench_with_input(
            BenchmarkId::new("rhai_validate_helper", case_name),
            payload,
            |b, payload| {
                b.iter_batched(
                    || ((*payload).clone(), rules.clone(), request.clone()),
                    |(payload, rules, request)| {
                        let output = processor
                            .process(black_box(payload), black_box(rules), black_box(request))
                            .expect("rhai validator should run");
                        black_box(output.deliveries)
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    invalid_group.finish();
}

criterion_group!(benches, rules_benchmark);
criterion_main!(benches);
