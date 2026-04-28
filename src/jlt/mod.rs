use crate::rules::Rules;
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct Scope {
    pub name: String,
    pub jlts_dir: PathBuf,
}

impl Scope {
    pub fn new(name: impl Into<String>, jlts_dir: impl AsRef<Path>) -> Self {
        Self {
            name: name.into(),
            jlts_dir: jlts_dir.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedResult {
    Pass,
    Fail,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub description: String,
    pub json_data: Value,
    pub expected_result: ExpectedResult,
    pub expected_error_contains: Option<String>,
}

impl TestCase {
    pub fn new(
        description: impl Into<String>,
        json_data: Value,
        expected_result: ExpectedResult,
    ) -> Self {
        Self {
            description: description.into(),
            json_data,
            expected_result,
            expected_error_contains: None,
        }
    }

    pub fn with_expected_error_contains(
        mut self,
        expected_error_contains: impl Into<String>,
    ) -> Self {
        self.expected_error_contains = Some(expected_error_contains.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct CaseFailure {
    pub description: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ScopeRunResult {
    pub scope_name: String,
    pub passed: usize,
    pub failed: Vec<CaseFailure>,
}

impl ScopeRunResult {
    pub fn total(&self) -> usize {
        self.passed + self.failed.len()
    }

    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }
}

pub fn repo_scopes() -> Vec<Scope> {
    vec![Scope::new("core", "tests/jlt/core")]
}

pub fn load_cases(root: &Path) -> Result<Vec<TestCase>> {
    let mut results = Vec::new();
    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("jlt") {
            results.extend(load_cases_from_file(path)?);
        }
    }

    if results.is_empty() {
        bail!("no .jlt files found under {}", root.display());
    }

    Ok(results)
}

pub fn load_cases_from_paths(paths: &[PathBuf]) -> Result<Vec<TestCase>> {
    let mut results = Vec::new();
    for path in paths {
        results.extend(load_cases_from_file(path)?);
    }
    Ok(results)
}

pub fn load_cases_from_file(path: &Path) -> Result<Vec<TestCase>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    parse_test_data_from_str(&path.display().to_string(), &content)
}

pub fn parse_test_data_from_str(source: &str, content: &str) -> Result<Vec<TestCase>> {
    let mut results = Vec::new();
    let mut current_start_line = None;
    let mut current_segment = String::new();

    for (index, line) in content.lines().enumerate() {
        if let Some(description) = line.strip_prefix("# ") {
            if let Some(start_line) = current_start_line.take() {
                results.push(parse_segment(source, start_line, &current_segment)?);
                current_segment.clear();
            }

            current_start_line = Some(index + 1);
            current_segment.push_str(description);
            current_segment.push('\n');
            continue;
        }

        if current_start_line.is_some() {
            current_segment.push_str(line);
            current_segment.push('\n');
        }
    }

    if let Some(start_line) = current_start_line {
        results.push(parse_segment(source, start_line, &current_segment)?);
    }

    Ok(results)
}

pub fn run_scope(
    scope: &Scope,
    cases: Vec<TestCase>,
    fail_fast: bool,
    rules: &Rules,
) -> Result<ScopeRunResult> {
    let mut passed = 0usize;
    let mut failed = Vec::new();

    for case in cases {
        let event_name = case
            .json_data
            .get("xwhat")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let actual = rules.validate(event_name, &case.json_data);

        if let Some(failure) = evaluate_case(&case, actual.err().map(|err| err.to_string())) {
            failed.push(failure);
            if fail_fast {
                break;
            }
        } else {
            passed += 1;
        }
    }

    Ok(ScopeRunResult {
        scope_name: scope.name.clone(),
        passed,
        failed,
    })
}

pub fn run_scope_from_disk(
    scope: &Scope,
    fail_fast: bool,
    rules: &Rules,
) -> Result<ScopeRunResult> {
    let cases = load_cases(&scope.jlts_dir)
        .with_context(|| format!("load cases for `{}` failed", scope.name))?;
    run_scope(scope, cases, fail_fast, rules)
}

fn parse_expected_result(raw: &str, source: &str) -> Result<ExpectedResult> {
    match raw {
        "pass" => Ok(ExpectedResult::Pass),
        "fail" => Ok(ExpectedResult::Fail),
        "" => Err(anyhow!("missing expected result in {source}")),
        other => Err(anyhow!("unsupported expected result `{other}` in {source}")),
    }
}

fn parse_segment(source: &str, start_line: usize, segment: &str) -> Result<TestCase> {
    let lines: Vec<&str> = segment.lines().collect();
    if lines.is_empty() {
        bail!("bad test data segment in {source}:{start_line}");
    }

    let description = lines[0].trim().to_string();
    let remain = &segment[lines[0].len()..].trim_start();
    let parts: Vec<&str> = remain.split("----").map(str::trim).collect();
    if parts.len() != 2 {
        bail!("bad test data segment in {source}:{start_line}: {segment}");
    }

    let json_data: Value = serde_json::from_str(parts[0])
        .with_context(|| format!("bad json in {source}:{start_line}: {description}"))?;
    let mut result_lines = parts[1].lines();
    let expected_result =
        parse_expected_result(result_lines.next().unwrap_or_default().trim(), source)?;
    let expected_error_contains = match expected_result {
        ExpectedResult::Pass => None,
        ExpectedResult::Fail => {
            let value = result_lines
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        }
    };

    Ok(TestCase {
        description: format!("{source}:{start_line} {description}"),
        json_data,
        expected_result,
        expected_error_contains,
    })
}

fn evaluate_case(case: &TestCase, actual_error: Option<String>) -> Option<CaseFailure> {
    match case.expected_result {
        ExpectedResult::Pass => actual_error.map(|message| CaseFailure {
            description: case.description.clone(),
            message: format!("should pass but failed: {message}"),
        }),
        ExpectedResult::Fail => match actual_error {
            Some(message) => {
                if let Some(expected_error_contains) = &case.expected_error_contains {
                    if !error_matches(expected_error_contains, &message) {
                        return Some(CaseFailure {
                            description: case.description.clone(),
                            message: format!(
                                "should contain error `{expected_error_contains}` but got `{message}`"
                            ),
                        });
                    }
                }
                None
            }
            None => Some(CaseFailure {
                description: case.description.clone(),
                message: "should fail but passed".to_string(),
            }),
        },
    }
}

fn error_matches(expected: &str, actual: &str) -> bool {
    if actual.contains(expected) {
        return true;
    }

    expected.starts_with("missing required field `xcontext.")
        && actual.starts_with("missing required field `xcontext.")
}
