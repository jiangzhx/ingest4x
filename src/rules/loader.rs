use super::merge::merge_fragments;
use super::types::{
    CompiledConditionalRequirement, CompiledConditionalRules, CompiledEventRule, CompiledFieldRule,
    FieldConditionalRequirement, FieldConstraints, FieldRule, FieldType, NumericConstraints,
    RuleFragment, Rules, StringConstraints,
};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn load_rules_from_dir(path: impl AsRef<Path>) -> Result<Rules> {
    let root = path.as_ref();
    let mut default_fragments: HashMap<PathBuf, RuleFragment> = HashMap::new();
    let mut event_fragments: HashMap<String, (PathBuf, RuleFragment)> = HashMap::new();

    for file_path in collect_rule_files(root)? {
        let fragment = load_fragment(&file_path)?;
        if fragment.extends.is_some() {
            bail!(
                "`extends` is not supported in directory rules mode: {}",
                file_path.display()
            );
        }

        let relative_path = file_path.strip_prefix(root).with_context(|| {
            format!(
                "failed to strip rules root prefix from {}",
                file_path.display()
            )
        })?;
        let relative_dir = relative_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .to_path_buf();
        let rule_name = file_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| anyhow!("invalid rule filename: {}", file_path.display()))?
            .to_string();

        if rule_name == "default" {
            default_fragments.insert(relative_dir, fragment);
            continue;
        }

        if let Some((existing_path, _)) = event_fragments.get(&rule_name) {
            bail!(
                "duplicate event `{rule_name}` found in `{}` and `{}`",
                existing_path.display(),
                relative_path.display()
            );
        }

        event_fragments.insert(rule_name, (relative_dir, fragment));
    }

    let mut events = HashMap::new();
    if let Some(root_default) = default_fragments.get(Path::new("")).cloned() {
        events.insert("default".to_string(), compile_event_rule(&root_default)?);
    }

    for (event_name, (relative_dir, fragment)) in event_fragments {
        let merged = merge_directory_chain(&default_fragments, &relative_dir, fragment);
        events.insert(event_name, compile_event_rule(&merged)?);
    }

    Ok(Rules { events, rhai: None })
}

fn collect_rule_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_rule_files_recursive(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_rule_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_rule_files_recursive(&path, files)?;
            continue;
        }

        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("yaml") {
            files.push(path);
        }
    }

    Ok(())
}

fn load_fragment(path: &Path) -> Result<RuleFragment> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str::<RuleFragment>(&content)
        .with_context(|| format!("failed to parse {}", path.display()))
}

fn merge_directory_chain(
    defaults: &HashMap<PathBuf, RuleFragment>,
    relative_dir: &Path,
    event_fragment: RuleFragment,
) -> RuleFragment {
    let mut merged = RuleFragment::default();

    for dir in ancestor_dirs(relative_dir) {
        if let Some(default_fragment) = defaults.get(&dir).cloned() {
            merged = merge_fragments(merged, default_fragment);
        }
    }

    merge_fragments(merged, event_fragment)
}

fn ancestor_dirs(relative_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::new()];
    if relative_dir.as_os_str().is_empty() {
        return dirs;
    }

    let mut current = PathBuf::new();
    for component in relative_dir.components() {
        current.push(component.as_os_str());
        dirs.push(current.clone());
    }
    dirs
}

pub(crate) fn compile_event_rule(fragment: &RuleFragment) -> Result<CompiledEventRule> {
    let mut fields = std::collections::BTreeMap::new();
    let mut rules = CompiledConditionalRules::default();

    for (path, rule) in &fragment.fields {
        let compiled = compile_field_rule(path, rule)?;
        rules.required_if.extend(
            rule.required_when
                .iter()
                .map(|condition| compile_conditional_requirement(path, condition)),
        );
        rules.required_any_if.extend(
            rule.required_any_when
                .iter()
                .map(|condition| compile_conditional_requirement(path, condition)),
        );
        fields.insert(path.clone(), compiled);
    }

    Ok(CompiledEventRule { fields, rules })
}

fn compile_conditional_requirement(
    path: &str,
    rule: &FieldConditionalRequirement,
) -> CompiledConditionalRequirement {
    CompiledConditionalRequirement {
        path: path.to_string(),
        equals: serde_json::to_value(&rule.equals).unwrap_or(Value::Null),
        fields: rule.fields.clone(),
    }
}

fn compile_field_rule(path: &str, rule: &FieldRule) -> Result<CompiledFieldRule> {
    let has_enum = !rule.enum_values.is_empty();
    let has_numeric_constraints =
        rule.gt.is_some() || rule.gte.is_some() || rule.lt.is_some() || rule.lte.is_some();

    let constraints = match rule.field_type {
        Some(FieldType::String) => {
            if has_numeric_constraints {
                bail!("field `{path}` is `string` but defines numeric constraints");
            }
            Some(FieldConstraints::String(StringConstraints {
                enum_values: has_enum.then(|| rule.enum_values.clone()),
            }))
        }
        Some(FieldType::Number) => {
            if has_enum {
                bail!("field `{path}` is `number` but defines string enum constraints");
            }
            Some(FieldConstraints::Number(NumericConstraints {
                gt: rule.gt,
                gte: rule.gte,
                lt: rule.lt,
                lte: rule.lte,
            }))
        }
        Some(FieldType::Integer) => {
            if has_enum {
                bail!("field `{path}` is `integer` but defines string enum constraints");
            }
            Some(FieldConstraints::Integer(NumericConstraints {
                gt: rule.gt,
                gte: rule.gte,
                lt: rule.lt,
                lte: rule.lte,
            }))
        }
        Some(FieldType::Boolean) => {
            if has_enum || has_numeric_constraints {
                bail!("field `{path}` is `boolean` but defines incompatible constraints");
            }
            Some(FieldConstraints::Boolean)
        }
        Some(FieldType::Object) => {
            if has_enum || has_numeric_constraints {
                bail!("field `{path}` is `object` but defines incompatible constraints");
            }
            Some(FieldConstraints::Object)
        }
        Some(FieldType::Array) => {
            if has_enum || has_numeric_constraints {
                bail!("field `{path}` is `array` but defines incompatible constraints");
            }
            Some(FieldConstraints::Array)
        }
        None => {
            if has_enum || has_numeric_constraints {
                bail!("field `{path}` defines typed constraints but has no `type`");
            }
            None
        }
    };

    Ok(CompiledFieldRule {
        required: rule.required,
        constraints,
    })
}
