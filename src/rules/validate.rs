use super::types::{
    CompiledConditionalRequirement, CompiledConditionalRules, CompiledFieldRule, FieldConstraints,
    FieldType, NumericConstraints, Rules, StringConstraints,
};
use anyhow::{anyhow, bail, Result};
use serde_json::Value;

pub(crate) fn validate_event(rules: &Rules, event_name: &str, payload: &Value) -> Result<()> {
    let Some(rule) = rules
        .events
        .get(event_name)
        .or_else(|| rules.events.get("default"))
    else {
        return Ok(());
    };

    for (path, field_rule) in &rule.fields {
        let value = lookup_path(payload, path);
        if field_rule.required && !is_present(value) {
            bail!("missing required field `{path}`");
        }

        if let Some(value) = value.filter(|value| !value.is_null()) {
            validate_field_type(path, field_rule, value)?;
            validate_enum(path, field_rule, value)?;
            validate_number_rules(path, field_rule, value)?;
        }
    }

    validate_conditional_rules(payload, &rule.rules)
}

fn validate_field_type(path: &str, field_rule: &CompiledFieldRule, value: &Value) -> Result<()> {
    let Some(field_type) = field_rule.field_type() else {
        return Ok(());
    };

    let valid = match field_type {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Object => value.is_object(),
        FieldType::Array => value.is_array(),
    };

    if valid {
        Ok(())
    } else {
        bail!("field `{path}` expected type `{field_type:?}`");
    }
}

fn validate_enum(path: &str, field_rule: &CompiledFieldRule, value: &Value) -> Result<()> {
    let Some(StringConstraints {
        enum_values: Some(enum_values),
    }) = (match field_rule.constraints.as_ref() {
        Some(FieldConstraints::String(constraints)) => Some(constraints),
        _ => None,
    })
    else {
        return Ok(());
    };

    let Some(actual) = value.as_str() else {
        bail!("field `{path}` must be a string to use enum");
    };

    if enum_values
        .iter()
        .any(|expected| expected.eq_ignore_ascii_case(actual))
    {
        Ok(())
    } else {
        bail!("field `{path}` must be one of [{}]", enum_values.join(", "))
    }
}

fn validate_number_rules(path: &str, field_rule: &CompiledFieldRule, value: &Value) -> Result<()> {
    let Some(NumericConstraints { gt, gte, lt, lte }) = (match field_rule.constraints.as_ref() {
        Some(FieldConstraints::Number(constraints)) => Some(constraints),
        Some(FieldConstraints::Integer(constraints)) => Some(constraints),
        _ => None,
    }) else {
        return Ok(());
    };

    let number = value
        .as_f64()
        .ok_or_else(|| anyhow!("field `{path}` could not be represented as f64"))?;

    if let Some(gt) = gt {
        if number <= *gt {
            bail!("field `{path}` must be greater than {gt}");
        }
    }
    if let Some(gte) = gte {
        if number < *gte {
            bail!("field `{path}` must be greater than or equal to {gte}");
        }
    }
    if let Some(lt) = lt {
        if number >= *lt {
            bail!("field `{path}` must be less than {lt}");
        }
    }
    if let Some(lte) = lte {
        if number > *lte {
            bail!("field `{path}` must be less than or equal to {lte}");
        }
    }

    Ok(())
}

fn validate_conditional_rules(payload: &Value, rules: &CompiledConditionalRules) -> Result<()> {
    for rule in &rules.required_if {
        if condition_matches(payload, rule) {
            for field in &rule.fields {
                if !is_present(lookup_path(payload, field)) {
                    bail!("missing required field `{field}`");
                }
            }
        }
    }

    for rule in &rules.required_any_if {
        if condition_matches(payload, rule)
            && !rule
                .fields
                .iter()
                .any(|field| is_present(lookup_path(payload, field)))
        {
            bail!("at least one field is required: {}", rule.fields.join(", "));
        }
    }

    Ok(())
}

fn condition_matches(payload: &Value, rule: &CompiledConditionalRequirement) -> bool {
    let Some(actual) = lookup_path(payload, &rule.path) else {
        return false;
    };

    match (actual, &rule.equals) {
        (Value::String(actual), Value::String(expected)) => actual.eq_ignore_ascii_case(expected),
        _ => actual == &rule.equals,
    }
}

fn lookup_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = payload;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn is_present(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Null) | None => false,
        Some(Value::String(text)) => !text.is_empty(),
        Some(_) => true,
    }
}
