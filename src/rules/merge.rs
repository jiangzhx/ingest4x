use super::types::{FieldRule, RuleFragment};

pub(crate) fn merge_fragments(mut base: RuleFragment, child: RuleFragment) -> RuleFragment {
    for (path, rule) in child.fields {
        base.fields
            .entry(path)
            .and_modify(|existing| merge_field_rules(existing, &rule))
            .or_insert(rule);
    }

    base.extends = None;
    base
}

fn merge_field_rules(base: &mut FieldRule, child: &FieldRule) {
    base.required |= child.required;
    if child.field_type.is_some() {
        base.field_type = child.field_type;
    }
    if !child.enum_values.is_empty() {
        base.enum_values = child.enum_values.clone();
    }
    if child.gt.is_some() {
        base.gt = child.gt;
    }
    if child.gte.is_some() {
        base.gte = child.gte;
    }
    if child.lt.is_some() {
        base.lt = child.lt;
    }
    if child.lte.is_some() {
        base.lte = child.lte;
    }
    base.required_when.extend(child.required_when.clone());
    base.required_any_when
        .extend(child.required_any_when.clone());
}
