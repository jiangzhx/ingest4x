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

#[cfg(test)]
mod tests {
    use super::merge_fragments;
    use crate::rules::types::{FieldType, RuleFragment};

    #[test]
    fn child_fragment_overrides_constraints_and_clears_extends() {
        let base: RuleFragment = serde_yaml::from_str(
            r#"
extends: base
fields:
  xcontext.level:
    required: false
    type: number
    gt: 0
  appid:
    required: true
    type: string
"#,
        )
        .expect("base fragment");
        let child: RuleFragment = serde_yaml::from_str(
            r#"
extends: ignored
fields:
  xcontext.level:
    required: true
    type: integer
    gte: 1
    lte: 99
  xcontext.channel:
    type: string
    enum: ["ios", "android"]
"#,
        )
        .expect("child fragment");

        let merged = merge_fragments(base, child);

        assert!(merged.extends.is_none());

        let level = merged
            .fields
            .get("xcontext.level")
            .expect("level field should remain");
        assert!(level.required);
        assert_eq!(level.field_type, Some(FieldType::Integer));
        assert_eq!(level.gt, Some(0.0));
        assert_eq!(level.gte, Some(1.0));
        assert_eq!(level.lte, Some(99.0));

        let channel = merged
            .fields
            .get("xcontext.channel")
            .expect("child-only field should be inserted");
        assert_eq!(channel.enum_values, vec!["ios", "android"]);
        assert!(merged.fields.contains_key("appid"));
    }

    #[test]
    fn child_fragment_appends_conditional_requirements() {
        let base: RuleFragment = serde_yaml::from_str(
            r#"
fields:
  xcontext:
    required_when:
      - equals:
          xwhat: install
        fields: ["xcontext.installid"]
"#,
        )
        .expect("base fragment");
        let child: RuleFragment = serde_yaml::from_str(
            r#"
fields:
  xcontext:
    required_when:
      - equals:
          xwhat: payment
        fields: ["xcontext.orderid"]
    required_any_when:
      - equals:
          xwhat: register
        fields: ["xwho", "xcontext.openid"]
"#,
        )
        .expect("child fragment");

        let merged = merge_fragments(base, child);
        let xcontext = merged.fields.get("xcontext").expect("xcontext field");

        assert_eq!(xcontext.required_when.len(), 2);
        assert_eq!(
            xcontext.required_when[0].fields,
            vec!["xcontext.installid".to_string()]
        );
        assert_eq!(
            xcontext.required_when[1].fields,
            vec!["xcontext.orderid".to_string()]
        );
        assert_eq!(xcontext.required_any_when.len(), 1);
        assert_eq!(
            xcontext.required_any_when[0].fields,
            vec!["xwho".to_string(), "xcontext.openid".to_string()]
        );
    }
}
