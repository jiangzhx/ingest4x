import type {
  CreateRulePayload,
  CreateRuleSetPayload,
  Rule,
  RuleFormValues,
  RuleSetFormValues,
  UpdateRulePayload,
  UpdateRuleSetPayload,
} from "./types";

const timeFormatter = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

export function formatRuleTimestamp(timestamp: number): string {
  try {
    return timeFormatter.format(new Date(timestamp));
  } catch {
    return "-";
  }
}

export function getErrorMessage(
  error: unknown,
  fallback = "Request failed, please try again later.",
): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}

export function toCreateRuleSetPayload(
  values: RuleSetFormValues,
): CreateRuleSetPayload {
  return {
    name: values.name.trim(),
    description: values.description.trim() || null,
    enabled: values.enabled,
  };
}

export function toUpdateRuleSetPayload(
  values: RuleSetFormValues,
): UpdateRuleSetPayload {
  return toCreateRuleSetPayload(values);
}

function normalizeXwhat(value: string): string | null {
  const normalized = value.trim();
  return normalized || null;
}

export function toCreateRulePayload(values: RuleFormValues): CreateRulePayload {
  return {
    parent_id: values.parent_id,
    name: values.name.trim(),
    xwhat: normalizeXwhat(values.xwhat),
    content: values.content.trim(),
    enabled: values.enabled,
  };
}

export function toUpdateRulePayload(values: RuleFormValues): UpdateRulePayload {
  return toCreateRulePayload(values);
}

export function ruleParentOptions(rules: Rule[], editingRuleId?: number | null) {
  return rules
    .filter((rule) => rule.id !== editingRuleId && !rule.xwhat)
    .map((rule) => ({
      label: rule.name,
      value: rule.id,
    }));
}
