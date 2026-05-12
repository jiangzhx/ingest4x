import { request, requestJson } from "../../shared/http";
import type {
  AssignProjectRuleSetPayload,
  CreateRulePayload,
  CreateRuleSetPayload,
  ProjectRuleSetAssignment,
  Rule,
  RuleSet,
  SaveValidationRulePayload,
  UpdateRulePayload,
  UpdateRuleSetPayload,
} from "./types";

type RuleSetResponse = {
  id?: unknown;
  name?: unknown;
  description?: unknown;
  enabled?: unknown;
  wildcard_rule_id?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

type RuleResponse = {
  id?: unknown;
  rule_set_id?: unknown;
  parent_id?: unknown;
  name?: unknown;
  xwhat?: unknown;
  content?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

type ProjectRuleSetAssignmentResponse = {
  id?: unknown;
  project_id?: unknown;
  rule_set_id?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

function invalidRulesData(message: string): Error {
  return new Error(`Invalid rule API response: ${message}`);
}

function normalizeRequiredString(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw invalidRulesData(`${fieldName} is missing or not a string`);
  }

  const normalized = value.trim();

  if (!normalized) {
    throw invalidRulesData(`${fieldName} cannot be empty`);
  }

  return normalized;
}

function normalizeRequiredContent(value: unknown): string {
  if (typeof value !== "string") {
    throw invalidRulesData("content is missing or not a string");
  }

  if (!value.trim()) {
    throw invalidRulesData("content cannot be empty");
  }

  return value;
}

function normalizeOptionalString(value: unknown, fieldName: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value !== "string") {
    throw invalidRulesData(`${fieldName} is not a string`);
  }

  const normalized = value.trim();
  return normalized || null;
}

function normalizeInteger(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw invalidRulesData(`${fieldName} is missing or not a valid integer`);
  }

  return value;
}

function normalizeNullableInteger(value: unknown, fieldName: string): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  return normalizeInteger(value, fieldName);
}

function normalizeTimestamp(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidRulesData(`${fieldName} is missing or not a valid timestamp`);
  }

  return Math.trunc(value);
}

function normalizeBoolean(value: unknown, fieldName: string): boolean {
  if (typeof value !== "boolean") {
    throw invalidRulesData(`${fieldName} is missing or not a boolean`);
  }

  return value;
}

export function normalizeRuleSetResponse(value: RuleSetResponse): RuleSet {
  if (!value || typeof value !== "object") {
    throw invalidRulesData("rule set data is not an object");
  }

  return {
    id: normalizeInteger(value.id, "id"),
    name: normalizeRequiredString(value.name, "name"),
    description: normalizeOptionalString(value.description, "description"),
    enabled: normalizeBoolean(value.enabled, "enabled"),
    wildcard_rule_id: normalizeNullableInteger(
      value.wildcard_rule_id,
      "wildcard_rule_id",
    ),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export function normalizeRuleSetsResponse(response: unknown): RuleSet[] {
  if (!Array.isArray(response)) {
    throw invalidRulesData("rule set list is not an array");
  }

  return response.map((ruleSet) => normalizeRuleSetResponse(ruleSet));
}

export function normalizeRuleResponse(value: RuleResponse): Rule {
  if (!value || typeof value !== "object") {
    throw invalidRulesData("rule data is not an object");
  }

  return {
    id: normalizeInteger(value.id, "id"),
    rule_set_id: normalizeInteger(value.rule_set_id, "rule_set_id"),
    parent_id: normalizeNullableInteger(value.parent_id, "parent_id"),
    name: normalizeRequiredString(value.name, "name"),
    xwhat: normalizeOptionalString(value.xwhat, "xwhat"),
    content: normalizeRequiredContent(value.content),
    enabled: normalizeBoolean(value.enabled, "enabled"),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export function normalizeRulesResponse(response: unknown): Rule[] {
  if (!Array.isArray(response)) {
    throw invalidRulesData("rule list is not an array");
  }

  return response.map((rule) => normalizeRuleResponse(rule));
}

export function normalizeProjectRuleSetAssignmentResponse(
  value: ProjectRuleSetAssignmentResponse,
): ProjectRuleSetAssignment {
  if (!value || typeof value !== "object") {
    throw invalidRulesData("project-rule-set assignment data is not an object");
  }

  return {
    id: normalizeInteger(value.id, "id"),
    project_id: normalizeInteger(value.project_id, "project_id"),
    rule_set_id: normalizeInteger(value.rule_set_id, "rule_set_id"),
    enabled: normalizeBoolean(value.enabled, "enabled"),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export function normalizeProjectRuleSetAssignmentsResponse(
  response: unknown,
): ProjectRuleSetAssignment[] {
  if (!Array.isArray(response)) {
    throw invalidRulesData("project-rule-set assignment list is not an array");
  }

  return response.map((assignment) =>
    normalizeProjectRuleSetAssignmentResponse(assignment),
  );
}

export async function listRuleSets(): Promise<RuleSet[]> {
  const response = await requestJson<RuleSetResponse[]>("/api/admin/rule-sets");
  return normalizeRuleSetsResponse(response);
}

export async function createRuleSet(
  payload: CreateRuleSetPayload,
): Promise<RuleSet> {
  const response = await requestJson<RuleSetResponse>("/api/admin/rule-sets", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  return normalizeRuleSetResponse(response);
}

export async function updateRuleSet(
  ruleSetId: number,
  payload: UpdateRuleSetPayload,
): Promise<RuleSet> {
  const response = await requestJson<RuleSetResponse>(
    `/api/admin/rule-sets/${ruleSetId}`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    },
  );
  return normalizeRuleSetResponse(response);
}

export async function deleteRuleSet(ruleSetId: number): Promise<void> {
  await request(`/api/admin/rule-sets/${ruleSetId}`, { method: "DELETE" });
}

export async function listRules(ruleSetId: number): Promise<Rule[]> {
  const response = await requestJson<RuleResponse[]>(
    `/api/admin/rule-sets/${ruleSetId}/rules`,
  );
  return normalizeRulesResponse(response);
}

export async function createRule(
  ruleSetId: number,
  payload: CreateRulePayload,
): Promise<Rule> {
  const response = await requestJson<RuleResponse>(
    `/api/admin/rule-sets/${ruleSetId}/rules`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    },
  );
  return normalizeRuleResponse(response);
}

export async function updateRule(
  ruleSetId: number,
  ruleId: number,
  payload: UpdateRulePayload,
): Promise<Rule> {
  const response = await requestJson<RuleResponse>(
    `/api/admin/rule-sets/${ruleSetId}/rules/${ruleId}`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    },
  );
  return normalizeRuleResponse(response);
}

export async function deleteRule(
  ruleSetId: number,
  ruleId: number,
): Promise<void> {
  await request(`/api/admin/rule-sets/${ruleSetId}/rules/${ruleId}`, {
    method: "DELETE",
  });
}

export async function saveValidationRule(
  ruleSetId: number,
  payload: SaveValidationRulePayload,
): Promise<Rule> {
  const response = await requestJson<RuleResponse>(
    `/api/admin/rule-sets/${ruleSetId}/validation-rule`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    },
  );
  return normalizeRuleResponse(response);
}

export async function listProjectRuleSetAssignments(
  projectId: number,
): Promise<ProjectRuleSetAssignment[]> {
  const response = await requestJson<ProjectRuleSetAssignmentResponse[]>(
    `/api/admin/projects/${projectId}/rule-sets`,
  );
  return normalizeProjectRuleSetAssignmentsResponse(response);
}

export async function assignProjectRuleSet(
  projectId: number,
  payload: AssignProjectRuleSetPayload,
): Promise<ProjectRuleSetAssignment> {
  const response = await requestJson<ProjectRuleSetAssignmentResponse>(
    `/api/admin/projects/${projectId}/rule-sets`,
    {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    },
  );
  return normalizeProjectRuleSetAssignmentResponse(response);
}

export async function deleteProjectRuleSetAssignment(
  projectId: number,
  ruleSetId: number,
): Promise<void> {
  await request(
    `/api/admin/projects/${projectId}/rule-sets/${ruleSetId}`,
    { method: "DELETE" },
  );
}
