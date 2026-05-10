export interface RuleSet {
  id: number;
  name: string;
  description: string | null;
  enabled: boolean;
  wildcard_rule_id: number | null;
  created_at: number;
  updated_at: number;
}

export interface Rule {
  id: number;
  rule_set_id: number;
  parent_id: number | null;
  name: string;
  xwhat: string | null;
  content: string;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface ProjectRuleSetAssignment {
  id: number;
  project_id: number;
  rule_set_id: number;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface RuleSetFormValues {
  name: string;
  description: string;
  enabled: boolean;
}

export interface RuleFormValues {
  parent_id: number | null;
  name: string;
  xwhat: string;
  content: string;
  enabled: boolean;
}

export interface CreateRuleSetPayload {
  name: string;
  description?: string | null;
  enabled: boolean;
}

export interface UpdateRuleSetPayload {
  name?: string;
  description?: string | null;
  enabled?: boolean;
  wildcard_rule_id?: number | null;
}

export interface CreateRulePayload {
  parent_id: number | null;
  name: string;
  xwhat?: string | null;
  content: string;
  enabled: boolean;
}

export interface UpdateRulePayload {
  parent_id?: number | null;
  name?: string;
  xwhat?: string | null;
  content?: string;
  enabled?: boolean;
}

export interface SaveValidationRulePayload {
  content: string;
  enabled?: boolean;
}

export interface AssignProjectRuleSetPayload {
  rule_set_id: number;
  enabled: boolean;
}
