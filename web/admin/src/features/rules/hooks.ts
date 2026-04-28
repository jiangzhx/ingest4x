import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  assignProjectRuleSet,
  createRule,
  createRuleSet,
  deleteProjectRuleSetAssignment,
  deleteRule,
  deleteRuleSet,
  listProjectRuleSetAssignments,
  listRules,
  listRuleSets,
  updateRule,
  updateRuleSet,
} from "./api";
import type {
  AssignProjectRuleSetPayload,
  CreateRulePayload,
  CreateRuleSetPayload,
  UpdateRulePayload,
  UpdateRuleSetPayload,
} from "./types";

export const ruleSetsQueryKey = ["admin", "rule-sets"] as const;

export function rulesQueryKey(ruleSetId: number | null) {
  return ["admin", "rule-sets", ruleSetId, "rules"] as const;
}

export function projectRuleSetAssignmentsQueryKey(appid: string | null) {
  return ["admin", "projects", appid, "rule-sets"] as const;
}

export function useRuleSetsQuery() {
  return useQuery({
    queryKey: ruleSetsQueryKey,
    queryFn: listRuleSets,
  });
}

export function useRulesQuery(ruleSetId: number | null) {
  return useQuery({
    queryKey: rulesQueryKey(ruleSetId),
    queryFn: () => listRules(ruleSetId ?? 0),
    enabled: ruleSetId !== null,
  });
}

export function useProjectRuleSetAssignmentsQuery(appid: string | null) {
  return useQuery({
    queryKey: projectRuleSetAssignmentsQueryKey(appid),
    queryFn: () => listProjectRuleSetAssignments(appid ?? ""),
    enabled: Boolean(appid),
  });
}

export function useCreateRuleSetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateRuleSetPayload) => createRuleSet(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ruleSetsQueryKey });
    },
  });
}

export function useUpdateRuleSetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      ruleSetId,
      payload,
    }: {
      ruleSetId: number;
      payload: UpdateRuleSetPayload;
    }) => updateRuleSet(ruleSetId, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ruleSetsQueryKey });
    },
  });
}

export function useDeleteRuleSetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ruleSetId: number) => deleteRuleSet(ruleSetId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ruleSetsQueryKey });
    },
  });
}

export function useCreateRuleMutation(ruleSetId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateRulePayload) => createRule(ruleSetId ?? 0, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: rulesQueryKey(ruleSetId) });
    },
  });
}

export function useUpdateRuleMutation(ruleSetId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      ruleId,
      payload,
    }: {
      ruleId: number;
      payload: UpdateRulePayload;
    }) => updateRule(ruleSetId ?? 0, ruleId, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: rulesQueryKey(ruleSetId) });
    },
  });
}

export function useDeleteRuleMutation(ruleSetId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ruleId: number) => deleteRule(ruleSetId ?? 0, ruleId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: rulesQueryKey(ruleSetId) }),
        queryClient.invalidateQueries({ queryKey: ruleSetsQueryKey }),
      ]);
    },
  });
}

export function useAssignProjectRuleSetMutation(appid: string | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: AssignProjectRuleSetPayload) =>
      assignProjectRuleSet(appid ?? "", payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: projectRuleSetAssignmentsQueryKey(appid),
      });
    },
  });
}

export function useDeleteProjectRuleSetAssignmentMutation(appid: string | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ruleSetId: number) =>
      deleteProjectRuleSetAssignment(appid ?? "", ruleSetId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: projectRuleSetAssignmentsQueryKey(appid),
      });
    },
  });
}
