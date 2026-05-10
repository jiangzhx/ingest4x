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
  saveValidationRule,
  updateRule,
  updateRuleSet,
} from "./api";
import type {
  AssignProjectRuleSetPayload,
  CreateRulePayload,
  CreateRuleSetPayload,
  SaveValidationRulePayload,
  UpdateRulePayload,
  UpdateRuleSetPayload,
} from "./types";

export const ruleSetsQueryKey = ["admin", "rule-sets"] as const;

export function rulesQueryKey(ruleSetId: number | null) {
  return ["admin", "rule-sets", ruleSetId, "rules"] as const;
}

export function projectRuleSetAssignmentsQueryKey(projectId: number | null) {
  return ["admin", "projects", projectId, "rule-sets"] as const;
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

export function useProjectRuleSetAssignmentsQuery(projectId: number | null) {
  return useQuery({
    queryKey: projectRuleSetAssignmentsQueryKey(projectId),
    queryFn: () => listProjectRuleSetAssignments(projectId ?? 0),
    enabled: projectId !== null,
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

export function useSaveValidationRuleMutation(ruleSetId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: SaveValidationRulePayload) =>
      saveValidationRule(ruleSetId ?? 0, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: rulesQueryKey(ruleSetId) }),
        queryClient.invalidateQueries({ queryKey: ruleSetsQueryKey }),
      ]);
    },
  });
}

export function useAssignProjectRuleSetMutation(projectId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: AssignProjectRuleSetPayload) =>
      assignProjectRuleSet(projectId ?? 0, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: projectRuleSetAssignmentsQueryKey(projectId),
      });
    },
  });
}

export function useDeleteProjectRuleSetAssignmentMutation(projectId: number | null) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ruleSetId: number) =>
      deleteProjectRuleSetAssignment(projectId ?? 0, ruleSetId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: projectRuleSetAssignmentsQueryKey(projectId),
      });
    },
  });
}
