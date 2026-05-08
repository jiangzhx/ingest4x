import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  assignProjectProcessor,
  createProcessorScript,
  deleteProjectProcessor,
  getProcessorScript,
  listProcessorScripts,
  listProjectProcessors,
  updateProcessorScript,
  updateProcessorScriptStatus,
  validateProcessorScript,
} from "./api";
import type {
  AssignProjectProcessorPayload,
  CreateProcessorScriptPayload,
  UpdateProcessorScriptPayload,
  UpdateProcessorScriptStatusPayload,
  ValidateProcessorScriptPayload,
} from "./types";

export const processorScriptsQueryKey = ["admin", "processor-scripts"] as const;
export const projectProcessorsQueryKey = ["admin", "project-processors"] as const;

export function useProcessorScriptsQuery() {
  return useQuery({
    queryKey: processorScriptsQueryKey,
    queryFn: listProcessorScripts,
  });
}

export function useProcessorScriptDetailQuery(id: number | null) {
  return useQuery({
    queryKey: [...processorScriptsQueryKey, id],
    queryFn: () => getProcessorScript(id ?? 0),
    enabled: id !== null,
  });
}

export function useProjectProcessorsQuery() {
  return useQuery({
    queryKey: projectProcessorsQueryKey,
    queryFn: listProjectProcessors,
  });
}

export function useCreateProcessorScriptMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateProcessorScriptPayload) =>
      createProcessorScript(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: processorScriptsQueryKey });
      await queryClient.invalidateQueries({ queryKey: projectProcessorsQueryKey });
    },
  });
}

export function useUpdateProcessorScriptMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      id,
      payload,
    }: {
      id: number;
      payload: UpdateProcessorScriptPayload;
    }) => updateProcessorScript(id, payload),
    onSuccess: async (_script, variables) => {
      await queryClient.invalidateQueries({ queryKey: processorScriptsQueryKey });
      await queryClient.invalidateQueries({
        queryKey: [...processorScriptsQueryKey, variables.id],
      });
      await queryClient.invalidateQueries({ queryKey: projectProcessorsQueryKey });
    },
  });
}

export function useValidateProcessorScriptMutation() {
  return useMutation({
    mutationFn: (payload: ValidateProcessorScriptPayload) =>
      validateProcessorScript(payload),
  });
}

export function useUpdateProcessorScriptStatusMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      id,
      payload,
    }: {
      id: number;
      payload: UpdateProcessorScriptStatusPayload;
    }) => updateProcessorScriptStatus(id, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: processorScriptsQueryKey });
      await queryClient.invalidateQueries({ queryKey: projectProcessorsQueryKey });
    },
  });
}

export function useAssignProjectProcessorMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      projectId,
      payload,
    }: {
      projectId: number;
      payload: AssignProjectProcessorPayload;
    }) => assignProjectProcessor(projectId, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: projectProcessorsQueryKey });
    },
  });
}

export function useDeleteProjectProcessorMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (projectId: number) => deleteProjectProcessor(projectId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: projectProcessorsQueryKey });
    },
  });
}
