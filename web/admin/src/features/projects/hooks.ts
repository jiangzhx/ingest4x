import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createProject,
  deleteProject,
  listProjects,
  updateProject,
} from "./api";
import type { CreateProjectPayload, UpdateProjectPayload } from "./types";

export const projectsQueryKey = ["admin", "projects"] as const;

export function useProjectsQuery() {
  return useQuery({
    queryKey: projectsQueryKey,
    queryFn: listProjects,
  });
}

export function useCreateProjectMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateProjectPayload) => createProject(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: projectsQueryKey });
    },
  });
}

export function useUpdateProjectMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      appid,
      payload,
    }: {
      appid: string;
      payload: UpdateProjectPayload;
    }) => updateProject(appid, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: projectsQueryKey });
    },
  });
}

export function useDeleteProjectMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (appid: string) => deleteProject(appid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: projectsQueryKey });
    },
  });
}
