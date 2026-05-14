import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteServiceNode, listServiceNodes } from "./api";

export const serviceNodesQueryKey = ["admin", "service-nodes"] as const;

export function useServiceNodesQuery() {
  return useQuery({
    queryKey: serviceNodesQueryKey,
    queryFn: listServiceNodes,
  });
}

export function useDeleteServiceNodeMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (nodeId: string) => deleteServiceNode(nodeId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: serviceNodesQueryKey });
    },
  });
}
