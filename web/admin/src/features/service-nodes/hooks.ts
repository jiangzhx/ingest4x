import { useQuery } from "@tanstack/react-query";
import { listServiceNodes } from "./api";

export const serviceNodesQueryKey = ["admin", "service-nodes"] as const;

export function useServiceNodesQuery() {
  return useQuery({
    queryKey: serviceNodesQueryKey,
    queryFn: listServiceNodes,
  });
}
