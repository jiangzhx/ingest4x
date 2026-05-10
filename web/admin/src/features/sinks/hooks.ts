import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createDeliveryTarget,
  createEventSink,
  deleteDeliveryTarget,
  deleteEventSink,
  listDeliveryTargets,
  listEventSinks,
  listSinkTypes,
  updateDeliveryTarget,
  updateEventSink,
} from "./api";
import type {
  CreateDeliveryTargetPayload,
  CreateEventSinkPayload,
  UpdateDeliveryTargetPayload,
  UpdateEventSinkPayload,
  SinkTypeMetadata,
} from "./types";

export const deliveryTargetsQueryKey = ["admin", "delivery-targets"] as const;
export const eventSinksQueryKey = ["admin", "event-sinks"] as const;
export const sinkTypesQueryKey = ["admin", "sink-types"] as const;

export function useSinkTypesQuery() {
  return useQuery({
    queryKey: sinkTypesQueryKey,
    queryFn: listSinkTypes,
  });
}

export function useDeliveryTargetsQuery(sinkTypes: SinkTypeMetadata[]) {
  return useQuery({
    queryKey: [...deliveryTargetsQueryKey, sinkTypes.map((type) => type.target_type)],
    queryFn: () => listDeliveryTargets(sinkTypes),
    enabled: sinkTypes.length > 0,
  });
}

export function useEventSinksQuery() {
  return useQuery({
    queryKey: eventSinksQueryKey,
    queryFn: listEventSinks,
  });
}

export function useCreateDeliveryTargetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      payload,
      sinkTypes,
    }: {
      payload: CreateDeliveryTargetPayload;
      sinkTypes: SinkTypeMetadata[];
    }) => createDeliveryTarget(payload, sinkTypes),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: deliveryTargetsQueryKey });
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}

export function useUpdateDeliveryTargetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      id,
      payload,
      sinkTypes,
    }: {
      id: number;
      payload: UpdateDeliveryTargetPayload;
      sinkTypes: SinkTypeMetadata[];
    }) => updateDeliveryTarget(id, payload, sinkTypes),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: deliveryTargetsQueryKey });
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}

export function useDeleteDeliveryTargetMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: number) => deleteDeliveryTarget(id),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: deliveryTargetsQueryKey });
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}

export function useCreateEventSinkMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateEventSinkPayload) => createEventSink(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}

export function useUpdateEventSinkMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      id,
      payload,
    }: {
      id: number;
      payload: UpdateEventSinkPayload;
    }) => updateEventSink(id, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}

export function useDeleteEventSinkMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: number) => deleteEventSink(id),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: eventSinksQueryKey });
    },
  });
}
