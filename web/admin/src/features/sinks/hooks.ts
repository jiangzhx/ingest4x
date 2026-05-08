import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createDeliveryTarget,
  createEventSink,
  deleteDeliveryTarget,
  deleteEventSink,
  listDeliveryTargets,
  listEventSinks,
  updateDeliveryTarget,
  updateEventSink,
} from "./api";
import type {
  CreateDeliveryTargetPayload,
  CreateEventSinkPayload,
  UpdateDeliveryTargetPayload,
  UpdateEventSinkPayload,
} from "./types";

export const deliveryTargetsQueryKey = ["admin", "delivery-targets"] as const;
export const eventSinksQueryKey = ["admin", "event-sinks"] as const;

export function useDeliveryTargetsQuery() {
  return useQuery({
    queryKey: deliveryTargetsQueryKey,
    queryFn: listDeliveryTargets,
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
    mutationFn: (payload: CreateDeliveryTargetPayload) => createDeliveryTarget(payload),
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
    }: {
      id: number;
      payload: UpdateDeliveryTargetPayload;
    }) => updateDeliveryTarget(id, payload),
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
