export type DeliveryTargetType = string;
export type AutoOffsetReset = "latest" | "earliest";

export interface SinkTypeMetadata {
  target_type: DeliveryTargetType;
  label: string;
}

export interface DeliveryTarget {
  id: number;
  target_id: string;
  name: string;
  target_type: DeliveryTargetType;
  config_json: Record<string, unknown>;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface EventSink {
  id: number;
  sink_id: string;
  name: string;
  delivery_target_id: number;
  destination_json: Record<string, unknown>;
  auto_offset_reset: AutoOffsetReset;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface DeliveryTargetFormValues {
  target_id: string;
  name: string;
  target_type: DeliveryTargetType;
  config_json: string;
  enabled: boolean;
}

export interface EventSinkFormValues {
  sink_id: string;
  name: string;
  delivery_target_id: number | null;
  destination_json: string;
  auto_offset_reset: AutoOffsetReset;
  enabled: boolean;
}

export interface CreateDeliveryTargetPayload {
  target_id: string;
  name: string;
  target_type: DeliveryTargetType;
  config_json: Record<string, unknown>;
  enabled: boolean;
}

export interface UpdateDeliveryTargetPayload {
  name?: string;
  config_json?: Record<string, unknown>;
  enabled?: boolean;
}

export interface CreateEventSinkPayload {
  sink_id: string;
  name: string;
  delivery_target_id: number;
  destination_json: Record<string, unknown>;
  auto_offset_reset: AutoOffsetReset;
  enabled: boolean;
}

export interface UpdateEventSinkPayload {
  name?: string;
  delivery_target_id?: number;
  destination_json?: Record<string, unknown>;
  auto_offset_reset?: AutoOffsetReset;
  enabled?: boolean;
}
