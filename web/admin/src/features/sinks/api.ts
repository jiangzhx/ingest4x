import { request, requestJson } from "../../shared/http";
import type {
  AutoOffsetReset,
  CreateDeliveryTargetPayload,
  CreateEventSinkPayload,
  DeliveryTarget,
  DeliveryTargetType,
  EventSink,
  UpdateDeliveryTargetPayload,
  UpdateEventSinkPayload,
} from "./types";

type DeliveryTargetResponse = {
  id?: unknown;
  target_id?: unknown;
  name?: unknown;
  target_type?: unknown;
  config_json?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

type EventSinkResponse = {
  id?: unknown;
  sink_id?: unknown;
  name?: unknown;
  delivery_target_id?: unknown;
  destination_json?: unknown;
  auto_offset_reset?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

function invalidSinkData(message: string): Error {
  return new Error(`Sink 接口响应无效：${message}`);
}

function normalizePositiveInteger(value: unknown, fieldName: string): number {
  if (!Number.isInteger(value) || typeof value !== "number" || value <= 0) {
    throw invalidSinkData(`${fieldName} 缺失或不是有效整数`);
  }

  return value;
}

function normalizeRequiredString(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw invalidSinkData(`${fieldName} 缺失或不是字符串`);
  }

  const normalized = value.trim();

  if (!normalized) {
    throw invalidSinkData(`${fieldName} 不能为空`);
  }

  return normalized;
}

function normalizeTimestamp(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidSinkData(`${fieldName} 缺失或不是有效时间戳`);
  }

  return Math.trunc(value);
}

function normalizeObject(value: unknown, fieldName: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw invalidSinkData(`${fieldName} 缺失或不是对象`);
  }

  return value as Record<string, unknown>;
}

function normalizeDeliveryTargetType(value: unknown): DeliveryTargetType {
  if (value !== "kafka" && value !== "stdout") {
    throw invalidSinkData("target_type 不是支持的类型");
  }

  return value;
}

function normalizeAutoOffsetReset(value: unknown): AutoOffsetReset {
  if (value !== "latest" && value !== "earliest") {
    throw invalidSinkData("auto_offset_reset 不是支持的值");
  }

  return value;
}

export function normalizeDeliveryTargetResponse(
  value: DeliveryTargetResponse,
): DeliveryTarget {
  if (!value || typeof value !== "object") {
    throw invalidSinkData("delivery target 数据不是对象");
  }

  if (typeof value.enabled !== "boolean") {
    throw invalidSinkData("enabled 缺失或不是布尔值");
  }

  return {
    id: normalizePositiveInteger(value.id, "id"),
    target_id: normalizeRequiredString(value.target_id, "target_id"),
    name: normalizeRequiredString(value.name, "name"),
    target_type: normalizeDeliveryTargetType(value.target_type),
    config_json: normalizeObject(value.config_json, "config_json"),
    enabled: value.enabled,
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export function normalizeDeliveryTargetsResponse(response: unknown): DeliveryTarget[] {
  if (!Array.isArray(response)) {
    throw invalidSinkData("delivery target 列表不是数组");
  }

  return response.map((target) => normalizeDeliveryTargetResponse(target));
}

export function normalizeEventSinkResponse(value: EventSinkResponse): EventSink {
  if (!value || typeof value !== "object") {
    throw invalidSinkData("event sink 数据不是对象");
  }

  if (typeof value.enabled !== "boolean") {
    throw invalidSinkData("enabled 缺失或不是布尔值");
  }

  return {
    id: normalizePositiveInteger(value.id, "id"),
    sink_id: normalizeRequiredString(value.sink_id, "sink_id"),
    name: normalizeRequiredString(value.name, "name"),
    delivery_target_id: normalizePositiveInteger(
      value.delivery_target_id,
      "delivery_target_id",
    ),
    destination_json: normalizeObject(value.destination_json, "destination_json"),
    auto_offset_reset: normalizeAutoOffsetReset(value.auto_offset_reset),
    enabled: value.enabled,
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export function normalizeEventSinksResponse(response: unknown): EventSink[] {
  if (!Array.isArray(response)) {
    throw invalidSinkData("event sink 列表不是数组");
  }

  return response.map((sink) => normalizeEventSinkResponse(sink));
}

export async function listDeliveryTargets(): Promise<DeliveryTarget[]> {
  const response = await requestJson<DeliveryTargetResponse[]>(
    "/api/admin/delivery-targets",
  );

  return normalizeDeliveryTargetsResponse(response);
}

export async function createDeliveryTarget(
  payload: CreateDeliveryTargetPayload,
): Promise<DeliveryTarget> {
  const response = await requestJson<DeliveryTargetResponse>(
    "/api/admin/delivery-targets",
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeDeliveryTargetResponse(response);
}

export async function updateDeliveryTarget(
  id: number,
  payload: UpdateDeliveryTargetPayload,
): Promise<DeliveryTarget> {
  const response = await requestJson<DeliveryTargetResponse>(
    `/api/admin/delivery-targets/${id}`,
    {
      method: "PUT",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeDeliveryTargetResponse(response);
}

export async function deleteDeliveryTarget(id: number): Promise<void> {
  await request(`/api/admin/delivery-targets/${id}`, {
    method: "DELETE",
  });
}

export async function listEventSinks(): Promise<EventSink[]> {
  const response = await requestJson<EventSinkResponse[]>("/api/admin/event-sinks");

  return normalizeEventSinksResponse(response);
}

export async function createEventSink(
  payload: CreateEventSinkPayload,
): Promise<EventSink> {
  const response = await requestJson<EventSinkResponse>("/api/admin/event-sinks", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return normalizeEventSinkResponse(response);
}

export async function updateEventSink(
  id: number,
  payload: UpdateEventSinkPayload,
): Promise<EventSink> {
  const response = await requestJson<EventSinkResponse>(
    `/api/admin/event-sinks/${id}`,
    {
      method: "PUT",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeEventSinkResponse(response);
}

export async function deleteEventSink(id: number): Promise<void> {
  await request(`/api/admin/event-sinks/${id}`, {
    method: "DELETE",
  });
}
