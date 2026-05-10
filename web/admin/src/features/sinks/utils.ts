import type {
  CreateDeliveryTargetPayload,
  CreateEventSinkPayload,
  DeliveryTarget,
  DeliveryTargetFormValues,
  EventSink,
  EventSinkFormValues,
  SinkTypeMetadata,
  UpdateDeliveryTargetPayload,
  UpdateEventSinkPayload,
} from "./types";

const timeFormatter = new Intl.DateTimeFormat("zh-CN", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

export function formatSinkTimestamp(timestamp: number): string {
  try {
    return timeFormatter.format(new Date(timestamp));
  } catch {
    return "-";
  }
}

export function getErrorMessage(
  error: unknown,
  fallback = "请求失败，请稍后重试。",
): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}

export function parseJsonObject(value: string, fieldName: string): Record<string, unknown> {
  const trimmed = value.trim();

  if (!trimmed) {
    return {};
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error(`${fieldName} 必须是合法 JSON`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${fieldName} 必须是 JSON 对象`);
  }

  return parsed as Record<string, unknown>;
}

export function stringifyJsonObject(value: Record<string, unknown>): string {
  if (Object.keys(value).length === 0) {
    return "{}";
  }

  return JSON.stringify(value, null, 2);
}

export function getDeliveryTargetTypeLabel(
  type: DeliveryTarget["target_type"],
  sinkTypes: SinkTypeMetadata[] = [],
): string {
  return sinkTypes.find((sinkType) => sinkType.target_type === type)?.label ?? type;
}

export function toCreateDeliveryTargetPayload(
  values: DeliveryTargetFormValues,
): CreateDeliveryTargetPayload {
  return {
    target_id: values.target_id.trim(),
    name: values.name.trim(),
    target_type: values.target_type,
    config_json: parseJsonObject(values.config_json, "连接配置"),
    enabled: values.enabled,
  };
}

export function toUpdateDeliveryTargetPayload(
  values: DeliveryTargetFormValues,
): UpdateDeliveryTargetPayload {
  return {
    name: values.name.trim(),
    config_json: parseJsonObject(values.config_json, "连接配置"),
    enabled: values.enabled,
  };
}

export function toCreateEventSinkPayload(
  values: EventSinkFormValues,
): CreateEventSinkPayload {
  if (values.delivery_target_id === null) {
    throw new Error("请选择 delivery target");
  }

  return {
    sink_id: values.sink_id.trim(),
    name: values.name.trim(),
    delivery_target_id: values.delivery_target_id,
    destination_json: parseJsonObject(values.destination_json, "投递目标配置"),
    auto_offset_reset: values.auto_offset_reset,
    enabled: values.enabled,
  };
}

export function toUpdateEventSinkPayload(
  values: EventSinkFormValues,
): UpdateEventSinkPayload {
  if (values.delivery_target_id === null) {
    throw new Error("请选择 delivery target");
  }

  return {
    name: values.name.trim(),
    delivery_target_id: values.delivery_target_id,
    destination_json: parseJsonObject(values.destination_json, "投递目标配置"),
    auto_offset_reset: values.auto_offset_reset,
    enabled: values.enabled,
  };
}

export function deliveryTargetToFormValues(
  target?: DeliveryTarget | null,
): DeliveryTargetFormValues {
  const config = target?.config_json ?? {};

  return {
    target_id: target?.target_id ?? "",
    name: target?.name ?? "",
    target_type: target?.target_type ?? "kafka",
    config_json: stringifyJsonObject(config),
    enabled: target?.enabled ?? true,
  };
}

export function eventSinkToFormValues(sink?: EventSink | null): EventSinkFormValues {
  const destination = sink?.destination_json ?? {};

  return {
    sink_id: sink?.sink_id ?? "",
    name: sink?.name ?? "",
    delivery_target_id: sink?.delivery_target_id ?? null,
    destination_json: stringifyJsonObject(destination),
    auto_offset_reset: sink?.auto_offset_reset ?? "latest",
    enabled: sink?.enabled ?? true,
  };
}
