import type {
  CreateDeliveryTargetPayload,
  CreateEventSinkPayload,
  DeliveryTarget,
  DeliveryTargetFormValues,
  EventSink,
  EventSinkFormValues,
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

export function getDeliveryTargetTypeLabel(type: DeliveryTarget["target_type"]): string {
  return type === "kafka" ? "Kafka" : "stdout";
}

export function toCreateDeliveryTargetPayload(
  values: DeliveryTargetFormValues,
): CreateDeliveryTargetPayload {
  return {
    target_id: values.target_id.trim(),
    name: values.name.trim(),
    target_type: values.target_type,
    config_json: buildDeliveryTargetConfig(values),
    enabled: values.enabled,
  };
}

export function toUpdateDeliveryTargetPayload(
  values: DeliveryTargetFormValues,
): UpdateDeliveryTargetPayload {
  return {
    name: values.name.trim(),
    config_json: buildDeliveryTargetConfig(values),
    enabled: values.enabled,
  };
}

export function toCreateEventSinkPayload(
  values: EventSinkFormValues,
  targets: DeliveryTarget[],
): CreateEventSinkPayload {
  if (values.delivery_target_id === null) {
    throw new Error("请选择 delivery target");
  }

  return {
    sink_id: values.sink_id.trim(),
    name: values.name.trim(),
    delivery_target_id: values.delivery_target_id,
    destination_json: buildEventSinkDestination(values, targets),
    auto_offset_reset: values.auto_offset_reset,
    enabled: values.enabled,
  };
}

export function toUpdateEventSinkPayload(
  values: EventSinkFormValues,
  targets: DeliveryTarget[],
): UpdateEventSinkPayload {
  if (values.delivery_target_id === null) {
    throw new Error("请选择 delivery target");
  }

  return {
    name: values.name.trim(),
    delivery_target_id: values.delivery_target_id,
    destination_json: buildEventSinkDestination(values, targets),
    auto_offset_reset: values.auto_offset_reset,
    enabled: values.enabled,
  };
}

function buildDeliveryTargetConfig(
  values: DeliveryTargetFormValues,
): Record<string, unknown> {
  if (values.target_type === "stdout") {
    return parseJsonObject(values.config_json, "连接配置");
  }

  const config = parseJsonObject(values.config_json, "连接配置");
  return {
    ...config,
    bootstrap_servers: values.bootstrap_servers.trim(),
    delivery_timeout_ms: values.delivery_timeout_ms.trim(),
    queue_buffering_max_ms: values.queue_buffering_max_ms.trim(),
    batch_num_messages: values.batch_num_messages.trim(),
    queue_buffering_max_messages: values.queue_buffering_max_messages.trim(),
    linger_ms: values.linger_ms.trim(),
  };
}

function buildEventSinkDestination(
  values: EventSinkFormValues,
  targets: DeliveryTarget[],
): Record<string, unknown> {
  const target = targets.find((candidate) => candidate.id === values.delivery_target_id);
  const destination = parseJsonObject(values.destination_json, "投递目标配置");

  if (target?.target_type === "kafka") {
    return {
      ...destination,
      topic: values.topic.trim(),
    };
  }

  return destination;
}

export function deliveryTargetToFormValues(
  target?: DeliveryTarget | null,
): DeliveryTargetFormValues {
  const config = target?.config_json ?? {};

  return {
    target_id: target?.target_id ?? "",
    name: target?.name ?? "",
    target_type: target?.target_type ?? "kafka",
    bootstrap_servers:
      typeof config.bootstrap_servers === "string" ? config.bootstrap_servers : "",
    delivery_timeout_ms:
      typeof config.delivery_timeout_ms === "string" ? config.delivery_timeout_ms : "3000",
    queue_buffering_max_ms:
      typeof config.queue_buffering_max_ms === "string"
        ? config.queue_buffering_max_ms
        : "0",
    batch_num_messages:
      typeof config.batch_num_messages === "string" ? config.batch_num_messages : "100",
    queue_buffering_max_messages:
      typeof config.queue_buffering_max_messages === "string"
        ? config.queue_buffering_max_messages
        : "300",
    linger_ms: typeof config.linger_ms === "string" ? config.linger_ms : "100",
    config_json: stringifyJsonObject(stripKnownKafkaConfig(config)),
    enabled: target?.enabled ?? true,
  };
}

export function eventSinkToFormValues(sink?: EventSink | null): EventSinkFormValues {
  const destination = sink?.destination_json ?? {};

  return {
    sink_id: sink?.sink_id ?? "",
    name: sink?.name ?? "",
    delivery_target_id: sink?.delivery_target_id ?? null,
    topic: typeof destination.topic === "string" ? destination.topic : "",
    destination_json: stringifyJsonObject(stripKnownDestination(destination)),
    auto_offset_reset: sink?.auto_offset_reset ?? "latest",
    enabled: sink?.enabled ?? true,
  };
}

function stripKnownKafkaConfig(
  value: Record<string, unknown>,
): Record<string, unknown> {
  const {
    bootstrap_servers: _bootstrapServers,
    delivery_timeout_ms: _deliveryTimeoutMs,
    queue_buffering_max_ms: _queueBufferingMaxMs,
    batch_num_messages: _batchNumMessages,
    queue_buffering_max_messages: _queueBufferingMaxMessages,
    linger_ms: _lingerMs,
    ...rest
  } = value;

  return rest;
}

function stripKnownDestination(
  value: Record<string, unknown>,
): Record<string, unknown> {
  const { topic: _topic, ...rest } = value;

  return rest;
}
