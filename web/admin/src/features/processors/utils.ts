import type {
  CreateProcessorScriptPayload,
  ProcessorScriptDetail,
  ProcessorScriptFormValues,
  UpdateProcessorScriptPayload,
  ValidateProcessorScriptPayload,
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

export const DEFAULT_PROCESSOR_SOURCE = `fn process(event, request) {
    let validation = validate(event);
    if validation["ok"] {
        emit(SINK_EVENTS, event);
    } else {
        emit(SINK_EVENTS_ERROR, event);
    }
}`;

export function formatProcessorTimestamp(timestamp: number | null): string {
  if (timestamp === null) {
    return "-";
  }

  try {
    return timeFormatter.format(new Date(timestamp));
  } catch {
    return "-";
  }
}

export function getErrorMessage(
  error: unknown,
  fallback = "Request failed, please try again later.",
): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}

export function toCreateProcessorScriptPayload(
  values: ProcessorScriptFormValues,
): CreateProcessorScriptPayload {
  const modules = values.modules.map((module) => ({
    module_name: module.module_name.trim(),
    source: module.source,
  }));

  if (modules.length === 0) {
    throw new Error("At least one Rhai module is required");
  }

  return {
    script_key: values.script_key.trim(),
    name: values.name.trim(),
    entry_module: values.entry_module.trim(),
    status: values.status,
    modules,
  };
}

export function toUpdateProcessorScriptPayload(
  values: ProcessorScriptFormValues,
): UpdateProcessorScriptPayload {
  const modules = values.modules.map((module) => ({
    module_name: module.module_name.trim(),
    source: module.source,
  }));

  if (modules.length === 0) {
    throw new Error("At least one Rhai module is required");
  }

  return {
    name: values.name.trim(),
    entry_module: values.entry_module.trim(),
    status: values.status,
    modules,
  };
}

export function toValidateProcessorScriptPayload(
  values: ProcessorScriptFormValues,
): ValidateProcessorScriptPayload {
  const modules = values.modules.map((module) => ({
    module_name: module.module_name.trim(),
    source: module.source,
  }));

  if (modules.length === 0) {
    throw new Error("At least one Rhai module is required");
  }

  return {
    entry_module: values.entry_module.trim(),
    modules,
  };
}

export function toProcessorScriptFormValues(
  detail: ProcessorScriptDetail,
): ProcessorScriptFormValues {
  return {
    script_key: detail.script_key,
    name: detail.name,
    entry_module: detail.entry_module,
    status: detail.status,
    modules: detail.modules.map((module) => ({
      module_name: module.module_name,
      source: module.source,
    })),
  };
}
