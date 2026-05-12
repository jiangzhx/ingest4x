import { request, requestJson } from "../../shared/http";
import type {
  AssignProjectProcessorPayload,
  CreateProcessorScriptPayload,
  ProcessorScript,
  ProcessorScriptDetail,
  ProcessorScriptModule,
  ProcessorScriptStatus,
  ProjectProcessor,
  UpdateProcessorScriptPayload,
  UpdateProcessorScriptStatusPayload,
  ValidateProcessorScriptPayload,
} from "./types";

type ProcessorScriptResponse = {
  id?: unknown;
  script_key?: unknown;
  name?: unknown;
  entry_module?: unknown;
  version?: unknown;
  status?: unknown;
  checksum?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
  activated_at?: unknown;
};

type ProcessorScriptModuleResponse = {
  id?: unknown;
  processor_script_id?: unknown;
  module_name?: unknown;
  source?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

type ProcessorScriptDetailResponse = ProcessorScriptResponse & {
  modules?: unknown;
};

type ProjectProcessorResponse = {
  id?: unknown;
  project_id?: unknown;
  processor_script_id?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

function invalidProcessorData(message: string): Error {
  return new Error(`Invalid Processor API response: ${message}`);
}

function normalizeRequiredString(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw invalidProcessorData(`${fieldName} is missing or not a string`);
  }

  const normalized = value.trim();
  if (!normalized) {
    throw invalidProcessorData(`${fieldName} cannot be empty`);
  }

  return normalized;
}

function normalizeInteger(value: unknown, fieldName: string): number {
  if (!Number.isInteger(value) || typeof value !== "number") {
    throw invalidProcessorData(`${fieldName} is missing or not an integer`);
  }

  return value;
}

function normalizePositiveInteger(value: unknown, fieldName: string): number {
  const integer = normalizeInteger(value, fieldName);
  if (integer <= 0) {
    throw invalidProcessorData(`${fieldName} must be greater than 0`);
  }

  return integer;
}

function normalizeTimestamp(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidProcessorData(`${fieldName} is missing or not a valid timestamp`);
  }

  return Math.trunc(value);
}

function normalizeNullableTimestamp(value: unknown, fieldName: string): number | null {
  if (value === null) {
    return null;
  }

  return normalizeTimestamp(value, fieldName);
}

function normalizeStatus(value: unknown): ProcessorScriptStatus {
  if (value !== "draft" && value !== "active" && value !== "archived") {
    throw invalidProcessorData("status is not a supported value");
  }

  return value;
}

export function normalizeProcessorScriptResponse(
  value: ProcessorScriptResponse,
): ProcessorScript {
  if (!value || typeof value !== "object") {
    throw invalidProcessorData("processor script data is not an object");
  }

  return {
    id: normalizePositiveInteger(value.id, "id"),
    script_key: normalizeRequiredString(value.script_key, "script_key"),
    name: normalizeRequiredString(value.name, "name"),
    entry_module: normalizeRequiredString(value.entry_module, "entry_module"),
    version: normalizePositiveInteger(value.version, "version"),
    status: normalizeStatus(value.status),
    checksum: normalizeRequiredString(value.checksum, "checksum"),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
    activated_at: normalizeNullableTimestamp(value.activated_at, "activated_at"),
  };
}

function normalizeProcessorScriptModuleResponse(
  value: ProcessorScriptModuleResponse,
): ProcessorScriptModule {
  if (!value || typeof value !== "object") {
    throw invalidProcessorData("processor module data is not an object");
  }

  return {
    id: normalizePositiveInteger(value.id, "id"),
    processor_script_id: normalizePositiveInteger(
      value.processor_script_id,
      "processor_script_id",
    ),
    module_name: normalizeRequiredString(value.module_name, "module_name"),
    source: normalizeRequiredString(value.source, "source"),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

function normalizeProcessorScriptDetailResponse(
  value: ProcessorScriptDetailResponse,
): ProcessorScriptDetail {
  const script = normalizeProcessorScriptResponse(value);
  if (!Array.isArray(value.modules)) {
    throw invalidProcessorData("modules is not an array");
  }

  return {
    ...script,
    modules: value.modules.map((module) =>
      normalizeProcessorScriptModuleResponse(module),
    ),
  };
}

function normalizeProjectProcessorResponse(
  value: ProjectProcessorResponse,
): ProjectProcessor {
  if (!value || typeof value !== "object") {
    throw invalidProcessorData("project processor data is not an object");
  }
  if (typeof value.enabled !== "boolean") {
    throw invalidProcessorData("enabled is missing or not a boolean");
  }

  return {
    id: normalizePositiveInteger(value.id, "id"),
    project_id: normalizePositiveInteger(value.project_id, "project_id"),
    processor_script_id: normalizePositiveInteger(
      value.processor_script_id,
      "processor_script_id",
    ),
    enabled: value.enabled,
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export async function listProcessorScripts(): Promise<ProcessorScript[]> {
  const response = await requestJson<ProcessorScriptResponse[]>(
    "/api/admin/processor-scripts",
  );

  if (!Array.isArray(response)) {
    throw invalidProcessorData("processor script list is not an array");
  }

  return response.map((script) => normalizeProcessorScriptResponse(script));
}

export async function getProcessorScript(
  id: number,
): Promise<ProcessorScriptDetail> {
  const response = await requestJson<ProcessorScriptDetailResponse>(
    `/api/admin/processor-scripts/${id}`,
  );

  return normalizeProcessorScriptDetailResponse(response);
}

export async function createProcessorScript(
  payload: CreateProcessorScriptPayload,
): Promise<ProcessorScript> {
  const response = await requestJson<ProcessorScriptResponse>(
    "/api/admin/processor-scripts",
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeProcessorScriptResponse(response);
}

export async function updateProcessorScript(
  id: number,
  payload: UpdateProcessorScriptPayload,
): Promise<ProcessorScript> {
  const response = await requestJson<ProcessorScriptResponse>(
    `/api/admin/processor-scripts/${id}`,
    {
      method: "PUT",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeProcessorScriptResponse(response);
}

export async function validateProcessorScript(
  payload: ValidateProcessorScriptPayload,
): Promise<void> {
  await request("/api/admin/processor-scripts/validate", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function updateProcessorScriptStatus(
  id: number,
  payload: UpdateProcessorScriptStatusPayload,
): Promise<ProcessorScript> {
  const response = await requestJson<ProcessorScriptResponse>(
    `/api/admin/processor-scripts/${id}/status`,
    {
      method: "PUT",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeProcessorScriptResponse(response);
}

export async function listProjectProcessors(): Promise<ProjectProcessor[]> {
  const response = await requestJson<ProjectProcessorResponse[]>(
    "/api/admin/project-processors",
  );

  if (!Array.isArray(response)) {
    throw invalidProcessorData("project processor list is not an array");
  }

  return response.map((binding) => normalizeProjectProcessorResponse(binding));
}

export async function assignProjectProcessor(
  projectId: number,
  payload: AssignProjectProcessorPayload,
): Promise<void> {
  await request(`/api/admin/projects/${projectId}/processor`, {
    method: "PUT",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function deleteProjectProcessor(projectId: number): Promise<void> {
  await request(`/api/admin/projects/${projectId}/processor`, {
    method: "DELETE",
  });
}
