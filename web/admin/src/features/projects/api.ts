import { request, requestJson } from "../../shared/http";
import type {
  CreateProjectPayload,
  Project,
  ProjectAuthMode,
  UpdateProjectPayload,
} from "./types";

type ProjectResponse = {
  id?: unknown;
  project_key?: unknown;
  name?: unknown;
  enabled?: unknown;
  auth_mode?: unknown;
  allowed_ips?: unknown;
  ingest_token?: unknown;
  ingest_token_prefix?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

function invalidProjectData(message: string): Error {
  return new Error(`Invalid project API response: ${message}`);
}

function normalizeRequiredString(
  value: unknown,
  fieldName: "project_key" | "name" | "ingest_token" | "ingest_token_prefix",
): string {
  if (typeof value !== "string") {
    throw invalidProjectData(`${fieldName} is missing or not a string`);
  }

  const normalized = value.trim();

  if (!normalized) {
    throw invalidProjectData(`${fieldName} cannot be empty`);
  }

  return normalized;
}

function normalizePositiveInteger(value: unknown, fieldName: "id"): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw invalidProjectData(`${fieldName} is missing or not a positive integer`);
  }
  return value;
}

function normalizeTimestamp(
  value: unknown,
  fieldName: "created_at" | "updated_at",
): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidProjectData(`${fieldName} is missing or not a valid timestamp`);
  }

  return Math.trunc(value);
}

function normalizeAuthMode(value: unknown): ProjectAuthMode {
  if (value === "token" || value === "public") {
    return value;
  }

  throw invalidProjectData("auth_mode is missing or invalid");
}

function normalizeStringList(value: unknown, fieldName: "allowed_ips"): string[] {
  if (!Array.isArray(value)) {
    throw invalidProjectData(`${fieldName} is missing or not an array`);
  }

  return value.map((item, index) => {
    if (typeof item !== "string") {
      throw invalidProjectData(`${fieldName}[${index}] is not a string`);
    }

    return item.trim();
  });
}

export function normalizeProjectResponse(value: ProjectResponse): Project {
  if (!value || typeof value !== "object") {
    throw invalidProjectData("project data is not an object");
  }

  if (typeof value.enabled !== "boolean") {
    throw invalidProjectData("enabled is missing or not a boolean");
  }

  const project: Project = {
    id: normalizePositiveInteger(value.id, "id"),
    project_key: normalizeRequiredString(value.project_key, "project_key"),
    name: normalizeRequiredString(value.name, "name"),
    enabled: value.enabled,
    auth_mode: normalizeAuthMode(value.auth_mode),
    allowed_ips: normalizeStringList(value.allowed_ips, "allowed_ips"),
    ingest_token: normalizeRequiredString(value.ingest_token, "ingest_token"),
    ingest_token_prefix: normalizeRequiredString(
      value.ingest_token_prefix,
      "ingest_token_prefix",
    ),
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };

  return project;
}

export async function listProjects(): Promise<Project[]> {
  const response = await requestJson<ProjectResponse[]>("/api/admin/projects");

  return normalizeProjectsResponse(response);
}

export function normalizeProjectsResponse(response: unknown): Project[] {
  if (!Array.isArray(response)) {
    throw invalidProjectData("project list is not an array");
  }

  return response.map((project) => normalizeProjectResponse(project));
}

export async function createProject(
  payload: CreateProjectPayload,
): Promise<Project> {
  const response = await requestJson<ProjectResponse>("/api/admin/projects", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return normalizeProjectResponse(response);
}

export async function updateProject(
  projectId: number,
  payload: UpdateProjectPayload,
): Promise<Project> {
  const response = await requestJson<ProjectResponse>(
    `/api/admin/projects/${projectId}`,
    {
      method: "PUT",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );

  return normalizeProjectResponse(response);
}

export async function deleteProject(projectId: number): Promise<void> {
  await request(`/api/admin/projects/${projectId}`, {
    method: "DELETE",
  });
}
