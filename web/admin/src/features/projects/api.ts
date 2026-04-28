import { request, requestJson } from "../../shared/http";
import type {
  CreateProjectPayload,
  Project,
  UpdateProjectPayload,
} from "./types";

type ProjectResponse = {
  appid?: unknown;
  name?: unknown;
  enabled?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
};

function invalidProjectData(message: string): Error {
  return new Error(`项目接口响应无效：${message}`);
}

function normalizeRequiredString(
  value: unknown,
  fieldName: "appid" | "name",
): string {
  if (typeof value !== "string") {
    throw invalidProjectData(`${fieldName} 缺失或不是字符串`);
  }

  const normalized = value.trim();

  if (!normalized) {
    throw invalidProjectData(`${fieldName} 不能为空`);
  }

  return normalized;
}

function normalizeTimestamp(
  value: unknown,
  fieldName: "created_at" | "updated_at",
): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidProjectData(`${fieldName} 缺失或不是有效时间戳`);
  }

  return Math.trunc(value);
}

export function normalizeProjectResponse(value: ProjectResponse): Project {
  if (!value || typeof value !== "object") {
    throw invalidProjectData("项目数据不是对象");
  }

  if (typeof value.enabled !== "boolean") {
    throw invalidProjectData("enabled 缺失或不是布尔值");
  }

  return {
    appid: normalizeRequiredString(value.appid, "appid"),
    name: normalizeRequiredString(value.name, "name"),
    enabled: value.enabled,
    created_at: normalizeTimestamp(value.created_at, "created_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
  };
}

export async function listProjects(): Promise<Project[]> {
  const response = await requestJson<ProjectResponse[]>("/api/admin/projects");

  return normalizeProjectsResponse(response);
}

export function normalizeProjectsResponse(response: unknown): Project[] {
  if (!Array.isArray(response)) {
    throw invalidProjectData("项目列表不是数组");
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
  appid: string,
  payload: UpdateProjectPayload,
): Promise<Project> {
  const response = await requestJson<ProjectResponse>(
    `/api/admin/projects/${encodeURIComponent(appid)}`,
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

export async function deleteProject(appid: string): Promise<void> {
  await request(`/api/admin/projects/${encodeURIComponent(appid)}`, {
    method: "DELETE",
  });
}
