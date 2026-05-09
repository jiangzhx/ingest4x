import type {
  CreateProjectPayload,
  ProjectFormValues,
  UpdateProjectPayload,
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

export function formatProjectTimestamp(timestamp: number): string {
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

export function toCreateProjectPayload(
  project: ProjectFormValues,
): CreateProjectPayload {
  const ingestToken = project.ingest_token?.trim();
  return {
    name: project.name.trim(),
    enabled: project.enabled,
    ...(ingestToken ? { ingest_token: ingestToken } : {}),
  };
}

export function toUpdateProjectPayload(
  project: ProjectFormValues,
): UpdateProjectPayload {
  const ingestToken = project.ingest_token?.trim();

  return {
    name: project.name.trim(),
    enabled: project.enabled,
    ...(ingestToken ? { ingest_token: ingestToken } : {}),
    ...(project.regenerate_ingest_token ? { regenerate_ingest_token: true } : {}),
  };
}
