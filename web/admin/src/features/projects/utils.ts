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
  fallback = "Request failed, please try again later.",
): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}

export function parseAllowedIps(value?: string): string[] {
  return (value ?? "")
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function toCreateProjectPayload(
  project: ProjectFormValues,
): CreateProjectPayload {
  const authMode = project.auth_mode;
  const ingestToken =
    authMode === "token" ? project.ingest_token?.trim() : undefined;
  return {
    name: project.name.trim(),
    project_key: project.project_key.trim(),
    enabled: project.enabled,
    auth_mode: authMode,
    allowed_ips: parseAllowedIps(project.allowed_ips_text),
    ...(ingestToken ? { ingest_token: ingestToken } : {}),
  };
}

export function toUpdateProjectPayload(
  project: ProjectFormValues,
): UpdateProjectPayload {
  const ingestToken =
    project.auth_mode === "token" ? project.ingest_token?.trim() : undefined;

  return {
    name: project.name.trim(),
    project_key: project.project_key.trim(),
    enabled: project.enabled,
    auth_mode: project.auth_mode,
    allowed_ips: parseAllowedIps(project.allowed_ips_text),
    ...(ingestToken ? { ingest_token: ingestToken } : {}),
  };
}
