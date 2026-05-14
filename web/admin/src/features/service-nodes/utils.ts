import type { ServiceNodeStatus } from "./types";

export function formatServiceNodeTimestamp(value: number): string {
  return new Date(value).toLocaleString();
}

export function getServiceNodeStatusLabel(status: ServiceNodeStatus): string {
  switch (status) {
    case "starting":
      return "Starting";
    case "running":
      return "Running";
    case "stopping":
      return "Stopping";
    case "stopped":
      return "Stopped";
    case "stale":
      return "Stale";
  }
}

export function getServiceNodeStatusColor(status: ServiceNodeStatus): string {
  switch (status) {
    case "running":
      return "success";
    case "starting":
    case "stopping":
      return "processing";
    case "stopped":
      return "default";
    case "stale":
      return "warning";
  }
}

export function getHeartbeatAge(lastSeenAt: number, now = Date.now()): string {
  const ageSeconds = Math.max(0, Math.floor((now - lastSeenAt) / 1000));
  if (ageSeconds < 60) {
    return `${ageSeconds}s`;
  }

  const ageMinutes = Math.floor(ageSeconds / 60);
  if (ageMinutes < 60) {
    return `${ageMinutes}m`;
  }

  return `${Math.floor(ageMinutes / 60)}h`;
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
