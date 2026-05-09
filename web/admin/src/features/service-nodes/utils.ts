import type { ServiceNodeStatus } from "./types";

export function formatServiceNodeTimestamp(value: number): string {
  return new Date(value).toLocaleString();
}

export function getServiceNodeStatusLabel(status: ServiceNodeStatus): string {
  switch (status) {
    case "starting":
      return "启动中";
    case "running":
      return "运行中";
    case "stopping":
      return "停止中";
    case "stopped":
      return "已停止";
    case "stale":
      return "已过期";
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
