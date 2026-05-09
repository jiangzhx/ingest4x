import { requestJson } from "../../shared/http";
import type { ServiceNode, ServiceNodeStatus } from "./types";

type ServiceNodeResponse = {
  node_id?: unknown;
  hostname?: unknown;
  machine_ip?: unknown;
  ingest_bind_address?: unknown;
  management_bind_address?: unknown;
  version?: unknown;
  status?: unknown;
  started_at?: unknown;
  last_seen_at?: unknown;
  updated_at?: unknown;
  metadata_json?: unknown;
};

const serviceNodeStatuses = new Set<ServiceNodeStatus>([
  "starting",
  "running",
  "stopping",
  "stopped",
  "stale",
]);

function invalidServiceNodeData(message: string): Error {
  return new Error(`节点接口响应无效：${message}`);
}

function normalizeRequiredString(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw invalidServiceNodeData(`${fieldName} 缺失或不是字符串`);
  }

  const normalized = value.trim();
  if (!normalized) {
    throw invalidServiceNodeData(`${fieldName} 不能为空`);
  }

  return normalized;
}

function normalizeOptionalString(
  value: unknown,
  fieldName: string,
): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "string") {
    throw invalidServiceNodeData(`${fieldName} 不是字符串`);
  }

  const normalized = value.trim();
  return normalized ? normalized : null;
}

function normalizeTimestamp(value: unknown, fieldName: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidServiceNodeData(`${fieldName} 缺失或不是有效时间戳`);
  }

  return Math.trunc(value);
}

function normalizeStatus(value: unknown): ServiceNodeStatus {
  const normalized = normalizeRequiredString(value, "status");
  if (!serviceNodeStatuses.has(normalized as ServiceNodeStatus)) {
    throw invalidServiceNodeData("status 不是支持的值");
  }

  return normalized as ServiceNodeStatus;
}

function normalizeMetadata(value: unknown): Record<string, unknown> | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (Array.isArray(value) || typeof value !== "object") {
    throw invalidServiceNodeData("metadata_json 不是对象");
  }

  return value as Record<string, unknown>;
}

export function normalizeServiceNodeResponse(
  value: ServiceNodeResponse,
): ServiceNode {
  if (!value || typeof value !== "object") {
    throw invalidServiceNodeData("节点数据不是对象");
  }

  return {
    node_id: normalizeRequiredString(value.node_id, "node_id"),
    hostname: normalizeOptionalString(value.hostname, "hostname"),
    machine_ip: normalizeOptionalString(value.machine_ip, "machine_ip"),
    ingest_bind_address: normalizeRequiredString(
      value.ingest_bind_address,
      "ingest_bind_address",
    ),
    management_bind_address: normalizeRequiredString(
      value.management_bind_address,
      "management_bind_address",
    ),
    version: normalizeRequiredString(value.version, "version"),
    status: normalizeStatus(value.status),
    started_at: normalizeTimestamp(value.started_at, "started_at"),
    last_seen_at: normalizeTimestamp(value.last_seen_at, "last_seen_at"),
    updated_at: normalizeTimestamp(value.updated_at, "updated_at"),
    metadata_json: normalizeMetadata(value.metadata_json),
  };
}

export function normalizeServiceNodesResponse(response: unknown): ServiceNode[] {
  if (!Array.isArray(response)) {
    throw invalidServiceNodeData("节点列表不是数组");
  }

  return response.map((node) => normalizeServiceNodeResponse(node));
}

export async function listServiceNodes(): Promise<ServiceNode[]> {
  const response = await requestJson<ServiceNodeResponse[]>(
    "/api/admin/service-nodes",
  );

  return normalizeServiceNodesResponse(response);
}
