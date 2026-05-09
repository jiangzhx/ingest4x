export type ServiceNodeStatus =
  | "starting"
  | "running"
  | "stopping"
  | "stopped"
  | "stale";

export interface ServiceNode {
  node_id: string;
  hostname: string | null;
  machine_ip: string | null;
  ingest_bind_address: string;
  management_bind_address: string;
  version: string;
  status: ServiceNodeStatus;
  started_at: number;
  last_seen_at: number;
  updated_at: number;
  metadata_json: Record<string, unknown> | null;
}
