export type ProjectAuthMode = "token" | "public";

export interface Project {
  id: number;
  project_key: string;
  name: string;
  enabled: boolean;
  auth_mode: ProjectAuthMode;
  allowed_ips: string[];
  ingest_token: string;
  ingest_token_prefix: string;
  created_at: number;
  updated_at: number;
}

export interface ProjectFormValues {
  name: string;
  project_key: string;
  enabled: boolean;
  auth_mode: ProjectAuthMode;
  allowed_ips_text?: string;
  ingest_token?: string;
}

export interface CreateProjectPayload {
  name: string;
  project_key: string;
  enabled: boolean;
  auth_mode: ProjectAuthMode;
  allowed_ips: string[];
  ingest_token?: string;
}

export interface UpdateProjectPayload {
  name?: string;
  project_key?: string;
  enabled?: boolean;
  auth_mode?: ProjectAuthMode;
  allowed_ips?: string[];
  ingest_token?: string;
}
