export interface Project {
  id: number;
  name: string;
  enabled: boolean;
  ingest_token_prefix: string;
  created_at: number;
  updated_at: number;
}

export interface ProjectFormValues {
  name: string;
  enabled: boolean;
  ingest_token?: string;
}

export interface CreateProjectPayload {
  name: string;
  enabled: boolean;
  ingest_token?: string;
}

export interface UpdateProjectPayload {
  name?: string;
  enabled?: boolean;
}
