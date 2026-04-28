export interface Project {
  appid: string;
  name: string;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface ProjectFormValues {
  appid: string;
  name: string;
  enabled: boolean;
}

export interface CreateProjectPayload {
  appid: string;
  name: string;
  enabled: boolean;
}

export interface UpdateProjectPayload {
  name?: string;
  enabled?: boolean;
}
