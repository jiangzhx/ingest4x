export type ProcessorScriptStatus = "draft" | "active" | "archived";

export interface ProcessorScript {
  id: number;
  script_key: string;
  name: string;
  entry_module: string;
  version: number;
  status: ProcessorScriptStatus;
  checksum: string;
  created_at: number;
  updated_at: number;
  activated_at: number | null;
}

export interface ProcessorScriptModule {
  id: number;
  processor_script_id: number;
  module_name: string;
  source: string;
  created_at: number;
  updated_at: number;
}

export interface ProcessorScriptDetail extends ProcessorScript {
  modules: ProcessorScriptModule[];
}

export interface ProjectProcessor {
  id: number;
  project_id: number;
  processor_script_id: number;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface ProcessorScriptModuleFormValues {
  module_name: string;
  source: string;
}

export interface ProcessorScriptFormValues {
  script_key: string;
  name: string;
  entry_module: string;
  status: ProcessorScriptStatus;
  modules: ProcessorScriptModuleFormValues[];
}

export interface CreateProcessorScriptPayload {
  script_key: string;
  name: string;
  entry_module: string;
  status: ProcessorScriptStatus;
  modules: ProcessorScriptModuleFormValues[];
}

export interface UpdateProcessorScriptPayload {
  name: string;
  entry_module: string;
  status: ProcessorScriptStatus;
  modules: ProcessorScriptModuleFormValues[];
}

export interface ValidateProcessorScriptPayload {
  entry_module: string;
  modules: ProcessorScriptModuleFormValues[];
}

export interface UpdateProcessorScriptStatusPayload {
  status: ProcessorScriptStatus;
}

export interface AssignProjectProcessorPayload {
  processor_script_id: number;
  enabled: boolean;
}
