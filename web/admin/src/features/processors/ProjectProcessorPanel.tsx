import { Alert, Select, Space, Tag, Typography } from "antd";
import type { ProcessorScript, ProjectProcessor } from "./types";

type ProjectProcessorPanelProps = {
  scripts: ProcessorScript[];
  projectName?: string;
  projectId: number | null;
  binding?: ProjectProcessor | null;
  loading?: boolean;
  updating?: boolean;
  onAssign: (processorScriptId: number) => Promise<void>;
};

export function processorLabel(
  scriptId: number,
  scripts: ProcessorScript[],
): string {
  const script = scripts.find((candidate) => candidate.id === scriptId);
  if (!script) {
    return `Processor #${scriptId}`;
  }

  return `${script.name} (${script.script_key} v${script.version})`;
}

export function ProjectProcessorPanel({
  scripts,
  projectName,
  projectId,
  binding = null,
  loading = false,
  updating = false,
  onAssign,
}: ProjectProcessorPanelProps) {
  const enabledBinding = binding?.enabled ? binding : null;
  const activeScripts = scripts.filter((script) => script.status === "active");
  const defaultScript =
    activeScripts.find((script) => script.script_key === "default") ?? null;
  const value = enabledBinding?.processor_script_id ?? defaultScript?.id;
  const currentScript =
    activeScripts.find((script) => script.id === value) ?? null;
  const options = activeScripts.map((script) => ({
    label: `${script.name} (${script.script_key} v${script.version})`,
    value: script.id,
  }));
  const currentName = currentScript
    ? processorLabel(currentScript.id, scripts)
    : "-";
  const isDefault = currentScript?.script_key === "default";

  return (
    <Space direction="vertical" size={12} style={{ display: "flex" }}>
      <Typography.Title level={4} style={{ margin: 0 }}>
        Processor Binding
      </Typography.Title>
      {projectId !== null ? (
        <Select
          showSearch
          value={value}
          placeholder="Select processor script"
          optionFilterProp="label"
          options={options}
          loading={loading || updating}
          disabled={updating || activeScripts.length === 0}
          style={{ width: "100%" }}
          onChange={(nextValue) => {
            if (nextValue === value) {
              return;
            }
            if (typeof nextValue === "number") {
              void onAssign(nextValue);
            }
          }}
        />
      ) : (
        <Alert
          type="info"
          showIcon
          message="Save the project first to assign a processor."
        />
      )}
      {projectId !== null ? (
        <Typography.Text type="secondary">
          Current project: {projectName ? `${projectName} (#${projectId})` : `#${projectId}`}
        </Typography.Text>
      ) : null}
      <Space size={8}>
        <Typography.Text>Current Processor: {currentName}</Typography.Text>
        {isDefault ? <Tag>default</Tag> : <Tag color="blue">custom</Tag>}
      </Space>
    </Space>
  );
}
