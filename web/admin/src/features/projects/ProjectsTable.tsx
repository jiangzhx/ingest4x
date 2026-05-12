import { CopyOutlined } from "@ant-design/icons";
import { Button, Empty, message, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { processorLabel } from "../processors/ProjectProcessorPanel";
import type { ProcessorScript, ProjectProcessor } from "../processors/types";
import type { Project } from "./types";
import { formatProjectTimestamp } from "./utils";

type ProjectsTableProps = {
  projects: Project[];
  processorScripts?: ProcessorScript[];
  processorBindings?: ProjectProcessor[];
  deletingProjectId?: number | null;
  actionsDisabled?: boolean;
  onEdit: (project: Project) => void;
  onDelete: (project: Project) => Promise<void>;
};

async function copyTextToClipboard(text: string): Promise<boolean> {
  const clipboard =
    typeof navigator === "undefined" ? undefined : navigator.clipboard;

  try {
    if (clipboard) {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the textarea fallback below.
  }

  if (typeof document === "undefined" || !document.body) {
    return false;
  }

  const selection = document.getSelection();
  const selectedRange =
    selection && selection.rangeCount > 0 ? selection.getRangeAt(0) : null;
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  textarea.style.top = "0";
  document.body.appendChild(textarea);

  try {
    textarea.focus();
    textarea.select();
    return document.execCommand("copy");
  } finally {
    document.body.removeChild(textarea);

    if (selection && selectedRange) {
      selection.removeAllRanges();
      selection.addRange(selectedRange);
    }
  }
}

async function handleCopyToken(tokenText: string) {
  const copied = await copyTextToClipboard(tokenText);
  if (copied) {
    message.success("Token copied");
    return;
  }

  message.error("Failed to copy token, please copy manually");
}

function projectProcessorLabel(
  project: Project,
  scripts: ProcessorScript[],
  bindings: ProjectProcessor[],
) {
  const binding = bindings.find((candidate) => candidate.project_id === project.id);
  const defaultScript =
    scripts.find(
      (candidate) =>
        candidate.script_key === "default" && candidate.status === "active",
    ) ?? null;
  const scriptId =
    binding && binding.enabled ? binding.processor_script_id : defaultScript?.id;
  if (scriptId === undefined) {
    return <Tag>-</Tag>;
  }

  const script = scripts.find((candidate) => candidate.id === scriptId);
  if (script?.script_key === "default") {
    return <Tag>default</Tag>;
  }

  return <Tag color="blue">{processorLabel(scriptId, scripts)}</Tag>;
}

export function ProjectsTable({
  projects,
  processorScripts = [],
  processorBindings = [],
  deletingProjectId = null,
  actionsDisabled = false,
  onEdit,
  onDelete,
}: ProjectsTableProps) {
  const columns: ColumnsType<Project> = [
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      width: 90,
      render: (value: number) => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: "Token",
      key: "ingest_token",
      width: 360,
      render: (_, project) => {
        const tokenText = project.ingest_token;

        return (
          <Space size={6}>
            <Typography.Text
              code
              style={{ whiteSpace: "normal", wordBreak: "break-all" }}
            >
              {tokenText}
            </Typography.Text>
            <Button
              aria-label="Copy token"
              icon={<CopyOutlined />}
              size="small"
              type="text"
              onClick={() => {
                void handleCopyToken(tokenText);
              }}
            />
          </Space>
        );
      },
    },
    {
      title: "Project Name",
      dataIndex: "name",
      key: "name",
      render: (value: string) => <Typography.Text strong>{value}</Typography.Text>,
    },
    {
      title: "Enabled",
      dataIndex: "enabled",
      key: "enabled",
      width: 140,
      render: (enabled: boolean) =>
        enabled ? <Tag color="success">Enabled</Tag> : <Tag>Disabled</Tag>,
    },
    {
      title: "Processor",
      key: "processor",
      width: 180,
      render: (_, project) =>
        projectProcessorLabel(project, processorScripts, processorBindings),
    },
    {
      title: "Created At",
      dataIndex: "created_at",
      key: "created_at",
      width: 200,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatProjectTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "Updated At",
      dataIndex: "updated_at",
      key: "updated_at",
      width: 200,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatProjectTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      width: 180,
      fixed: "right",
      render: (_, project) => {
        const isDeleting = deletingProjectId === project.id;
        const disableRowActions = actionsDisabled && !isDeleting;
        const deleteButtonLabel = isDeleting ? "Deleting..." : "Delete";

        return (
          <Space size={8}>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(project)}
            >
              Edit
            </Button>
            <Popconfirm
              title="Delete project"
              description={
                isDeleting
                  ? `Deleting project ${project.name}...`
                  : `Project ${project.name} will be deleted and cannot be undone.`
              }
              okText="Delete"
              cancelText="Cancel"
              disabled={disableRowActions || isDeleting}
              okButtonProps={{ danger: true, loading: isDeleting }}
              onConfirm={() => onDelete(project)}
            >
              <Button
                size="small"
                danger
                disabled={disableRowActions}
                loading={isDeleting}
              >
                {deleteButtonLabel}
              </Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <Table<Project>
      rowKey="id"
      columns={columns}
      dataSource={projects}
      pagination={false}
      locale={{
        emptyText: (
          <Empty
            description="No projects are configured"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: 1300 }}
    />
  );
}
