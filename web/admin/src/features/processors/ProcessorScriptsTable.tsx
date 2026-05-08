import { Button, Empty, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { ProcessorScript } from "./types";
import { formatProcessorTimestamp } from "./utils";

type ProcessorScriptsTableProps = {
  scripts: ProcessorScript[];
  updatingScriptId?: number | null;
  onView: (script: ProcessorScript) => void;
  onStatusChange: (script: ProcessorScript) => Promise<void>;
};

function statusTag(status: ProcessorScript["status"]) {
  if (status === "active") {
    return <Tag color="success">active</Tag>;
  }
  if (status === "draft") {
    return <Tag color="warning">draft</Tag>;
  }
  return <Tag>archived</Tag>;
}

export function ProcessorScriptsTable({
  scripts,
  updatingScriptId = null,
  onView,
  onStatusChange,
}: ProcessorScriptsTableProps) {
  const columns: ColumnsType<ProcessorScript> = [
    {
      title: "Script Key",
      dataIndex: "script_key",
      key: "script_key",
      width: 220,
      render: (value: string) => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: "展示名",
      dataIndex: "name",
      key: "name",
      width: 220,
      ellipsis: true,
      render: (value: string) => (
        <Typography.Text strong ellipsis={{ tooltip: value }}>
          {value}
        </Typography.Text>
      ),
    },
    {
      title: "Entry",
      dataIndex: "entry_module",
      key: "entry_module",
      width: 120,
      render: (value: string) => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: "版本",
      dataIndex: "version",
      key: "version",
      width: 90,
      render: (value: number) => <Tag>v{value}</Tag>,
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      width: 110,
      render: statusTag,
    },
    {
      title: "Checksum",
      dataIndex: "checksum",
      key: "checksum",
      width: 140,
      render: (value: string) => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: "更新时间",
      dataIndex: "updated_at",
      key: "updated_at",
      width: 180,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatProcessorTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "操作",
      key: "actions",
      width: 190,
      fixed: "right",
      render: (_, script) => {
        const isActive = script.status === "active";
        const isUpdating = updatingScriptId === script.id;

        return (
          <Space>
            <Button size="small" onClick={() => onView(script)}>
              查看
            </Button>
            <Button
              size="small"
              danger={isActive}
              loading={isUpdating}
              disabled={updatingScriptId !== null && !isUpdating}
              onClick={() => {
                void onStatusChange(script);
              }}
            >
              {isActive ? "禁用" : "启用"}
            </Button>
          </Space>
        );
      },
    },
  ];

  return (
    <Table<ProcessorScript>
      rowKey="id"
      columns={columns}
      dataSource={scripts}
      pagination={false}
      tableLayout="fixed"
      locale={{
        emptyText: (
          <Empty
            description="当前还没有 processor script"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: 1220 }}
    />
  );
}
