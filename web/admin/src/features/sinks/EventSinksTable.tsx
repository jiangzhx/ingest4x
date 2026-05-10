import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { DeliveryTarget, EventSink, SinkTypeMetadata } from "./types";
import {
  formatSinkTimestamp,
  getDeliveryTargetTypeLabel,
} from "./utils";

type EventSinksTableProps = {
  sinks: EventSink[];
  targets: DeliveryTarget[];
  sinkTypes: SinkTypeMetadata[];
  deletingSinkId?: number | null;
  actionsDisabled?: boolean;
  onEdit: (sink: EventSink) => void;
  onDelete: (sink: EventSink) => Promise<void>;
};

function targetLabel(
  sink: EventSink,
  targets: DeliveryTarget[],
  sinkTypes: SinkTypeMetadata[],
): string {
  const target = targets.find((candidate) => candidate.id === sink.delivery_target_id);

  if (!target) {
    return `#${sink.delivery_target_id}`;
  }

  return `${target.target_id} / ${getDeliveryTargetTypeLabel(
    target.target_type,
    sinkTypes,
  )}`;
}

function destinationLabel(sink: EventSink): string {
  const topic = sink.destination_json.topic;

  if (typeof topic === "string" && topic.trim()) {
    return topic;
  }

  return JSON.stringify(sink.destination_json);
}

export function EventSinksTable({
  sinks,
  targets,
  sinkTypes,
  deletingSinkId = null,
  actionsDisabled = false,
  onEdit,
  onDelete,
}: EventSinksTableProps) {
  const columns: ColumnsType<EventSink> = [
    {
      title: "Sink ID",
      dataIndex: "sink_id",
      key: "sink_id",
      width: 180,
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
      title: "Delivery Target",
      dataIndex: "delivery_target_id",
      key: "delivery_target_id",
      width: 260,
      ellipsis: true,
      render: (_, sink) => {
        const label = targetLabel(sink, targets, sinkTypes);

        return (
          <Typography.Text ellipsis={{ tooltip: label }}>
            {label}
          </Typography.Text>
        );
      },
    },
    {
      title: "投递目标",
      dataIndex: "destination_json",
      key: "destination_json",
      width: 240,
      ellipsis: true,
      render: (_, sink) => {
        const label = destinationLabel(sink);

        return (
          <Typography.Text ellipsis={{ tooltip: label }}>
            {label}
          </Typography.Text>
        );
      },
    },
    {
      title: "Offset",
      dataIndex: "auto_offset_reset",
      key: "auto_offset_reset",
      width: 120,
      render: (value: EventSink["auto_offset_reset"]) => <Tag>{value}</Tag>,
    },
    {
      title: "状态",
      dataIndex: "enabled",
      key: "enabled",
      width: 120,
      render: (enabled: boolean) =>
        enabled ? <Tag color="success">已启用</Tag> : <Tag>已停用</Tag>,
    },
    {
      title: "更新时间",
      dataIndex: "updated_at",
      key: "updated_at",
      width: 180,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatSinkTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "操作",
      key: "actions",
      width: 160,
      fixed: "right",
      render: (_, sink) => {
        const isDeleting = deletingSinkId === sink.id;
        const disableRowActions = actionsDisabled && !isDeleting;

        return (
          <Space size={8}>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(sink)}
            >
              编辑
            </Button>
            <Popconfirm
              title="删除 Event Sink"
              description={`将删除 ${sink.sink_id}，对应 checkpoint 文件不会被删除。`}
              okText="删除"
              cancelText="取消"
              disabled={disableRowActions || isDeleting}
              okButtonProps={{ danger: true, loading: isDeleting }}
              onConfirm={() => onDelete(sink)}
            >
              <Button
                size="small"
                danger
                disabled={disableRowActions}
                loading={isDeleting}
              >
                删除
              </Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <Table<EventSink>
      rowKey="id"
      columns={columns}
      dataSource={sinks}
      pagination={false}
      tableLayout="fixed"
      locale={{
        emptyText: (
          <Empty
            description="当前还没有 event sink"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: 1460 }}
    />
  );
}
