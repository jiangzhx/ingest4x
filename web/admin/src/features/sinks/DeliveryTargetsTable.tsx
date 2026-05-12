import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { DeliveryTarget, SinkTypeMetadata } from "./types";
import {
  formatSinkTimestamp,
  getDeliveryTargetTypeLabel,
} from "./utils";

type DeliveryTargetsTableProps = {
  targets: DeliveryTarget[];
  sinkTypes: SinkTypeMetadata[];
  deletingTargetId?: number | null;
  actionsDisabled?: boolean;
  onEdit: (target: DeliveryTarget) => void;
  onDelete: (target: DeliveryTarget) => Promise<void>;
};

export function DeliveryTargetsTable({
  targets,
  sinkTypes,
  deletingTargetId = null,
  actionsDisabled = false,
  onEdit,
  onDelete,
}: DeliveryTargetsTableProps) {
  const columns: ColumnsType<DeliveryTarget> = [
    {
      title: "Target ID",
      dataIndex: "target_id",
      key: "target_id",
      width: 220,
      render: (value: string) => <Typography.Text code>{value}</Typography.Text>,
    },
    {
      title: "Display Name",
      dataIndex: "name",
      key: "name",
      render: (value: string) => <Typography.Text strong>{value}</Typography.Text>,
    },
    {
      title: "Type",
      dataIndex: "target_type",
      key: "target_type",
      width: 120,
      render: (value: DeliveryTarget["target_type"]) => (
        <Tag color={value === "kafka" ? "blue" : "default"}>
          {getDeliveryTargetTypeLabel(value, sinkTypes)}
        </Tag>
      ),
    },
    {
      title: "Status",
      dataIndex: "enabled",
      key: "enabled",
      width: 120,
      render: (enabled: boolean) =>
        enabled ? <Tag color="success">Enabled</Tag> : <Tag>Disabled</Tag>,
    },
    {
      title: "Updated At",
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
      title: "Actions",
      key: "actions",
      width: 180,
      fixed: "right",
      render: (_, target) => {
        const isDeleting = deletingTargetId === target.id;
        const disableRowActions = actionsDisabled && !isDeleting;

        return (
          <Space size={8}>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(target)}
            >
              Edit
            </Button>
            <Popconfirm
              title="Delete Delivery Target"
              description={`Target ${target.target_id} will be deleted. Deletion is blocked if any sink still references it.`}
              okText="Delete"
              cancelText="Cancel"
              disabled={disableRowActions || isDeleting}
              okButtonProps={{ danger: true, loading: isDeleting }}
              onConfirm={() => onDelete(target)}
            >
              <Button
                size="small"
                danger
                disabled={disableRowActions}
                loading={isDeleting}
              >
                Delete
              </Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <Table<DeliveryTarget>
      rowKey="id"
      columns={columns}
      dataSource={targets}
      pagination={false}
      locale={{
        emptyText: (
          <Empty
            description="No delivery targets are configured yet"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: 1000 }}
    />
  );
}
