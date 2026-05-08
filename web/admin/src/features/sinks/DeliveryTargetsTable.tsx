import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { DeliveryTarget } from "./types";
import {
  formatSinkTimestamp,
  getDeliveryTargetTypeLabel,
} from "./utils";

type DeliveryTargetsTableProps = {
  targets: DeliveryTarget[];
  deletingTargetId?: number | null;
  actionsDisabled?: boolean;
  onEdit: (target: DeliveryTarget) => void;
  onDelete: (target: DeliveryTarget) => Promise<void>;
};

export function DeliveryTargetsTable({
  targets,
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
      title: "展示名",
      dataIndex: "name",
      key: "name",
      render: (value: string) => <Typography.Text strong>{value}</Typography.Text>,
    },
    {
      title: "类型",
      dataIndex: "target_type",
      key: "target_type",
      width: 120,
      render: (value: DeliveryTarget["target_type"]) => (
        <Tag color={value === "kafka" ? "blue" : "default"}>
          {getDeliveryTargetTypeLabel(value)}
        </Tag>
      ),
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
              编辑
            </Button>
            <Popconfirm
              title="删除 Delivery Target"
              description={`将删除 ${target.target_id}，如果仍有 sink 使用它，后端会拒绝删除。`}
              okText="删除"
              cancelText="取消"
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
                删除
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
            description="当前还没有 delivery target"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: 1000 }}
    />
  );
}
