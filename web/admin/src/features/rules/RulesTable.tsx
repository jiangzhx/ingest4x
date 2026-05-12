import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { Rule } from "./types";
import { formatRuleTimestamp } from "./utils";

type RulesTableProps = {
  rules: Rule[];
  actionsDisabled?: boolean;
  deletingRuleId?: number | null;
  onEdit: (rule: Rule) => void;
  onDelete: (rule: Rule) => Promise<void>;
};

function renderRuleStatus(rule: Rule) {
  if (!rule.enabled) {
    return <Tag>Disabled</Tag>;
  }
  return <Tag color="success">Enabled</Tag>;
}

export function RulesTable({
  rules,
  actionsDisabled = false,
  deletingRuleId = null,
  onEdit,
  onDelete,
}: RulesTableProps) {
  const columns: ColumnsType<Rule> = [
    {
      title: "Script",
      dataIndex: "name",
      key: "name",
      render: (value: string) => (
        <Typography.Text strong>{value}</Typography.Text>
      ),
    },
    {
      title: "Status",
      dataIndex: "enabled",
      key: "enabled",
      width: 120,
      render: (_, rule) => renderRuleStatus(rule),
    },
    {
      title: "Updated At",
      dataIndex: "updated_at",
      key: "updated_at",
      width: 180,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatRuleTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      width: 160,
      render: (_, rule) => {
        const isDeleting = deletingRuleId === rule.id;

        return (
          <Space size={8}>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(rule)}
            >
              Edit
            </Button>
            <Popconfirm
              title="Delete script"
              description="After deletion, the rule script must be saved again to enable validation."
              okText="Delete"
              cancelText="Cancel"
              disabled={actionsDisabled}
              okButtonProps={{ danger: true, loading: isDeleting }}
              onConfirm={() => onDelete(rule)}
            >
              <Button
                size="small"
                danger
                disabled={actionsDisabled}
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
    <Table<Rule>
      rowKey="id"
      columns={columns}
      dataSource={rules}
      pagination={false}
      locale={{
        emptyText: (
            <Empty
              description="Current rule set has no scripts"
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        ),
      }}
      scroll={{ x: 700 }}
    />
  );
}
