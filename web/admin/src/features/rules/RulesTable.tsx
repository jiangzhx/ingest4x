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
    return <Tag>已停用</Tag>;
  }
  return <Tag color="success">已启用</Tag>;
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
      title: "脚本",
      dataIndex: "name",
      key: "name",
      render: (value: string) => (
        <Typography.Text strong>{value}</Typography.Text>
      ),
    },
    {
      title: "状态",
      dataIndex: "enabled",
      key: "enabled",
      width: 120,
      render: (_, rule) => renderRuleStatus(rule),
    },
    {
      title: "更新时间",
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
      title: "操作",
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
              编辑
            </Button>
            <Popconfirm
              title="删除脚本"
              description="删除后需要重新保存脚本才能启用校验。"
              okText="删除"
              cancelText="取消"
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
                删除
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
          <Empty description="当前规则集还没有脚本" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ),
      }}
      scroll={{ x: 700 }}
    />
  );
}
