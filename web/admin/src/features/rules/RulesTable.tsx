import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { Rule } from "./types";
import { formatRuleTimestamp } from "./utils";

type RulesTableProps = {
  rules: Rule[];
  wildcardRuleId?: number | null;
  actionsDisabled?: boolean;
  deletingRuleId?: number | null;
  onEdit: (rule: Rule) => void;
  onDelete: (rule: Rule) => Promise<void>;
};

function buildParentName(rule: Rule, rules: Rule[]): string {
  if (rule.parent_id === null) {
    return "-";
  }

  return rules.find((candidate) => candidate.id === rule.parent_id)?.name ?? "-";
}

function renderRuleStatus(rule: Rule, wildcardRuleId: number | null) {
  if (!rule.enabled) {
    return <Tag>已停用</Tag>;
  }
  if (wildcardRuleId === rule.id) {
    return <Tag color="processing">通配规则</Tag>;
  }
  if (!rule.xwhat) {
    return <Tag color="blue">公共规则</Tag>;
  }
  return <Tag color="success">事件规则</Tag>;
}

export function RulesTable({
  rules,
  wildcardRuleId = null,
  actionsDisabled = false,
  deletingRuleId = null,
  onEdit,
  onDelete,
}: RulesTableProps) {
  const columns: ColumnsType<Rule> = [
    {
      title: "规则名称",
      dataIndex: "name",
      key: "name",
      render: (value: string, rule) => (
        <Space direction="vertical" size={2}>
          <Typography.Text strong>{value}</Typography.Text>
          <Typography.Text type="secondary">
            父规则：{buildParentName(rule, rules)}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: "事件名",
      dataIndex: "xwhat",
      key: "xwhat",
      width: 160,
      render: (value: string | null, rule) =>
        value ? (
          <Typography.Text code>{value}</Typography.Text>
        ) : wildcardRuleId === rule.id ? (
          <Tag>通配符</Tag>
        ) : (
          <Tag>父规则</Tag>
        ),
    },
    {
      title: "状态",
      dataIndex: "enabled",
      key: "enabled",
      width: 120,
      render: (_, rule) => renderRuleStatus(rule, wildcardRuleId),
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
              title="删除规则"
              description={`将删除规则 ${rule.name}，该操作不可恢复。`}
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
          <Empty description="当前规则集还没有规则" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ),
      }}
      scroll={{ x: 900 }}
    />
  );
}
