import { Button, Empty, Popconfirm, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { RuleSet } from "./types";
import { formatRuleTimestamp } from "./utils";

type RuleSetsTableProps = {
  ruleSets: RuleSet[];
  selectedRuleSetId: number | null;
  actionsDisabled?: boolean;
  deletingRuleSetId?: number | null;
  onSelect: (ruleSet: RuleSet) => void;
  onEdit: (ruleSet: RuleSet) => void;
  onDelete: (ruleSet: RuleSet) => Promise<void>;
};

export function RuleSetsTable({
  ruleSets,
  selectedRuleSetId,
  actionsDisabled = false,
  deletingRuleSetId = null,
  onSelect,
  onEdit,
  onDelete,
}: RuleSetsTableProps) {
  const columns: ColumnsType<RuleSet> = [
    {
      title: "规则集",
      dataIndex: "name",
      key: "name",
      render: (value: string, ruleSet) => (
        <Space direction="vertical" size={2}>
          <Typography.Text strong>{value}</Typography.Text>
          {ruleSet.description ? (
            <Typography.Text type="secondary">{ruleSet.description}</Typography.Text>
          ) : null}
        </Space>
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
          {formatRuleTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "操作",
      key: "actions",
      width: 220,
      render: (_, ruleSet) => {
        const isDeleting = deletingRuleSetId === ruleSet.id;

        return (
          <Space size={8}>
            <Button size="small" onClick={() => onSelect(ruleSet)}>
              查看
            </Button>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(ruleSet)}
            >
              编辑
            </Button>
            <Popconfirm
              title="删除规则集"
              description={`将删除规则集 ${ruleSet.name}，该操作不可恢复。`}
              okText="删除"
              cancelText="取消"
              disabled={actionsDisabled}
              okButtonProps={{ danger: true, loading: isDeleting }}
              onConfirm={() => onDelete(ruleSet)}
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
    <Table<RuleSet>
      rowKey="id"
      columns={columns}
      dataSource={ruleSets}
      pagination={false}
      rowClassName={(ruleSet) =>
        ruleSet.id === selectedRuleSetId ? "ant-table-row-selected" : ""
      }
      locale={{
        emptyText: (
          <Empty description="当前还没有规则集" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ),
      }}
      scroll={{ x: 860 }}
    />
  );
}
