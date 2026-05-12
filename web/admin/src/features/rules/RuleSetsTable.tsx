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
      title: "Rule Set",
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
          {formatRuleTimestamp(value)}
        </Typography.Text>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      width: 220,
      render: (_, ruleSet) => {
        const isDeleting = deletingRuleSetId === ruleSet.id;

        return (
          <Space size={8}>
            <Button size="small" onClick={() => onSelect(ruleSet)}>
              View
            </Button>
            <Button
              size="small"
              disabled={actionsDisabled}
              onClick={() => onEdit(ruleSet)}
            >
              Edit
            </Button>
            <Popconfirm
              title="Delete rule set"
              description={`Rule set ${ruleSet.name} will be deleted and cannot be undone.`}
              okText="Delete"
              cancelText="Cancel"
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
                Delete
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
            <Empty
              description="No rule sets are configured yet"
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        ),
      }}
      scroll={{ x: 860 }}
    />
  );
}
