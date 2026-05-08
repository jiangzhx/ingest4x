import { Alert, Button, Select, Space, Tag, Typography } from "antd";
import type { ProjectRuleSetAssignment, RuleSet } from "./types";

type ProjectRuleSetsPanelProps = {
  ruleSets: RuleSet[];
  projectName?: string;
  projectId: number | null;
  assignments: ProjectRuleSetAssignment[];
  loading?: boolean;
  updatingRuleSetId?: number | null;
  onAssign: (ruleSetId: number) => Promise<void>;
  onUnassign: (ruleSetId: number) => Promise<void>;
};

export function ProjectRuleSetsPanel({
  ruleSets,
  projectName,
  projectId,
  assignments,
  loading = false,
  updatingRuleSetId = null,
  onAssign,
  onUnassign,
}: ProjectRuleSetsPanelProps) {
  const currentAssignment = assignments.find((assignment) => assignment.enabled);
  const ruleSetById = new Map(ruleSets.map((ruleSet) => [ruleSet.id, ruleSet]));
  const ruleSetOptions = ruleSets.map((ruleSet) => ({
    label: ruleSet.name,
    value: ruleSet.id,
  }));
  const currentRuleSetName =
    currentAssignment === undefined
      ? null
      : ruleSetById.get(currentAssignment.rule_set_id)?.name ??
        `规则集 #${currentAssignment.rule_set_id}`;

  return (
    <Space direction="vertical" size={12} style={{ display: "flex" }}>
      <Typography.Title level={4} style={{ margin: 0 }}>
        规则集绑定
      </Typography.Title>
      {projectId !== null ? (
        <Space.Compact style={{ width: "100%" }}>
          <Select
            placeholder="选择启用规则集"
            value={currentAssignment?.rule_set_id}
            options={ruleSetOptions}
            loading={loading}
            style={{ flex: 1 }}
            onChange={(ruleSetId) => {
              void onAssign(ruleSetId);
            }}
          />
          {currentAssignment ? (
            <Button
              danger
              loading={updatingRuleSetId === currentAssignment.rule_set_id}
              onClick={() => onUnassign(currentAssignment.rule_set_id)}
            >
              解绑
            </Button>
          ) : null}
        </Space.Compact>
      ) : (
        <Alert type="info" showIcon message="保存项目后即可绑定规则集" />
      )}
      {projectId !== null ? (
        <Typography.Text type="secondary">
          当前项目：{projectName ? `${projectName} (#${projectId})` : `#${projectId}`}
        </Typography.Text>
      ) : null}
      {currentRuleSetName ? (
        <Space size={8}>
          <Typography.Text>当前启用规则集：{currentRuleSetName}</Typography.Text>
          <Tag color="success">已启用</Tag>
        </Space>
      ) : (
        <Alert type="info" showIcon message="当前项目未绑定规则集" />
      )}
    </Space>
  );
}
