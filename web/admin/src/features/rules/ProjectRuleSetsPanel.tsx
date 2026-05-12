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
        `Rule set #${currentAssignment.rule_set_id}`;

  return (
    <Space direction="vertical" size={12} style={{ display: "flex" }}>
      <Typography.Title level={4} style={{ margin: 0 }}>
        Rule Set Binding
      </Typography.Title>
      {projectId !== null ? (
        <Space.Compact style={{ width: "100%" }}>
          <Select
            placeholder="Select enabled rule set"
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
              Unassign
            </Button>
          ) : null}
        </Space.Compact>
      ) : (
        <Alert type="info" showIcon message="Save the project before assigning a rule set." />
      )}
      {projectId !== null ? (
        <Typography.Text type="secondary">
          Current project: {projectName ? `${projectName} (#${projectId})` : `#${projectId}`}
        </Typography.Text>
      ) : null}
      {currentRuleSetName ? (
        <Space size={8}>
          <Typography.Text>Current enabled rule set: {currentRuleSetName}</Typography.Text>
          <Tag color="success">Enabled</Tag>
        </Space>
      ) : (
        <Alert type="info" showIcon message="Current project has no assigned rule set" />
      )}
    </Space>
  );
}
