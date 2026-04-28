import { useEffect, useState } from "react";
import {
  App,
  Alert,
  Button,
  Popconfirm,
  Result,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import type { Rule, RuleFormValues, RuleSet, RuleSetFormValues } from "./types";
import {
  useCreateRuleMutation,
  useCreateRuleSetMutation,
  useDeleteRuleMutation,
  useDeleteRuleSetMutation,
  useRulesQuery,
  useRuleSetsQuery,
  useUpdateRuleMutation,
  useUpdateRuleSetMutation,
} from "./hooks";
import { RuleFormModal } from "./RuleFormModal";
import { RuleSetFormModal } from "./RuleSetFormModal";
import { RulesTable } from "./RulesTable";
import {
  getErrorMessage,
  toCreateRulePayload,
  toCreateRuleSetPayload,
  toUpdateRulePayload,
  toUpdateRuleSetPayload,
} from "./utils";

export function RulesPage() {
  const { message } = App.useApp();
  const ruleSetsQuery = useRuleSetsQuery();
  const ruleSets = ruleSetsQuery.data ?? [];
  const [selectedRuleSetId, setSelectedRuleSetId] = useState<number | null>(null);
  const rulesQuery = useRulesQuery(selectedRuleSetId);
  const createRuleSetMutation = useCreateRuleSetMutation();
  const updateRuleSetMutation = useUpdateRuleSetMutation();
  const deleteRuleSetMutation = useDeleteRuleSetMutation();
  const createRuleMutation = useCreateRuleMutation(selectedRuleSetId);
  const updateRuleMutation = useUpdateRuleMutation(selectedRuleSetId);
  const deleteRuleMutation = useDeleteRuleMutation(selectedRuleSetId);
  const [ruleSetModalMode, setRuleSetModalMode] = useState<"create" | "edit">(
    "create",
  );
  const [editingRuleSet, setEditingRuleSet] = useState<RuleSet | null>(null);
  const [isRuleSetModalOpen, setIsRuleSetModalOpen] = useState(false);
  const [ruleModalMode, setRuleModalMode] = useState<"create" | "edit">("create");
  const [editingRule, setEditingRule] = useState<Rule | null>(null);
  const [isRuleModalOpen, setIsRuleModalOpen] = useState(false);
  const [deletingRuleSetId, setDeletingRuleSetId] = useState<number | null>(null);
  const [deletingRuleId, setDeletingRuleId] = useState<number | null>(null);

  useEffect(() => {
    if (selectedRuleSetId !== null || ruleSets.length === 0) {
      return;
    }

    setSelectedRuleSetId(ruleSets[0].id);
  }, [ruleSets, selectedRuleSetId]);

  const selectedRuleSet =
    ruleSets.find((ruleSet) => ruleSet.id === selectedRuleSetId) ?? null;
  const ruleSetOptions = ruleSets.map((ruleSet) => ({
    label: `${ruleSet.name}${ruleSet.enabled ? "" : "（已停用）"}`,
    value: ruleSet.id,
  }));
  const rules = rulesQuery.data ?? [];
  const isRuleSetSubmitting =
    createRuleSetMutation.isPending || updateRuleSetMutation.isPending;
  const isRuleSubmitting = createRuleMutation.isPending || updateRuleMutation.isPending;
  const ruleSetActionsDisabled = deletingRuleSetId !== null;
  const ruleActionsDisabled = deletingRuleId !== null || selectedRuleSetId === null;

  const openCreateRuleSetModal = () => {
    createRuleSetMutation.reset();
    updateRuleSetMutation.reset();
    setRuleSetModalMode("create");
    setEditingRuleSet(null);
    setIsRuleSetModalOpen(true);
  };

  const openEditRuleSetModal = (ruleSet: RuleSet) => {
    createRuleSetMutation.reset();
    updateRuleSetMutation.reset();
    setRuleSetModalMode("edit");
    setEditingRuleSet(ruleSet);
    setIsRuleSetModalOpen(true);
  };

  const openCreateRuleModal = () => {
    if (selectedRuleSetId === null) {
      return;
    }

    createRuleMutation.reset();
    updateRuleMutation.reset();
    setRuleModalMode("create");
    setEditingRule(null);
    setIsRuleModalOpen(true);
  };

  const openEditRuleModal = (rule: Rule) => {
    createRuleMutation.reset();
    updateRuleMutation.reset();
    setRuleModalMode("edit");
    setEditingRule(rule);
    setIsRuleModalOpen(true);
  };

  const closeRuleSetModal = () => {
    if (isRuleSetSubmitting) {
      return;
    }

    setIsRuleSetModalOpen(false);
    setEditingRuleSet(null);
  };

  const closeRuleModal = () => {
    if (isRuleSubmitting) {
      return;
    }

    setIsRuleModalOpen(false);
    setEditingRule(null);
  };

  const handleRuleSetSubmit = async (values: RuleSetFormValues) => {
    try {
      if (ruleSetModalMode === "create") {
        const created = await createRuleSetMutation.mutateAsync(
          toCreateRuleSetPayload(values),
        );
        setSelectedRuleSetId(created.id);
        message.success(`规则集 ${created.name} 创建成功`);
      } else if (editingRuleSet) {
        await updateRuleSetMutation.mutateAsync({
          ruleSetId: editingRuleSet.id,
          payload: toUpdateRuleSetPayload(values),
        });
        message.success(`规则集 ${editingRuleSet.name} 保存成功`);
      }

      setIsRuleSetModalOpen(false);
      setEditingRuleSet(null);
    } catch (error) {
      message.error(getErrorMessage(error, "保存规则集失败，请稍后重试。"));
      throw error;
    }
  };

  const handleRuleSubmit = async (values: RuleFormValues) => {
    try {
      if (ruleModalMode === "create") {
        await createRuleMutation.mutateAsync(toCreateRulePayload(values));
        message.success(`规则 ${values.name} 创建成功`);
      } else if (editingRule) {
        await updateRuleMutation.mutateAsync({
          ruleId: editingRule.id,
          payload: toUpdateRulePayload(values),
        });
        message.success(`规则 ${editingRule.name} 保存成功`);
      }

      setIsRuleModalOpen(false);
      setEditingRule(null);
    } catch (error) {
      message.error(getErrorMessage(error, "保存规则失败，请稍后重试。"));
      throw error;
    }
  };

  const handleDeleteRuleSet = async (ruleSet: RuleSet) => {
    setDeletingRuleSetId(ruleSet.id);
    try {
      await deleteRuleSetMutation.mutateAsync(ruleSet.id);
      if (selectedRuleSetId === ruleSet.id) {
        setSelectedRuleSetId(null);
      }
      message.success(`规则集 ${ruleSet.name} 删除成功`);
    } catch (error) {
      message.error(getErrorMessage(error, "删除规则集失败，请稍后重试。"));
    } finally {
      setDeletingRuleSetId(null);
    }
  };

  const handleDeleteRule = async (rule: Rule) => {
    setDeletingRuleId(rule.id);
    try {
      await deleteRuleMutation.mutateAsync(rule.id);
      message.success(`规则 ${rule.name} 删除成功`);
    } catch (error) {
      message.error(getErrorMessage(error, "删除规则失败，请稍后重试。"));
    } finally {
      setDeletingRuleId(null);
    }
  };

  const showInitialError = ruleSetsQuery.isError && ruleSetsQuery.data === undefined;

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            规则管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            管理规则集和规则继承树。
          </Typography.Paragraph>
        </div>
      </Space>

      {ruleSetsQuery.isLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">正在加载规则集...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="规则集加载失败"
          subTitle={getErrorMessage(ruleSetsQuery.error)}
          extra={
            <Button type="primary" onClick={() => void ruleSetsQuery.refetch()}>
              重试
            </Button>
          }
        />
      ) : null}

      {!ruleSetsQuery.isLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`共 ${ruleSets.length} 个规则集`}
          />
          <Space
            align="center"
            wrap
            style={{ justifyContent: "space-between", width: "100%" }}
          >
            <Space align="center" wrap>
              <Typography.Text strong>规则集</Typography.Text>
              <Select
                showSearch
                placeholder="选择规则集"
                value={selectedRuleSetId ?? undefined}
                options={ruleSetOptions}
                optionFilterProp="label"
                style={{ minWidth: 320 }}
                onChange={setSelectedRuleSetId}
              />
              {selectedRuleSet ? (
                selectedRuleSet.enabled ? (
                  <Tag color="success">已启用</Tag>
                ) : (
                  <Tag>已停用</Tag>
                )
              ) : null}
            </Space>
            <Space>
              <Button
                onClick={() => void ruleSetsQuery.refetch()}
                loading={ruleSetsQuery.isFetching}
              >
                刷新
              </Button>
              <Button type="primary" onClick={openCreateRuleSetModal}>
                新建规则集
              </Button>
              <Button
                disabled={!selectedRuleSet || ruleSetActionsDisabled}
                onClick={() => {
                  if (selectedRuleSet) {
                    openEditRuleSetModal(selectedRuleSet);
                  }
                }}
              >
                编辑规则集
              </Button>
              <Popconfirm
                title="删除规则集"
                description={
                  selectedRuleSet
                    ? `将删除规则集 ${selectedRuleSet.name}，该操作不可恢复。`
                    : "请选择规则集"
                }
                okText="删除"
                cancelText="取消"
                disabled={!selectedRuleSet || ruleSetActionsDisabled}
                okButtonProps={{
                  danger: true,
                  loading: selectedRuleSet
                    ? deletingRuleSetId === selectedRuleSet.id
                    : false,
                }}
                onConfirm={() => {
                  if (selectedRuleSet) {
                    void handleDeleteRuleSet(selectedRuleSet);
                  }
                }}
              >
                <Button
                  danger
                  disabled={!selectedRuleSet || ruleSetActionsDisabled}
                  loading={
                    selectedRuleSet
                      ? deletingRuleSetId === selectedRuleSet.id
                      : false
                  }
                >
                  删除规则集
                </Button>
              </Popconfirm>
            </Space>
          </Space>
          <Space
            align="start"
            style={{ justifyContent: "space-between", width: "100%" }}
          >
            <div>
              <Typography.Title level={4} style={{ margin: 0 }}>
                {selectedRuleSet ? selectedRuleSet.name : "规则"}
              </Typography.Title>
              <Typography.Text type="secondary">
                规则会沿父节点向上继承并合并。
              </Typography.Text>
            </div>
            <Button
              type="primary"
              disabled={selectedRuleSetId === null}
              onClick={openCreateRuleModal}
            >
              新建规则
            </Button>
          </Space>
          <RulesTable
            rules={rules}
            wildcardRuleId={selectedRuleSet?.wildcard_rule_id ?? null}
            actionsDisabled={ruleActionsDisabled}
            deletingRuleId={deletingRuleId}
            onEdit={openEditRuleModal}
            onDelete={handleDeleteRule}
          />
        </Space>
      ) : null}

      <RuleSetFormModal
        open={isRuleSetModalOpen}
        mode={ruleSetModalMode}
        ruleSet={editingRuleSet}
        rules={rules}
        confirmLoading={isRuleSetSubmitting}
        onCancel={closeRuleSetModal}
        onSubmit={handleRuleSetSubmit}
      />
      <RuleFormModal
        open={isRuleModalOpen}
        mode={ruleModalMode}
        rule={editingRule}
        rules={rules}
        confirmLoading={isRuleSubmitting}
        onCancel={closeRuleModal}
        onSubmit={handleRuleSubmit}
      />
    </Space>
  );
}
