import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import {
  App,
  Alert,
  Button,
  Popconfirm,
  Result,
  Select,
  Skeleton,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import type { Rule, RuleSet, RuleSetFormValues } from "./types";
import {
  useRulesQuery,
  useRuleSetsQuery,
  useCreateRuleSetMutation,
  useDeleteRuleSetMutation,
  useSaveValidationRuleMutation,
  useUpdateRuleSetMutation,
} from "./hooks";
import { RuleSetFormModal } from "./RuleSetFormModal";
import {
  getErrorMessage,
  toCreateRuleSetPayload,
  toUpdateRuleSetPayload,
} from "./utils";

const RhaiEditor = lazy(() =>
  import("../processors/RhaiEditor").then((module) => ({
    default: module.RhaiEditor,
  })),
);

const EMPTY_RHAI_RULE_CONTENT = `fn validate(event) {
    event.result()
}
`;

type LazyRhaiEditorProps = {
  value?: string;
  onChange?: (value: string) => void;
};

function LazyRhaiEditor({ value, onChange }: LazyRhaiEditorProps) {
  return (
    <Suspense fallback={<Skeleton.Input block active style={{ height: 420 }} />}>
      <RhaiEditor value={value} onChange={onChange} height="420px" />
    </Suspense>
  );
}

function findValidationRule(
  rules: Rule[],
  selectedRuleSet: RuleSet | null,
): Rule | null {
  return (
    rules.find((rule) => rule.id === selectedRuleSet?.wildcard_rule_id) ??
    rules.find((rule) => rule.content.includes("fn validate")) ??
    rules[0] ??
    null
  );
}

export function RulesPage() {
  const { message } = App.useApp();
  const ruleSetsQuery = useRuleSetsQuery();
  const ruleSets = ruleSetsQuery.data ?? [];
  const [selectedRuleSetId, setSelectedRuleSetId] = useState<number | null>(null);
  const rulesQuery = useRulesQuery(selectedRuleSetId);
  const createRuleSetMutation = useCreateRuleSetMutation();
  const updateRuleSetMutation = useUpdateRuleSetMutation();
  const deleteRuleSetMutation = useDeleteRuleSetMutation();
  const saveValidationRuleMutation = useSaveValidationRuleMutation(selectedRuleSetId);
  const [ruleSetModalMode, setRuleSetModalMode] = useState<"create" | "edit">(
    "create",
  );
  const [editingRuleSet, setEditingRuleSet] = useState<RuleSet | null>(null);
  const [isRuleSetModalOpen, setIsRuleSetModalOpen] = useState(false);
  const [scriptContent, setScriptContent] = useState(EMPTY_RHAI_RULE_CONTENT);
  const [deletingRuleSetId, setDeletingRuleSetId] = useState<number | null>(null);

  useEffect(() => {
    if (selectedRuleSetId !== null || ruleSets.length === 0) {
      return;
    }

    setSelectedRuleSetId(ruleSets[0].id);
  }, [ruleSets, selectedRuleSetId]);

  const selectedRuleSet =
    ruleSets.find((ruleSet) => ruleSet.id === selectedRuleSetId) ?? null;
  const rules = rulesQuery.data ?? [];
  const validationRule = useMemo(
    () => findValidationRule(rules, selectedRuleSet),
    [rules, selectedRuleSet],
  );
  const ruleSetOptions = ruleSets.map((ruleSet) => ({
    label: `${ruleSet.name}${ruleSet.enabled ? "" : "（已停用）"}`,
    value: ruleSet.id,
  }));
  const isRuleSetSubmitting =
    createRuleSetMutation.isPending || updateRuleSetMutation.isPending;
  const isScriptSaving = saveValidationRuleMutation.isPending;
  const ruleSetActionsDisabled = deletingRuleSetId !== null || isScriptSaving;

  useEffect(() => {
    setScriptContent(validationRule?.content ?? EMPTY_RHAI_RULE_CONTENT);
  }, [selectedRuleSetId, validationRule?.id, validationRule?.content]);

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

  const closeRuleSetModal = () => {
    if (isRuleSetSubmitting) {
      return;
    }

    setIsRuleSetModalOpen(false);
    setEditingRuleSet(null);
  };

  const handleRuleSetSubmit = async (values: RuleSetFormValues) => {
    try {
      if (ruleSetModalMode === "create") {
        const created = await createRuleSetMutation.mutateAsync(
          toCreateRuleSetPayload(values),
        );
        setSelectedRuleSetId(created.id);
        setScriptContent(EMPTY_RHAI_RULE_CONTENT);
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

  const handleDeleteRuleSet = async (ruleSet: RuleSet) => {
    setDeletingRuleSetId(ruleSet.id);
    try {
      await deleteRuleSetMutation.mutateAsync(ruleSet.id);
      if (selectedRuleSetId === ruleSet.id) {
        setSelectedRuleSetId(null);
        setScriptContent(EMPTY_RHAI_RULE_CONTENT);
      }
      message.success(`规则集 ${ruleSet.name} 删除成功`);
    } catch (error) {
      message.error(getErrorMessage(error, "删除规则集失败，请稍后重试。"));
    } finally {
      setDeletingRuleSetId(null);
    }
  };

  const handleSaveScript = async () => {
    if (selectedRuleSetId === null) {
      return;
    }

    try {
      await saveValidationRuleMutation.mutateAsync({
        content: scriptContent.trim(),
        enabled: true,
      });
      message.success("Rhai 校验脚本保存成功");
    } catch (error) {
      message.error(getErrorMessage(error, "保存 Rhai 校验脚本失败，请稍后重试。"));
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
            管理规则集和 Rhai 校验脚本。
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
          <Alert type="info" showIcon message={`共 ${ruleSets.length} 个规则集`} />
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

          <Space direction="vertical" size={12} style={{ display: "flex" }}>
            <Space
              align="center"
              style={{ justifyContent: "space-between", width: "100%" }}
            >
              <Typography.Title level={4} style={{ margin: 0 }}>
                Rhai 校验脚本
              </Typography.Title>
              <Button
                type="primary"
                disabled={selectedRuleSetId === null}
                loading={isScriptSaving}
                onClick={() => void handleSaveScript()}
              >
                保存脚本
              </Button>
            </Space>
            {rulesQuery.isFetching ? (
              <Typography.Text type="secondary">正在同步脚本...</Typography.Text>
            ) : null}
            <LazyRhaiEditor value={scriptContent} onChange={setScriptContent} />
          </Space>
        </Space>
      ) : null}

      <RuleSetFormModal
        open={isRuleSetModalOpen}
        mode={ruleSetModalMode}
        ruleSet={editingRuleSet}
        confirmLoading={isRuleSetSubmitting}
        onCancel={closeRuleSetModal}
        onSubmit={handleRuleSetSubmit}
      />
    </Space>
  );
}
