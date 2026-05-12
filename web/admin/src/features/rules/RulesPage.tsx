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
    label: `${ruleSet.name}${ruleSet.enabled ? "" : " (disabled)"}`,
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
        message.success(`Rule set ${created.name} created`);
      } else if (editingRuleSet) {
        await updateRuleSetMutation.mutateAsync({
          ruleSetId: editingRuleSet.id,
          payload: toUpdateRuleSetPayload(values),
        });
        message.success(`Rule set ${editingRuleSet.name} saved`);
      }

      setIsRuleSetModalOpen(false);
      setEditingRuleSet(null);
    } catch (error) {
      message.error(
        getErrorMessage(error, "Failed to save the rule set, please try again later."),
      );
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
      message.success(`Rule set ${ruleSet.name} deleted`);
    } catch (error) {
      message.error(
        getErrorMessage(error, "Failed to delete the rule set, please try again later."),
      );
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
      message.success("Rhai validation script saved");
    } catch (error) {
      message.error(
        getErrorMessage(
          error,
          "Failed to save the Rhai validation script, please try again later.",
        ),
      );
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
            Rule Management
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            Manage rule sets and Rhai validation scripts.
          </Typography.Paragraph>
        </div>
      </Space>

      {ruleSetsQuery.isLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">Loading rule sets...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Failed to load rule sets"
          subTitle={getErrorMessage(ruleSetsQuery.error)}
          extra={
            <Button type="primary" onClick={() => void ruleSetsQuery.refetch()}>
              Retry
            </Button>
          }
        />
      ) : null}

      {!ruleSetsQuery.isLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`Total ${ruleSets.length} rule sets`}
          />
          <Space
            align="center"
            wrap
            style={{ justifyContent: "space-between", width: "100%" }}
          >
            <Space align="center" wrap>
              <Typography.Text strong>Rule Set</Typography.Text>
              <Select
                showSearch
                placeholder="Select rule set"
                value={selectedRuleSetId ?? undefined}
                options={ruleSetOptions}
                optionFilterProp="label"
                style={{ minWidth: 320 }}
                onChange={setSelectedRuleSetId}
              />
              {selectedRuleSet ? (
                selectedRuleSet.enabled ? (
                  <Tag color="success">Enabled</Tag>
                ) : (
                  <Tag>Disabled</Tag>
                )
              ) : null}
            </Space>
            <Space>
              <Button
                onClick={() => void ruleSetsQuery.refetch()}
                loading={ruleSetsQuery.isFetching}
              >
                Refresh
              </Button>
              <Button type="primary" onClick={openCreateRuleSetModal}>
                Create Rule Set
              </Button>
              <Button
                disabled={!selectedRuleSet || ruleSetActionsDisabled}
                onClick={() => {
                  if (selectedRuleSet) {
                    openEditRuleSetModal(selectedRuleSet);
                  }
                }}
              >
                Edit Rule Set
              </Button>
              <Popconfirm
                title="Delete rule set"
                description={
                  selectedRuleSet
                    ? `Rule set ${selectedRuleSet.name} will be deleted and cannot be undone.`
                    : "Please select a rule set"
                }
                okText="Delete"
                cancelText="Cancel"
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
                  Delete Rule Set
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
                Rhai Validation Script
              </Typography.Title>
              <Button
                type="primary"
                disabled={selectedRuleSetId === null}
                loading={isScriptSaving}
                onClick={() => void handleSaveScript()}
              >
                Save Script
              </Button>
            </Space>
            {rulesQuery.isFetching ? (
              <Typography.Text type="secondary">Syncing script...</Typography.Text>
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
