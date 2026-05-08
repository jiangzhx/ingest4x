import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { ProcessorScriptDetailModal } from "./ProcessorScriptDetailModal";
import { ProcessorScriptFormModal } from "./ProcessorScriptFormModal";
import { ProcessorScriptsTable } from "./ProcessorScriptsTable";
import {
  useCreateProcessorScriptMutation,
  useProcessorScriptDetailQuery,
  useProcessorScriptsQuery,
  useUpdateProcessorScriptMutation,
  useUpdateProcessorScriptStatusMutation,
  useValidateProcessorScriptMutation,
} from "./hooks";
import type { ProcessorScript, ProcessorScriptFormValues } from "./types";
import {
  getErrorMessage,
  toCreateProcessorScriptPayload,
  toProcessorScriptFormValues,
  toUpdateProcessorScriptPayload,
  toValidateProcessorScriptPayload,
} from "./utils";

export function ProcessorsPage() {
  const { message } = App.useApp();
  const scriptsQuery = useProcessorScriptsQuery();
  const createScriptMutation = useCreateProcessorScriptMutation();
  const updateScriptMutation = useUpdateProcessorScriptMutation();
  const validateScriptMutation = useValidateProcessorScriptMutation();
  const updateStatusMutation = useUpdateProcessorScriptStatusMutation();
  const scripts = scriptsQuery.data ?? [];
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [viewingScript, setViewingScript] = useState<ProcessorScript | null>(null);
  const [editingScript, setEditingScript] = useState<ProcessorScript | null>(null);
  const [updatingScriptId, setUpdatingScriptId] = useState<number | null>(null);
  const [scriptValidationError, setScriptValidationError] = useState<string | null>(
    null,
  );
  const selectedScript = viewingScript ?? editingScript;
  const detailQuery = useProcessorScriptDetailQuery(selectedScript?.id ?? null);
  const editingDetail =
    editingScript !== null && detailQuery.data?.id === editingScript.id
      ? detailQuery.data
      : null;
  const isInitialLoading = scriptsQuery.isLoading;
  const showInitialError = scriptsQuery.isError && scriptsQuery.data === undefined;

  const refreshAll = () => {
    void scriptsQuery.refetch();
  };

  const handleCreateScript = async (values: ProcessorScriptFormValues) => {
    try {
      await createScriptMutation.mutateAsync(toCreateProcessorScriptPayload(values));
      message.success(`Processor script ${values.script_key} 创建成功`);
      setScriptValidationError(null);
      setIsCreateOpen(false);
    } catch (error) {
      message.error(getErrorMessage(error, "创建 processor script 失败。"));
      throw error;
    }
  };

  const handleValidateScript = async (
    values: ProcessorScriptFormValues,
    options: { notify?: boolean } = {},
  ) => {
    setScriptValidationError(null);
    try {
      await validateScriptMutation.mutateAsync(
        toValidateProcessorScriptPayload(values),
      );
      if (options.notify) {
        message.success("Rhai 脚本语法校验通过");
      }
    } catch (error) {
      setScriptValidationError(
        getErrorMessage(error, "Rhai 脚本语法校验失败。"),
      );
      throw error;
    }
  };

  const handleUpdateScript = async (values: ProcessorScriptFormValues) => {
    if (editingScript === null) {
      return;
    }

    try {
      const updated = await updateScriptMutation.mutateAsync({
        id: editingScript.id,
        payload: toUpdateProcessorScriptPayload(values),
      });
      message.success(
        `Processor script ${editingScript.script_key} 已更新到 v${updated.version}`,
      );
      setScriptValidationError(null);
      setEditingScript(null);
    } catch (error) {
      message.error(getErrorMessage(error, "更新 processor script 失败。"));
      throw error;
    }
  };

  const handleStatusChange = async (script: ProcessorScript) => {
    const nextStatus = script.status === "active" ? "archived" : "active";
    setUpdatingScriptId(script.id);
    try {
      await updateStatusMutation.mutateAsync({
        id: script.id,
        payload: { status: nextStatus },
      });
      message.success(
        `Processor script ${script.script_key} 已${nextStatus === "active" ? "启用" : "禁用"}`,
      );
    } catch (error) {
      message.error(getErrorMessage(error, "更新 processor 状态失败。"));
    } finally {
      setUpdatingScriptId(null);
    }
  };

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Processor 管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            管理 Rhai 脚本版本和项目绑定关系。
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            onClick={refreshAll}
            loading={scriptsQuery.isFetching}
          >
            刷新
          </Button>
          <Button
            type="primary"
            onClick={() => {
              setScriptValidationError(null);
              setIsCreateOpen(true);
            }}
          >
            新建 Script
          </Button>
        </Space>
      </Space>

      {isInitialLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">
              正在加载 processor 配置...
            </Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Processor 配置加载失败"
          subTitle={getErrorMessage(scriptsQuery.error)}
          extra={
            <Button type="primary" onClick={refreshAll}>
              重试
            </Button>
          }
        />
      ) : null}

      {!isInitialLoading && !showInitialError ? (
        <>
          {scriptsQuery.isError ? (
            <Alert
              showIcon
              type="warning"
              message="部分数据刷新失败"
              description={getErrorMessage(scriptsQuery.error)}
            />
          ) : null}

          <ProcessorScriptsTable
            scripts={scripts}
            updatingScriptId={updatingScriptId}
            onView={(script) => setViewingScript(script)}
            onEdit={(script) => {
              setScriptValidationError(null);
              setViewingScript(null);
              setEditingScript(script);
            }}
            onStatusChange={handleStatusChange}
          />
        </>
      ) : null}

      {createScriptMutation.isError ? (
        <Alert
          showIcon
          type="error"
          message="创建失败"
          description={getErrorMessage(createScriptMutation.error)}
        />
      ) : null}

      {updateScriptMutation.isError ? (
        <Alert
          showIcon
          type="error"
          message="更新失败"
          description={getErrorMessage(updateScriptMutation.error)}
        />
      ) : null}

      <ProcessorScriptFormModal
        open={isCreateOpen || editingScript !== null}
        mode={editingScript === null ? "create" : "edit"}
        initialValues={
          editingDetail === null
            ? undefined
            : toProcessorScriptFormValues(editingDetail)
        }
        confirmLoading={
          createScriptMutation.isPending || updateScriptMutation.isPending
        }
        validateLoading={validateScriptMutation.isPending}
        validationError={scriptValidationError}
        loading={editingScript !== null && detailQuery.isLoading}
        onCancel={() => {
          if (
            !createScriptMutation.isPending &&
            !updateScriptMutation.isPending &&
            !validateScriptMutation.isPending
          ) {
            createScriptMutation.reset();
            updateScriptMutation.reset();
            validateScriptMutation.reset();
            setScriptValidationError(null);
            setIsCreateOpen(false);
            setEditingScript(null);
          }
        }}
        onValidate={handleValidateScript}
        onSubmit={editingScript === null ? handleCreateScript : handleUpdateScript}
      />
      <ProcessorScriptDetailModal
        open={viewingScript !== null}
        detail={detailQuery.data ?? null}
        loading={detailQuery.isLoading}
        onCancel={() => setViewingScript(null)}
      />
    </Space>
  );
}
