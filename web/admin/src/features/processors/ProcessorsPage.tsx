import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { ProcessorScriptDetailModal } from "./ProcessorScriptDetailModal";
import { ProcessorScriptFormModal } from "./ProcessorScriptFormModal";
import { ProcessorScriptsTable } from "./ProcessorScriptsTable";
import {
  useCreateProcessorScriptMutation,
  useProcessorScriptDetailQuery,
  useProcessorScriptsQuery,
  useUpdateProcessorScriptStatusMutation,
} from "./hooks";
import type { ProcessorScript, ProcessorScriptFormValues } from "./types";
import { getErrorMessage, toCreateProcessorScriptPayload } from "./utils";

export function ProcessorsPage() {
  const { message } = App.useApp();
  const scriptsQuery = useProcessorScriptsQuery();
  const createScriptMutation = useCreateProcessorScriptMutation();
  const updateStatusMutation = useUpdateProcessorScriptStatusMutation();
  const scripts = scriptsQuery.data ?? [];
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [viewingScript, setViewingScript] = useState<ProcessorScript | null>(null);
  const [updatingScriptId, setUpdatingScriptId] = useState<number | null>(null);
  const detailQuery = useProcessorScriptDetailQuery(viewingScript?.id ?? null);
  const isInitialLoading = scriptsQuery.isLoading;
  const showInitialError = scriptsQuery.isError && scriptsQuery.data === undefined;

  const refreshAll = () => {
    void scriptsQuery.refetch();
  };

  const handleCreateScript = async (values: ProcessorScriptFormValues) => {
    try {
      await createScriptMutation.mutateAsync(toCreateProcessorScriptPayload(values));
      message.success(`Processor script ${values.script_key} 创建成功`);
      setIsCreateOpen(false);
    } catch (error) {
      message.error(getErrorMessage(error, "创建 processor script 失败。"));
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
          <Button type="primary" onClick={() => setIsCreateOpen(true)}>
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

      <ProcessorScriptFormModal
        open={isCreateOpen}
        confirmLoading={createScriptMutation.isPending}
        onCancel={() => {
          if (!createScriptMutation.isPending) {
            createScriptMutation.reset();
            setIsCreateOpen(false);
          }
        }}
        onSubmit={handleCreateScript}
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
