import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Tabs, Typography } from "antd";
import { DeliveryTargetFormModal } from "./DeliveryTargetFormModal";
import { DeliveryTargetsTable } from "./DeliveryTargetsTable";
import { EventSinkFormModal } from "./EventSinkFormModal";
import { EventSinksTable } from "./EventSinksTable";
import {
  useCreateDeliveryTargetMutation,
  useCreateEventSinkMutation,
  useDeleteDeliveryTargetMutation,
  useDeleteEventSinkMutation,
  useDeliveryTargetsQuery,
  useEventSinksQuery,
  useUpdateDeliveryTargetMutation,
  useUpdateEventSinkMutation,
} from "./hooks";
import type {
  DeliveryTarget,
  DeliveryTargetFormValues,
  EventSink,
  EventSinkFormValues,
} from "./types";
import {
  getErrorMessage,
  toCreateDeliveryTargetPayload,
  toCreateEventSinkPayload,
  toUpdateDeliveryTargetPayload,
  toUpdateEventSinkPayload,
} from "./utils";

export function SinksPage() {
  const { message } = App.useApp();
  const deliveryTargetsQuery = useDeliveryTargetsQuery();
  const eventSinksQuery = useEventSinksQuery();
  const targets = deliveryTargetsQuery.data ?? [];
  const sinks = eventSinksQuery.data ?? [];
  const createDeliveryTargetMutation = useCreateDeliveryTargetMutation();
  const updateDeliveryTargetMutation = useUpdateDeliveryTargetMutation();
  const deleteDeliveryTargetMutation = useDeleteDeliveryTargetMutation();
  const createEventSinkMutation = useCreateEventSinkMutation();
  const updateEventSinkMutation = useUpdateEventSinkMutation();
  const deleteEventSinkMutation = useDeleteEventSinkMutation();
  const [targetModalMode, setTargetModalMode] = useState<"create" | "edit">(
    "create",
  );
  const [sinkModalMode, setSinkModalMode] = useState<"create" | "edit">("create");
  const [editingTarget, setEditingTarget] = useState<DeliveryTarget | null>(null);
  const [editingSink, setEditingSink] = useState<EventSink | null>(null);
  const [isTargetModalOpen, setIsTargetModalOpen] = useState(false);
  const [isSinkModalOpen, setIsSinkModalOpen] = useState(false);
  const [deletingTargetId, setDeletingTargetId] = useState<number | null>(null);
  const [deletingSinkId, setDeletingSinkId] = useState<number | null>(null);
  const showInitialError =
    (deliveryTargetsQuery.isError && deliveryTargetsQuery.data === undefined) ||
    (eventSinksQuery.isError && eventSinksQuery.data === undefined);
  const isInitialLoading =
    deliveryTargetsQuery.isLoading || eventSinksQuery.isLoading;
  const targetFormError =
    targetModalMode === "create"
      ? createDeliveryTargetMutation.error
      : updateDeliveryTargetMutation.error;
  const sinkFormError =
    sinkModalMode === "create"
      ? createEventSinkMutation.error
      : updateEventSinkMutation.error;
  const isTargetSubmitting =
    createDeliveryTargetMutation.isPending ||
    updateDeliveryTargetMutation.isPending;
  const isSinkSubmitting =
    createEventSinkMutation.isPending || updateEventSinkMutation.isPending;

  const resetTargetMutationState = () => {
    createDeliveryTargetMutation.reset();
    updateDeliveryTargetMutation.reset();
  };

  const resetSinkMutationState = () => {
    createEventSinkMutation.reset();
    updateEventSinkMutation.reset();
  };

  const openCreateTargetModal = () => {
    resetTargetMutationState();
    setTargetModalMode("create");
    setEditingTarget(null);
    setIsTargetModalOpen(true);
  };

  const openEditTargetModal = (target: DeliveryTarget) => {
    resetTargetMutationState();
    setTargetModalMode("edit");
    setEditingTarget(target);
    setIsTargetModalOpen(true);
  };

  const openCreateSinkModal = () => {
    resetSinkMutationState();
    setSinkModalMode("create");
    setEditingSink(null);
    setIsSinkModalOpen(true);
  };

  const openEditSinkModal = (sink: EventSink) => {
    resetSinkMutationState();
    setSinkModalMode("edit");
    setEditingSink(sink);
    setIsSinkModalOpen(true);
  };

  const closeTargetModal = () => {
    if (isTargetSubmitting) {
      return;
    }

    resetTargetMutationState();
    setIsTargetModalOpen(false);
    setEditingTarget(null);
  };

  const closeSinkModal = () => {
    if (isSinkSubmitting) {
      return;
    }

    resetSinkMutationState();
    setIsSinkModalOpen(false);
    setEditingSink(null);
  };

  const refreshAll = () => {
    void deliveryTargetsQuery.refetch();
    void eventSinksQuery.refetch();
  };

  const handleTargetSubmit = async (values: DeliveryTargetFormValues) => {
    try {
      if (targetModalMode === "create") {
        await createDeliveryTargetMutation.mutateAsync(
          toCreateDeliveryTargetPayload(values),
        );
        message.success(`Delivery target ${values.target_id} 创建成功`);
      } else if (editingTarget) {
        await updateDeliveryTargetMutation.mutateAsync({
          id: editingTarget.id,
          payload: toUpdateDeliveryTargetPayload(values),
        });
        message.success(`Delivery target ${editingTarget.target_id} 保存成功`);
      }

      setIsTargetModalOpen(false);
      setEditingTarget(null);
    } catch (error) {
      message.error(getErrorMessage(error, "保存 delivery target 失败。"));
      throw error;
    }
  };

  const handleSinkSubmit = async (values: EventSinkFormValues) => {
    try {
      if (sinkModalMode === "create") {
        await createEventSinkMutation.mutateAsync(
          toCreateEventSinkPayload(values, targets),
        );
        message.success(`Event sink ${values.sink_id} 创建成功`);
      } else if (editingSink) {
        await updateEventSinkMutation.mutateAsync({
          id: editingSink.id,
          payload: toUpdateEventSinkPayload(values, targets),
        });
        message.success(`Event sink ${editingSink.sink_id} 保存成功`);
      }

      setIsSinkModalOpen(false);
      setEditingSink(null);
    } catch (error) {
      message.error(getErrorMessage(error, "保存 event sink 失败。"));
      throw error;
    }
  };

  const handleDeleteTarget = async (target: DeliveryTarget) => {
    setDeletingTargetId(target.id);
    try {
      await deleteDeliveryTargetMutation.mutateAsync(target.id);
      message.success(`Delivery target ${target.target_id} 删除成功`);
    } catch (error) {
      message.error(getErrorMessage(error, "删除 delivery target 失败。"));
    } finally {
      setDeletingTargetId(null);
    }
  };

  const handleDeleteSink = async (sink: EventSink) => {
    setDeletingSinkId(sink.id);
    try {
      await deleteEventSinkMutation.mutateAsync(sink.id);
      message.success(`Event sink ${sink.sink_id} 删除成功`);
    } catch (error) {
      message.error(getErrorMessage(error, "删除 event sink 失败。"));
    } finally {
      setDeletingSinkId(null);
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
            Sink 管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            管理投递连接资源和 Rhai emit 目标。
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            onClick={refreshAll}
            loading={deliveryTargetsQuery.isFetching || eventSinksQuery.isFetching}
          >
            刷新
          </Button>
          <Button type="primary" onClick={openCreateTargetModal}>
            新建 Target
          </Button>
          <Button
            type="primary"
            disabled={targets.length === 0}
            onClick={openCreateSinkModal}
          >
            新建 Sink
          </Button>
        </Space>
      </Space>

      {isInitialLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">正在加载 sink 配置...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Sink 配置加载失败"
          subTitle={getErrorMessage(
            deliveryTargetsQuery.error ?? eventSinksQuery.error,
          )}
          extra={
            <Button type="primary" onClick={refreshAll}>
              重试
            </Button>
          }
        />
      ) : null}

      {!isInitialLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`共 ${targets.length} 个 delivery target，${sinks.length} 个 event sink`}
          />
          {targetFormError ? (
            <Alert
              type="error"
              showIcon
              message="Delivery target 保存失败"
              description={getErrorMessage(targetFormError)}
            />
          ) : null}
          {sinkFormError ? (
            <Alert
              type="error"
              showIcon
              message="Event sink 保存失败"
              description={getErrorMessage(sinkFormError)}
            />
          ) : null}
          <Tabs
            items={[
              {
                key: "targets",
                label: "Delivery Targets",
                children: (
                  <DeliveryTargetsTable
                    targets={targets}
                    deletingTargetId={deletingTargetId}
                    actionsDisabled={deletingTargetId !== null}
                    onEdit={openEditTargetModal}
                    onDelete={handleDeleteTarget}
                  />
                ),
              },
              {
                key: "sinks",
                label: "Event Sinks",
                children: (
                  <EventSinksTable
                    sinks={sinks}
                    targets={targets}
                    deletingSinkId={deletingSinkId}
                    actionsDisabled={deletingSinkId !== null}
                    onEdit={openEditSinkModal}
                    onDelete={handleDeleteSink}
                  />
                ),
              },
            ]}
          />
        </Space>
      ) : null}

      <DeliveryTargetFormModal
        open={isTargetModalOpen}
        mode={targetModalMode}
        target={editingTarget}
        confirmLoading={isTargetSubmitting}
        onCancel={closeTargetModal}
        onSubmit={handleTargetSubmit}
      />
      <EventSinkFormModal
        open={isSinkModalOpen}
        mode={sinkModalMode}
        sink={editingSink}
        targets={targets}
        confirmLoading={isSinkSubmitting}
        onCancel={closeSinkModal}
        onSubmit={handleSinkSubmit}
      />
    </Space>
  );
}
