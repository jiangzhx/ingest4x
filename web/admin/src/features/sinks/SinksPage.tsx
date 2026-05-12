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
  useSinkTypesQuery,
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
  const sinkTypesQuery = useSinkTypesQuery();
  const sinkTypes = sinkTypesQuery.data ?? [];
  const deliveryTargetsQuery = useDeliveryTargetsQuery(sinkTypes);
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
    (sinkTypesQuery.isError && sinkTypesQuery.data === undefined) ||
    (deliveryTargetsQuery.isError && deliveryTargetsQuery.data === undefined) ||
    (eventSinksQuery.isError && eventSinksQuery.data === undefined);
  const isInitialLoading =
    sinkTypesQuery.isLoading || deliveryTargetsQuery.isLoading || eventSinksQuery.isLoading;
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
    void sinkTypesQuery.refetch();
    void deliveryTargetsQuery.refetch();
    void eventSinksQuery.refetch();
  };

  const handleTargetSubmit = async (values: DeliveryTargetFormValues) => {
    try {
      if (targetModalMode === "create") {
        await createDeliveryTargetMutation.mutateAsync({
          payload: toCreateDeliveryTargetPayload(values),
          sinkTypes,
        });
        message.success(`Delivery target ${values.target_id} created`);
      } else if (editingTarget) {
        await updateDeliveryTargetMutation.mutateAsync({
          id: editingTarget.id,
          payload: toUpdateDeliveryTargetPayload(values),
          sinkTypes,
        });
        message.success(`Delivery target ${editingTarget.target_id} saved`);
      }

      setIsTargetModalOpen(false);
      setEditingTarget(null);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to save delivery target."));
      throw error;
    }
  };

  const handleSinkSubmit = async (values: EventSinkFormValues) => {
    try {
      if (sinkModalMode === "create") {
        await createEventSinkMutation.mutateAsync(toCreateEventSinkPayload(values));
        message.success(`Event sink ${values.sink_id} created`);
      } else if (editingSink) {
        await updateEventSinkMutation.mutateAsync({
          id: editingSink.id,
          payload: toUpdateEventSinkPayload(values),
        });
        message.success(`Event sink ${editingSink.sink_id} saved`);
      }

      setIsSinkModalOpen(false);
      setEditingSink(null);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to save event sink."));
      throw error;
    }
  };

  const handleDeleteTarget = async (target: DeliveryTarget) => {
    setDeletingTargetId(target.id);
    try {
      await deleteDeliveryTargetMutation.mutateAsync(target.id);
      message.success(`Delivery target ${target.target_id} deleted`);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to delete delivery target."));
    } finally {
      setDeletingTargetId(null);
    }
  };

  const handleDeleteSink = async (sink: EventSink) => {
    setDeletingSinkId(sink.id);
    try {
      await deleteEventSinkMutation.mutateAsync(sink.id);
      message.success(`Event sink ${sink.sink_id} deleted`);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to delete event sink."));
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
            Sink Management
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            Manage delivery target and event sink resources and Rhai emit mappings.
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            onClick={refreshAll}
            loading={
              sinkTypesQuery.isFetching ||
              deliveryTargetsQuery.isFetching ||
              eventSinksQuery.isFetching
            }
          >
            Refresh
          </Button>
          <Button type="primary" onClick={openCreateTargetModal}>
            New Target
          </Button>
          <Button
            type="primary"
            disabled={targets.length === 0}
            onClick={openCreateSinkModal}
          >
            New Sink
          </Button>
        </Space>
      </Space>

      {isInitialLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">Loading sink configuration...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Failed to load sink configuration"
          subTitle={getErrorMessage(
            sinkTypesQuery.error ?? deliveryTargetsQuery.error ?? eventSinksQuery.error,
          )}
          extra={
            <Button type="primary" onClick={refreshAll}>
              Retry
            </Button>
          }
        />
      ) : null}

      {!isInitialLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`Total ${targets.length} delivery targets, ${sinks.length} event sinks`}
          />
          {targetFormError ? (
            <Alert
              type="error"
              showIcon
              message="Failed to save delivery target"
              description={getErrorMessage(targetFormError)}
            />
          ) : null}
          {sinkFormError ? (
            <Alert
              type="error"
              showIcon
              message="Failed to save event sink"
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
                    sinkTypes={sinkTypes}
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
                    sinkTypes={sinkTypes}
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
        sinkTypes={sinkTypes}
        confirmLoading={isTargetSubmitting}
        onCancel={closeTargetModal}
        onSubmit={handleTargetSubmit}
      />
      <EventSinkFormModal
        open={isSinkModalOpen}
        mode={sinkModalMode}
        sink={editingSink}
        targets={targets}
        sinkTypes={sinkTypes}
        confirmLoading={isSinkSubmitting}
        onCancel={closeSinkModal}
        onSubmit={handleSinkSubmit}
      />
    </Space>
  );
}
