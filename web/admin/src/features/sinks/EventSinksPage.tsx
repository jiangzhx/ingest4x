import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { EventSinkFormModal } from "./EventSinkFormModal";
import { EventSinksTable } from "./EventSinksTable";
import {
  useCreateEventSinkMutation,
  useDeleteEventSinkMutation,
  useDeliveryTargetsQuery,
  useEventSinksQuery,
  useSinkTypesQuery,
  useUpdateEventSinkMutation,
} from "./hooks";
import type { EventSink, EventSinkFormValues } from "./types";
import {
  getErrorMessage,
  toCreateEventSinkPayload,
  toUpdateEventSinkPayload,
} from "./utils";

export function EventSinksPage() {
  const { message } = App.useApp();
  const sinkTypesQuery = useSinkTypesQuery();
  const sinkTypes = sinkTypesQuery.data ?? [];
  const deliveryTargetsQuery = useDeliveryTargetsQuery(sinkTypes);
  const eventSinksQuery = useEventSinksQuery();
  const targets = deliveryTargetsQuery.data ?? [];
  const sinks = eventSinksQuery.data ?? [];
  const createEventSinkMutation = useCreateEventSinkMutation();
  const updateEventSinkMutation = useUpdateEventSinkMutation();
  const deleteEventSinkMutation = useDeleteEventSinkMutation();
  const [modalMode, setModalMode] = useState<"create" | "edit">("create");
  const [editingSink, setEditingSink] = useState<EventSink | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deletingSinkId, setDeletingSinkId] = useState<number | null>(null);
  const showInitialError =
    (sinkTypesQuery.isError && sinkTypesQuery.data === undefined) ||
    (deliveryTargetsQuery.isError && deliveryTargetsQuery.data === undefined) ||
    (eventSinksQuery.isError && eventSinksQuery.data === undefined);
  const isInitialLoading =
    sinkTypesQuery.isLoading || deliveryTargetsQuery.isLoading || eventSinksQuery.isLoading;
  const formError =
    modalMode === "create"
      ? createEventSinkMutation.error
      : updateEventSinkMutation.error;
  const isSubmitting =
    createEventSinkMutation.isPending || updateEventSinkMutation.isPending;

  const resetMutationState = () => {
    createEventSinkMutation.reset();
    updateEventSinkMutation.reset();
  };

  const openCreateModal = () => {
    resetMutationState();
    setModalMode("create");
    setEditingSink(null);
    setIsModalOpen(true);
  };

  const openEditModal = (sink: EventSink) => {
    resetMutationState();
    setModalMode("edit");
    setEditingSink(sink);
    setIsModalOpen(true);
  };

  const closeModal = () => {
    if (isSubmitting) {
      return;
    }

    resetMutationState();
    setIsModalOpen(false);
    setEditingSink(null);
  };

  const refreshAll = () => {
    void sinkTypesQuery.refetch();
    void deliveryTargetsQuery.refetch();
    void eventSinksQuery.refetch();
  };

  const handleSubmit = async (values: EventSinkFormValues) => {
    try {
      if (modalMode === "create") {
        await createEventSinkMutation.mutateAsync(toCreateEventSinkPayload(values));
        message.success(`Event sink ${values.sink_id} created`);
      } else if (editingSink) {
        await updateEventSinkMutation.mutateAsync({
          id: editingSink.id,
          payload: toUpdateEventSinkPayload(values),
        });
        message.success(`Event sink ${editingSink.sink_id} saved`);
      }

      setIsModalOpen(false);
      setEditingSink(null);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to save event sink."));
      throw error;
    }
  };

  const handleDelete = async (sink: EventSink) => {
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
            Event Sinks
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            Manage sink resources, emit destinations, and delivery target bindings.
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
          <Button
            type="primary"
            disabled={targets.length === 0}
            onClick={openCreateModal}
          >
            New Sink
          </Button>
        </Space>
      </Space>

      {isInitialLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">Loading event sinks...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Failed to load event sinks"
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
            message={`Total ${sinks.length} event sinks across ${targets.length} delivery targets`}
          />
          {formError ? (
            <Alert
              type="error"
              showIcon
              message="Failed to save event sink"
              description={getErrorMessage(formError)}
            />
          ) : null}
          <EventSinksTable
            sinks={sinks}
            targets={targets}
            sinkTypes={sinkTypes}
            deletingSinkId={deletingSinkId}
            actionsDisabled={deletingSinkId !== null}
            onEdit={openEditModal}
            onDelete={handleDelete}
          />
        </Space>
      ) : null}

      <EventSinkFormModal
        open={isModalOpen}
        mode={modalMode}
        sink={editingSink}
        targets={targets}
        sinkTypes={sinkTypes}
        confirmLoading={isSubmitting}
        onCancel={closeModal}
        onSubmit={handleSubmit}
      />
    </Space>
  );
}
