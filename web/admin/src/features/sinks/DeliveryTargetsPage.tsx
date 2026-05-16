import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { DeliveryTargetFormModal } from "./DeliveryTargetFormModal";
import { DeliveryTargetsTable } from "./DeliveryTargetsTable";
import {
  useCreateDeliveryTargetMutation,
  useDeleteDeliveryTargetMutation,
  useDeliveryTargetsQuery,
  useSinkTypesQuery,
  useUpdateDeliveryTargetMutation,
} from "./hooks";
import type { DeliveryTarget, DeliveryTargetFormValues } from "./types";
import {
  getErrorMessage,
  toCreateDeliveryTargetPayload,
  toUpdateDeliveryTargetPayload,
} from "./utils";

export function DeliveryTargetsPage() {
  const { message } = App.useApp();
  const sinkTypesQuery = useSinkTypesQuery();
  const sinkTypes = sinkTypesQuery.data ?? [];
  const deliveryTargetsQuery = useDeliveryTargetsQuery(sinkTypes);
  const targets = deliveryTargetsQuery.data ?? [];
  const createDeliveryTargetMutation = useCreateDeliveryTargetMutation();
  const updateDeliveryTargetMutation = useUpdateDeliveryTargetMutation();
  const deleteDeliveryTargetMutation = useDeleteDeliveryTargetMutation();
  const [modalMode, setModalMode] = useState<"create" | "edit">("create");
  const [editingTarget, setEditingTarget] = useState<DeliveryTarget | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deletingTargetId, setDeletingTargetId] = useState<number | null>(null);
  const showInitialError =
    (sinkTypesQuery.isError && sinkTypesQuery.data === undefined) ||
    (deliveryTargetsQuery.isError && deliveryTargetsQuery.data === undefined);
  const isInitialLoading =
    sinkTypesQuery.isLoading || deliveryTargetsQuery.isLoading;
  const formError =
    modalMode === "create"
      ? createDeliveryTargetMutation.error
      : updateDeliveryTargetMutation.error;
  const isSubmitting =
    createDeliveryTargetMutation.isPending ||
    updateDeliveryTargetMutation.isPending;

  const resetMutationState = () => {
    createDeliveryTargetMutation.reset();
    updateDeliveryTargetMutation.reset();
  };

  const openCreateModal = () => {
    resetMutationState();
    setModalMode("create");
    setEditingTarget(null);
    setIsModalOpen(true);
  };

  const openEditModal = (target: DeliveryTarget) => {
    resetMutationState();
    setModalMode("edit");
    setEditingTarget(target);
    setIsModalOpen(true);
  };

  const closeModal = () => {
    if (isSubmitting) {
      return;
    }

    resetMutationState();
    setIsModalOpen(false);
    setEditingTarget(null);
  };

  const refreshAll = () => {
    void sinkTypesQuery.refetch();
    void deliveryTargetsQuery.refetch();
  };

  const handleSubmit = async (values: DeliveryTargetFormValues) => {
    try {
      if (modalMode === "create") {
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

      setIsModalOpen(false);
      setEditingTarget(null);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to save delivery target."));
      throw error;
    }
  };

  const handleDelete = async (target: DeliveryTarget) => {
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

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Delivery Targets
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            Manage delivery target resources that back event sink outputs.
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            onClick={refreshAll}
            loading={sinkTypesQuery.isFetching || deliveryTargetsQuery.isFetching}
          >
            Refresh
          </Button>
          <Button type="primary" onClick={openCreateModal}>
            New Target
          </Button>
        </Space>
      </Space>

      {isInitialLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">
              Loading delivery targets...
            </Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Failed to load delivery targets"
          subTitle={getErrorMessage(sinkTypesQuery.error ?? deliveryTargetsQuery.error)}
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
            message={`Total ${targets.length} delivery targets`}
          />
          {formError ? (
            <Alert
              type="error"
              showIcon
              message="Failed to save delivery target"
              description={getErrorMessage(formError)}
            />
          ) : null}
          <DeliveryTargetsTable
            targets={targets}
            sinkTypes={sinkTypes}
            deletingTargetId={deletingTargetId}
            actionsDisabled={deletingTargetId !== null}
            onEdit={openEditModal}
            onDelete={handleDelete}
          />
        </Space>
      ) : null}

      <DeliveryTargetFormModal
        open={isModalOpen}
        mode={modalMode}
        target={editingTarget}
        sinkTypes={sinkTypes}
        confirmLoading={isSubmitting}
        onCancel={closeModal}
        onSubmit={handleSubmit}
      />
    </Space>
  );
}
