import { useState } from "react";
import { App as AntApp, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { ServiceNodesTable } from "./ServiceNodesTable";
import { useDeleteServiceNodeMutation, useServiceNodesQuery } from "./hooks";
import type { ServiceNode } from "./types";
import { getErrorMessage } from "./utils";

export function ServiceNodesPage() {
  const { message } = AntApp.useApp();
  const serviceNodesQuery = useServiceNodesQuery();
  const deleteServiceNodeMutation = useDeleteServiceNodeMutation();
  const [deletingNodeId, setDeletingNodeId] = useState<string | null>(null);
  const nodes = serviceNodesQuery.data ?? [];
  const hasLoadedNodes = serviceNodesQuery.data !== undefined;
  const showInitialError = serviceNodesQuery.isError && !hasLoadedNodes;
  const showRefreshError = serviceNodesQuery.isError && hasLoadedNodes;
  const isDeletePending = deleteServiceNodeMutation.isPending;

  const handleDeleteServiceNode = async (node: ServiceNode) => {
    if (deletingNodeId) {
      return;
    }

    setDeletingNodeId(node.node_id);
    try {
      await deleteServiceNodeMutation.mutateAsync(node.node_id);
      message.success(`Service node ${node.node_id} cleaned up`);
    } catch (error) {
      message.error(getErrorMessage(error, "Failed to clean up service node."));
    } finally {
      setDeletingNodeId(null);
    }
  };

  if (serviceNodesQuery.isLoading) {
    return <Spin tip="Loading service nodes..." />;
  }

  if (showInitialError) {
    return (
      <Result
        status="error"
        title="Failed to load service nodes"
        subTitle="Please verify admin API access and that the admin password is valid."
        extra={
          <Button type="primary" onClick={() => void serviceNodesQuery.refetch()}>
            Retry
          </Button>
        }
      />
    );
  }

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Service Nodes
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            View registered ingest4x service nodes and recent heartbeat status.
          </Typography.Paragraph>
        </div>
        <Button
          loading={serviceNodesQuery.isFetching}
          onClick={() => void serviceNodesQuery.refetch()}
        >
          Refresh
        </Button>
      </Space>

      {showRefreshError ? (
        <Alert
          type="warning"
          showIcon
          message="Refresh failed, still showing the last successful data."
        />
      ) : null}

      <ServiceNodesTable
        nodes={nodes}
        deletingNodeId={deletingNodeId}
        actionsDisabled={isDeletePending}
        onDelete={handleDeleteServiceNode}
      />
    </Space>
  );
}
