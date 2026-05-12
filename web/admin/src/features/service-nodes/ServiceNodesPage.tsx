import { Alert, Button, Result, Space, Spin, Typography } from "antd";
import { ServiceNodesTable } from "./ServiceNodesTable";
import { useServiceNodesQuery } from "./hooks";

export function ServiceNodesPage() {
  const serviceNodesQuery = useServiceNodesQuery();
  const nodes = serviceNodesQuery.data ?? [];
  const hasLoadedNodes = serviceNodesQuery.data !== undefined;
  const showInitialError = serviceNodesQuery.isError && !hasLoadedNodes;
  const showRefreshError = serviceNodesQuery.isError && hasLoadedNodes;

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

      <ServiceNodesTable nodes={nodes} />
    </Space>
  );
}
