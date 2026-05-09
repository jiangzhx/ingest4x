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
    return <Spin tip="正在加载服务节点..." />;
  }

  if (showInitialError) {
    return (
      <Result
        status="error"
        title="服务节点加载失败"
        subTitle="请确认管理端 API 可访问，并检查管理员密码是否有效。"
        extra={
          <Button type="primary" onClick={() => void serviceNodesQuery.refetch()}>
            重试
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
            节点管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            查看当前已注册的 ingest4x 服务节点和最近心跳。
          </Typography.Paragraph>
        </div>
        <Button
          loading={serviceNodesQuery.isFetching}
          onClick={() => void serviceNodesQuery.refetch()}
        >
          刷新
        </Button>
      </Space>

      {showRefreshError ? (
        <Alert
          type="warning"
          showIcon
          message="刷新服务节点失败，当前仍显示上一次成功加载的数据。"
        />
      ) : null}

      <ServiceNodesTable nodes={nodes} />
    </Space>
  );
}
