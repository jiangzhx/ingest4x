import { Empty, Space, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { ServiceNode } from "./types";
import {
  formatServiceNodeTimestamp,
  getHeartbeatAge,
  getServiceNodeStatusColor,
  getServiceNodeStatusLabel,
} from "./utils";

type ServiceNodesTableProps = {
  nodes: ServiceNode[];
};

export function ServiceNodesTable({ nodes }: ServiceNodesTableProps) {
  const columns: ColumnsType<ServiceNode> = [
    {
      title: "Node ID",
      dataIndex: "node_id",
      key: "node_id",
      render: (value: string) => (
        <Typography.Text code style={{ whiteSpace: "nowrap" }}>
          {value}
        </Typography.Text>
      ),
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (value: ServiceNode["status"]) => (
        <Tag color={getServiceNodeStatusColor(value)}>
          {getServiceNodeStatusLabel(value)}
        </Tag>
      ),
    },
    {
      title: "Host",
      key: "host",
      width: 220,
      render: (_, node) => (
        <Space direction="vertical" size={2}>
          <Typography.Text>{node.hostname ?? "-"}</Typography.Text>
          <Typography.Text type="secondary">{node.machine_ip ?? "-"}</Typography.Text>
        </Space>
      ),
    },
    {
      title: "Listen Addresses",
      key: "addresses",
      width: 260,
      render: (_, node) => (
        <Space direction="vertical" size={2}>
          <Typography.Text code>{node.ingest_bind_address}</Typography.Text>
          <Typography.Text code type="secondary">
            {node.management_bind_address}
          </Typography.Text>
        </Space>
      ),
    },
    {
      title: "Version",
      dataIndex: "version",
      key: "version",
      width: 120,
      render: (value: string) => <Tag>{value}</Tag>,
    },
    {
      title: "Latest Heartbeat",
      dataIndex: "last_seen_at",
      key: "last_seen_at",
      width: 220,
      render: (value: number) => (
        <Space direction="vertical" size={2}>
          <Typography.Text>{formatServiceNodeTimestamp(value)}</Typography.Text>
          <Typography.Text type="secondary">{getHeartbeatAge(value)} ago</Typography.Text>
        </Space>
      ),
    },
    {
      title: "Start Time",
      dataIndex: "started_at",
      key: "started_at",
      width: 200,
      render: (value: number) => (
        <Typography.Text type="secondary">
          {formatServiceNodeTimestamp(value)}
        </Typography.Text>
      ),
    },
  ];

  return (
    <Table<ServiceNode>
      rowKey="node_id"
      columns={columns}
      dataSource={nodes}
      pagination={false}
      locale={{
        emptyText: (
          <Empty
            description="No registered nodes are available"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ),
      }}
      scroll={{ x: "max-content" }}
    />
  );
}
