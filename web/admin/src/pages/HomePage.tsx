import { Button, Space, Typography } from "antd";
import { Link } from "@tanstack/react-router";

export function HomePage() {
  return (
    <Space direction="vertical" size={12}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Admin Console
      </Typography.Title>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        管理项目、规则、事件投递 sink 和 Rhai processor。
      </Typography.Paragraph>
      <Space>
        <Button type="primary">
          <Link to="/projects">项目管理</Link>
        </Button>
        <Button>
          <Link to="/rules">规则管理</Link>
        </Button>
        <Button>
          <Link to="/sinks">Sink 管理</Link>
        </Button>
        <Button>
          <Link to="/processors">Processor 管理</Link>
        </Button>
      </Space>
    </Space>
  );
}
