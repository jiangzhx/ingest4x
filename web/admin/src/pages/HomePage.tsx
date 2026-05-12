import { Button, Space, Typography } from "antd";
import { Link } from "@tanstack/react-router";

export function HomePage() {
  return (
    <Space direction="vertical" size={12}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Admin Console
      </Typography.Title>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        Manage projects, rules, event sink delivery, and Rhai processors.
      </Typography.Paragraph>
      <Space>
        <Button type="primary">
          <Link to="/projects">Projects</Link>
        </Button>
        <Button>
          <Link to="/rules">Rules</Link>
        </Button>
        <Button>
          <Link to="/sinks">Sink Management</Link>
        </Button>
        <Button>
          <Link to="/processors">Processor Management</Link>
        </Button>
        <Button>
          <Link to="/service-nodes">Service Nodes</Link>
        </Button>
      </Space>
    </Space>
  );
}
