import { Button, Space, Typography } from "antd";
import { Link } from "@tanstack/react-router";

export function HomePage() {
  return (
    <Space direction="vertical" size={12}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Admin Console
      </Typography.Title>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        当前只提供管理后台基础壳，后续任务会补齐登录和项目管理页面。
      </Typography.Paragraph>
      <Button type="primary">
        <Link to="/projects">进入项目页占位</Link>
      </Button>
    </Space>
  );
}
