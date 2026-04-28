import { Space, Typography } from "antd";

export function ProjectsPlaceholderPage() {
  return (
    <Space direction="vertical" size={12}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Projects
      </Typography.Title>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        项目管理页将在后续任务中实现，这里先保留最小占位。
      </Typography.Paragraph>
    </Space>
  );
}
