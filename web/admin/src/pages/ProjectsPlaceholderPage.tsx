import { Space, Typography } from "antd";

export function ProjectsPlaceholderPage() {
  return (
    <Space direction="vertical" size={12}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Projects
      </Typography.Title>
      <Typography.Paragraph style={{ marginBottom: 0 }}>
        Project management page is planned for a follow-up task; keep a minimal
        placeholder for now.
      </Typography.Paragraph>
    </Space>
  );
}
