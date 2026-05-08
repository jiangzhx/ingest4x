import type { PropsWithChildren } from "react";
import { Button, Layout, Menu, Typography } from "antd";
import { Link, useLocation, useNavigate } from "@tanstack/react-router";
import { clearAdminPassword } from "../features/auth/storage";

const { Header, Content, Sider } = Layout;

const menuItems = [
  {
    key: "/",
    label: <Link to="/">概览</Link>,
  },
  {
    key: "/projects",
    label: <Link to="/projects">项目管理</Link>,
  },
  {
    key: "/rules",
    label: <Link to="/rules">规则管理</Link>,
  },
  {
    key: "/sinks",
    label: <Link to="/sinks">Sink 管理</Link>,
  },
  {
    key: "/processors",
    label: <Link to="/processors">Processor 管理</Link>,
  },
];

export function AdminShell({ children }: PropsWithChildren) {
  const location = useLocation();
  const navigate = useNavigate();
  const selectedKey =
    menuItems.find(
      (item) => item.key !== "/" && location.pathname.startsWith(item.key),
    )?.key ?? (location.pathname === "/" ? "/" : "");

  const handleLogout = async () => {
    clearAdminPassword();
    await navigate({ to: "/login" });
  };

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider breakpoint="lg" collapsedWidth="0" theme="light" width={220}>
        <div style={{ padding: "24px 20px 16px" }}>
          <Typography.Title level={4} style={{ margin: 0 }}>
            Ingest4x
          </Typography.Title>
          <Typography.Text type="secondary">管理后台</Typography.Text>
        </div>
        <Menu
          mode="inline"
          selectedKeys={selectedKey ? [selectedKey] : []}
          items={menuItems}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            display: "flex",
            alignItems: "center",
            padding: "0 24px",
            background: "#ffffff",
            borderBottom: "1px solid #f0f0f0",
          }}
        >
          <Typography.Text strong>控制台</Typography.Text>
          <Button
            onClick={() => void handleLogout()}
            style={{ marginLeft: "auto" }}
          >
            登出
          </Button>
        </Header>
        <Content style={{ padding: 24 }}>
          <div
            style={{
              minHeight: 280,
              padding: 24,
              background: "#ffffff",
              borderRadius: 16,
              boxShadow: "0 8px 24px rgba(15, 23, 42, 0.06)",
            }}
          >
            {children}
          </div>
        </Content>
      </Layout>
    </Layout>
  );
}
