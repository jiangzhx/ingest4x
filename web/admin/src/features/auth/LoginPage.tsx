import { useState } from "react";
import {
  App as AntApp,
  Button,
  Card,
  Form,
  Input,
  Space,
  Typography,
} from "antd";
import { useNavigate } from "@tanstack/react-router";
import { loginWithPassword } from "./api";
import {
  clearAdminPassword,
  hasAdminPassword,
  setAdminPassword,
} from "./storage";

type LoginFormValues = {
  password: string;
};

function getLoginRedirectPath() {
  const redirectPath =
    new URLSearchParams(window.location.search).get("redirect") ?? "/";

  if (
    redirectPath.startsWith("/") &&
    !redirectPath.startsWith("//") &&
    redirectPath !== "/login"
  ) {
    return redirectPath;
  }

  return "/";
}

export function LoginPage() {
  const navigate = useNavigate();
  const { message } = AntApp.useApp();
  const [submitting, setSubmitting] = useState(false);
  const passwordExistsInSession = hasAdminPassword();

  const handleSubmit = async ({ password }: LoginFormValues) => {
    setSubmitting(true);

      try {
      await loginWithPassword(password);
      setAdminPassword(password);
      message.success("Login successful");
      await navigate({ to: getLoginRedirectPath() as "/" });
    } catch (error) {
      clearAdminPassword();
      message.error(error instanceof Error ? error.message : "Login failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "grid",
        placeItems: "center",
        padding: 24,
        background:
          "radial-gradient(circle at top, rgba(17, 24, 39, 0.12), transparent 35%), #f5f7fa",
      }}
    >
      <Card
        style={{
          width: "100%",
          maxWidth: 420,
          borderRadius: 20,
          boxShadow: "0 20px 60px rgba(15, 23, 42, 0.12)",
        }}
      >
        <Space direction="vertical" size={16} style={{ width: "100%" }}>
          <div>
            <Typography.Text type="secondary">Ingest4x Admin</Typography.Text>
            <Typography.Title level={2} style={{ margin: "8px 0 0" }}>
              Admin Login
            </Typography.Title>
          </div>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            After entering the admin password, it is saved in the current browser
            session and automatically attached to subsequent `/api/admin/*`
            requests. The login state is kept after page refresh.
          </Typography.Paragraph>
          {passwordExistsInSession ? (
            <Typography.Paragraph style={{ marginBottom: 0 }}>
              There is an existing admin password in this browser session. You can
              sign in again to overwrite it.
            </Typography.Paragraph>
          ) : null}
          <Form<LoginFormValues>
            layout="vertical"
            onFinish={(values) => void handleSubmit(values)}
          >
              <Form.Item
              label="Admin Password"
              name="password"
              rules={[{ required: true, message: "Please enter admin password" }]}
            >
              <Input.Password
                autoComplete="current-password"
                placeholder="Enter password"
                size="large"
              />
            </Form.Item>
            <Button
              block
              htmlType="submit"
              loading={submitting}
              size="large"
              type="primary"
            >
              Sign In
            </Button>
          </Form>
        </Space>
      </Card>
    </div>
  );
}
