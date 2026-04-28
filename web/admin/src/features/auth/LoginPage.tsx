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
      message.success("登录成功");
      await navigate({ to: getLoginRedirectPath() as "/" });
    } catch (error) {
      clearAdminPassword();
      message.error(error instanceof Error ? error.message : "登录失败");
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
              登录管理后台
            </Typography.Title>
          </div>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            输入管理员密码后，前端会保存在当前浏览器会话中，并在后续
            `/api/admin/*` 请求中自动附带；刷新页面会保持登录状态。
          </Typography.Paragraph>
          {passwordExistsInSession ? (
            <Typography.Paragraph style={{ marginBottom: 0 }}>
              当前浏览器会话中已有管理员密码，你可以重新登录以覆盖它。
            </Typography.Paragraph>
          ) : null}
          <Form<LoginFormValues>
            layout="vertical"
            onFinish={(values) => void handleSubmit(values)}
          >
            <Form.Item
              label="管理员密码"
              name="password"
              rules={[{ required: true, message: "请输入管理员密码" }]}
            >
              <Input.Password
                autoComplete="current-password"
                placeholder="输入密码"
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
              登录
            </Button>
          </Form>
        </Space>
      </Card>
    </div>
  );
}
