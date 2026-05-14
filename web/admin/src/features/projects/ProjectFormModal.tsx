import { useEffect } from "react";
import type { ReactNode } from "react";
import {
  Divider,
  Form,
  Input,
  Modal,
  Select,
  Switch,
  Typography,
} from "antd";
import type { Project, ProjectFormValues } from "./types";

type ProjectFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  project?: Project | null;
  confirmLoading?: boolean;
  processorSection?: ReactNode;
  ruleSetsSection?: ReactNode;
  onCancel: () => void;
  onSubmit: (values: ProjectFormValues) => Promise<void>;
};

function toFormValues(project?: Project | null): ProjectFormValues {
  return {
    name: project?.name ?? "",
    project_key: project?.project_key ?? "",
    enabled: project?.enabled ?? true,
    auth_mode: project?.auth_mode ?? "token",
    allowed_ips_text: project?.allowed_ips.join("\n") ?? "",
    ingest_token: project?.ingest_token ?? "",
  };
}

export function ProjectFormModal({
  open,
  mode,
  project,
  confirmLoading = false,
  processorSection,
  ruleSetsSection,
  onCancel,
  onSubmit,
}: ProjectFormModalProps) {
  const [form] = Form.useForm<ProjectFormValues>();
  const projectKey = Form.useWatch("project_key", form);
  const authMode = Form.useWatch("auth_mode", form);

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(toFormValues(project));
  }, [form, open, project]);

  const handleOk = async () => {
    const values = await form.validateFields();
    await onSubmit({
      name: values.name.trim(),
      project_key: values.project_key.trim(),
      enabled: values.enabled,
      auth_mode: values.auth_mode,
      allowed_ips_text: values.allowed_ips_text,
      ingest_token: values.ingest_token?.trim(),
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={mode === "edit" ? 820 : 520}
      title={mode === "create" ? "Create Project" : "Edit Project"}
      okText={mode === "create" ? "Create" : "Save"}
      cancelText="Cancel"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<ProjectFormValues>
        form={form}
        layout="vertical"
        initialValues={toFormValues(project)}
      >
        <Form.Item<ProjectFormValues>
          label="Project Name"
          name="name"
          rules={[
            { required: true, message: "Please enter project name" },
            { whitespace: true, message: "Project name cannot be empty" },
          ]}
        >
          <Input placeholder="Enter project name" maxLength={120} />
        </Form.Item>
        <Form.Item<ProjectFormValues>
          label="Project Key"
          name="project_key"
          extra={
            <Typography.Text type="secondary">
              Ingest path:{" "}
              <Typography.Text code>
                /ingest/{projectKey || "{project_key}"}
              </Typography.Text>
            </Typography.Text>
          }
          rules={[
            { required: true, message: "Please enter project key" },
            { whitespace: true, message: "Project key cannot be empty" },
            {
              pattern: /^[A-Za-z0-9_-]+$/,
              message: "Only letters, numbers, underscore, and hyphen are allowed",
            },
          ]}
        >
          <Input placeholder="adjust-app" maxLength={120} />
        </Form.Item>
        <Form.Item<ProjectFormValues>
          label="Auth Mode"
          name="auth_mode"
          rules={[{ required: true, message: "Please select auth mode" }]}
        >
          <Select
            options={[
              { label: "Token", value: "token" },
              { label: "Public", value: "public" },
            ]}
          />
        </Form.Item>
        <Form.Item<ProjectFormValues>
          label="Allowed IPs"
          name="allowed_ips_text"
        >
          <Input.TextArea
            autoSize={{ minRows: 2, maxRows: 5 }}
            placeholder="Optional. One IP per line, or separate with commas"
          />
        </Form.Item>
        {authMode === "token" ? (
          <Form.Item<ProjectFormValues>
            label="Ingest Token"
            name="ingest_token"
          >
            <Input
              placeholder="Leave empty to auto-generate"
              maxLength={256}
            />
          </Form.Item>
        ) : null}
        <Form.Item<ProjectFormValues>
          label="Enabled"
          name="enabled"
          valuePropName="checked"
        >
          <Switch checkedChildren="Enabled" unCheckedChildren="Disabled" />
        </Form.Item>
      </Form>
      {mode === "edit" && (processorSection || ruleSetsSection) ? (
        <>
          <Divider />
          {processorSection}
          {processorSection && ruleSetsSection ? <Divider /> : null}
          {ruleSetsSection}
        </>
      ) : null}
    </Modal>
  );
}
