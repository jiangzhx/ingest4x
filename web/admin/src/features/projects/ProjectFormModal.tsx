import { useEffect } from "react";
import type { ReactNode } from "react";
import { Checkbox, Divider, Form, Input, Modal, Switch, Typography } from "antd";
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
    enabled: project?.enabled ?? true,
    ingest_token: "",
    regenerate_ingest_token: false,
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
      enabled: values.enabled,
      ingest_token: values.ingest_token?.trim(),
      regenerate_ingest_token: values.regenerate_ingest_token,
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
        {mode === "edit" && project ? (
          <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
            Current Token:{" "}
            <Typography.Text code>{project.ingest_token}</Typography.Text>
          </Typography.Paragraph>
        ) : null}
        <Form.Item<ProjectFormValues>
          label={mode === "create" ? "Ingest Token" : "New Ingest Token"}
          name="ingest_token"
        >
          <Input
            placeholder={mode === "create" ? "Leave empty to auto-generate" : "Leave empty to keep current"}
            maxLength={256}
          />
        </Form.Item>
        {mode === "edit" ? (
          <Form.Item<ProjectFormValues>
            name="regenerate_ingest_token"
            valuePropName="checked"
          >
            <Checkbox>Regenerate token when saving</Checkbox>
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
