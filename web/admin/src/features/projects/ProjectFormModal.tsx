import { useEffect } from "react";
import type { ReactNode } from "react";
import { Divider, Form, Input, Modal, Switch } from "antd";
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
    appid: project?.appid ?? "",
    name: project?.name ?? "",
    enabled: project?.enabled ?? true,
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
      appid: values.appid.trim(),
      name: values.name.trim(),
      enabled: values.enabled,
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={mode === "edit" ? 820 : 520}
      title={mode === "create" ? "创建项目" : "编辑项目"}
      okText={mode === "create" ? "创建" : "保存"}
      cancelText="取消"
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
          label="AppID"
          name="appid"
          rules={[
            { required: true, message: "请输入项目 AppID" },
            { whitespace: true, message: "AppID 不能为空" },
          ]}
          extra={mode === "create" ? "创建后不可修改。" : "AppID 创建后不可修改。"}
        >
          <Input placeholder="例如：my-app" disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<ProjectFormValues>
          label="项目名称"
          name="name"
          rules={[
            { required: true, message: "请输入项目名称" },
            { whitespace: true, message: "项目名称不能为空" },
          ]}
        >
          <Input placeholder="请输入项目名称" maxLength={120} />
        </Form.Item>
        <Form.Item<ProjectFormValues>
          label="启用状态"
          name="enabled"
          valuePropName="checked"
        >
          <Switch checkedChildren="已启用" unCheckedChildren="已停用" />
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
