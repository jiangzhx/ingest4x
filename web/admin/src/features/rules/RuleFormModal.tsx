import { lazy, Suspense, useEffect } from "react";
import { Form, Modal, Skeleton, Switch } from "antd";
import type { Rule, RuleFormValues } from "./types";

const RhaiEditor = lazy(() =>
  import("../processors/RhaiEditor").then((module) => ({
    default: module.RhaiEditor,
  })),
);

const EMPTY_RHAI_CONTENT = `fn validate(event) {
    event.result()
}
`;

type LazyRhaiEditorProps = {
  value?: string;
  onChange?: (value: string) => void;
};

function LazyRhaiEditor({ value, onChange }: LazyRhaiEditorProps) {
  return (
    <Suspense fallback={<Skeleton.Input block active style={{ height: 320 }} />}>
      <RhaiEditor value={value} onChange={onChange} />
    </Suspense>
  );
}

type RuleFormFields = Omit<RuleFormValues, "parent_id" | "name" | "xwhat">;

type RuleFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  rule?: Rule | null;
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: RuleFormValues) => Promise<void>;
};

function toFormValues(rule?: Rule | null): RuleFormFields {
  return {
    content: rule?.content ?? EMPTY_RHAI_CONTENT,
    enabled: rule?.enabled ?? true,
  };
}

export function RuleFormModal({
  open,
  mode,
  rule,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: RuleFormModalProps) {
  const [form] = Form.useForm<RuleFormFields>();

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(toFormValues(rule));
  }, [form, open, rule]);

  const handleOk = async () => {
    const values = await form.validateFields();
    await onSubmit({
      parent_id: null,
      name: "Validation rule",
      xwhat: "",
      content: values.content.trim(),
      enabled: values.enabled,
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={760}
      title={mode === "create" ? "Create Rhai Script" : "Edit Rhai Script"}
      okText={mode === "create" ? "Create" : "Save"}
      cancelText="Cancel"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<RuleFormFields>
        form={form}
        layout="vertical"
        initialValues={toFormValues(rule)}
      >
        <Form.Item<RuleFormFields>
          label="Rhai Script"
          name="content"
          rules={[
            { required: true, message: "Please enter Rhai script" },
            { whitespace: true, message: "Rhai script cannot be empty" },
          ]}
        >
          <LazyRhaiEditor />
        </Form.Item>
        <Form.Item<RuleFormFields>
          label="Enabled"
          name="enabled"
          valuePropName="checked"
        >
          <Switch checkedChildren="Enabled" unCheckedChildren="Disabled" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
