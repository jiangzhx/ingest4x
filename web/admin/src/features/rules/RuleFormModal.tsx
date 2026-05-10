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
      title={mode === "create" ? "创建 Rhai 脚本" : "编辑 Rhai 脚本"}
      okText={mode === "create" ? "创建" : "保存"}
      cancelText="取消"
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
          label="Rhai 脚本"
          name="content"
          rules={[
            { required: true, message: "请输入 Rhai 脚本" },
            { whitespace: true, message: "Rhai 脚本不能为空" },
          ]}
        >
          <LazyRhaiEditor />
        </Form.Item>
        <Form.Item<RuleFormFields>
          label="启用状态"
          name="enabled"
          valuePropName="checked"
        >
          <Switch checkedChildren="已启用" unCheckedChildren="已停用" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
