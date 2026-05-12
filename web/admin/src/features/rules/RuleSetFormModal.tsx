import { useEffect } from "react";
import { Form, Input, Modal, Switch } from "antd";
import type { RuleSet, RuleSetFormValues } from "./types";

type RuleSetFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  ruleSet?: RuleSet | null;
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: RuleSetFormValues) => Promise<void>;
};

function toFormValues(ruleSet?: RuleSet | null): RuleSetFormValues {
  return {
    name: ruleSet?.name ?? "",
    description: ruleSet?.description ?? "",
    enabled: ruleSet?.enabled ?? true,
  };
}

export function RuleSetFormModal({
  open,
  mode,
  ruleSet,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: RuleSetFormModalProps) {
  const [form] = Form.useForm<RuleSetFormValues>();

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(toFormValues(ruleSet));
  }, [form, open, ruleSet]);

  const handleOk = async () => {
    const values = await form.validateFields();
    await onSubmit({
      name: values.name.trim(),
      description: values.description.trim(),
      enabled: values.enabled,
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      title={mode === "create" ? "Create Rule Set" : "Edit Rule Set"}
      okText={mode === "create" ? "Create" : "Save"}
      cancelText="Cancel"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<RuleSetFormValues>
        form={form}
        layout="vertical"
        initialValues={toFormValues(ruleSet)}
      >
        <Form.Item<RuleSetFormValues>
          label="Rule Set Name"
          name="name"
          rules={[
            { required: true, message: "Please enter a rule set name" },
            { whitespace: true, message: "Rule set name cannot be empty" },
          ]}
        >
          <Input placeholder="For example: default ingest rule set" maxLength={120} />
        </Form.Item>
        <Form.Item<RuleSetFormValues> label="Description" name="description">
          <Input.TextArea
            rows={3}
            placeholder="Enter rule set description"
            maxLength={500}
          />
        </Form.Item>
        <Form.Item<RuleSetFormValues>
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
