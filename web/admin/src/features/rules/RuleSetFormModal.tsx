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
      title={mode === "create" ? "创建规则集" : "编辑规则集"}
      okText={mode === "create" ? "创建" : "保存"}
      cancelText="取消"
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
          label="规则集名称"
          name="name"
          rules={[
            { required: true, message: "请输入规则集名称" },
            { whitespace: true, message: "规则集名称不能为空" },
          ]}
        >
          <Input placeholder="例如：默认 ingest 规则集" maxLength={120} />
        </Form.Item>
        <Form.Item<RuleSetFormValues> label="描述" name="description">
          <Input.TextArea rows={3} placeholder="请输入规则集说明" maxLength={500} />
        </Form.Item>
        <Form.Item<RuleSetFormValues>
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
