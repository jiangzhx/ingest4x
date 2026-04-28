import { lazy, Suspense, useEffect } from "react";
import { Form, Input, Modal, Select, Skeleton, Switch } from "antd";
import type { Rule, RuleFormValues } from "./types";
import { ruleParentOptions } from "./utils";

const YamlEditor = lazy(() =>
  import("./YamlEditor").then((module) => ({ default: module.YamlEditor })),
);

const EMPTY_RULE_CONTENT = "fields:\n  {}\n";

type LazyYamlEditorProps = {
  value?: string;
  onChange?: (value: string) => void;
};

function LazyYamlEditor({ value, onChange }: LazyYamlEditorProps) {
  return (
    <Suspense fallback={<Skeleton.Input block active style={{ height: 320 }} />}>
      <YamlEditor value={value} onChange={onChange} />
    </Suspense>
  );
}

type RuleFormFields = Omit<RuleFormValues, "parent_id"> & {
  parent_id: number;
};

type RuleFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  rule?: Rule | null;
  rules: Rule[];
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: RuleFormValues) => Promise<void>;
};

function toFormValues(rule?: Rule | null): RuleFormFields {
  return {
    parent_id: rule?.parent_id ?? 0,
    name: rule?.name ?? "",
    xwhat: rule?.xwhat ?? "",
    content: rule?.content ?? EMPTY_RULE_CONTENT,
    enabled: rule?.enabled ?? true,
  };
}

export function RuleFormModal({
  open,
  mode,
  rule,
  rules,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: RuleFormModalProps) {
  const [form] = Form.useForm<RuleFormFields>();
  const parentOptions = [
    { label: "无父规则", value: 0 },
    ...ruleParentOptions(rules, rule?.id),
  ];

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
      parent_id: values.parent_id === 0 ? null : values.parent_id,
      name: values.name.trim(),
      xwhat: values.xwhat.trim(),
      content: values.content.trim(),
      enabled: values.enabled,
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={760}
      title={mode === "create" ? "创建规则" : "编辑规则"}
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
        <Form.Item<RuleFormFields> label="父规则" name="parent_id">
          <Select options={parentOptions} />
        </Form.Item>
        <Form.Item<RuleFormFields>
          label="规则名称"
          name="name"
          rules={[
            { required: true, message: "请输入规则名称" },
            { whitespace: true, message: "规则名称不能为空" },
          ]}
        >
          <Input placeholder="例如：支付" maxLength={120} />
        </Form.Item>
        <Form.Item<RuleFormFields> label="事件名" name="xwhat">
          <Input
            placeholder="留空表示父规则；填写后表示事件规则，不能再作为父规则"
            maxLength={120}
          />
        </Form.Item>
        <Form.Item<RuleFormFields>
          label="规则内容"
          name="content"
          rules={[
            { required: true, message: "请输入规则内容" },
            { whitespace: true, message: "规则内容不能为空" },
          ]}
        >
          <LazyYamlEditor />
        </Form.Item>
        <Form.Item<RuleFormFields>
          label="启用当前规则"
          name="enabled"
          valuePropName="checked"
        >
          <Switch checkedChildren="已启用" unCheckedChildren="已停用" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
