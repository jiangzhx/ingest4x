import { useEffect } from "react";
import { Button, Form, Input, Modal, Select, Space } from "antd";
import type {
  ProcessorScriptFormValues,
  ProcessorScriptStatus,
} from "./types";
import { DEFAULT_PROCESSOR_SOURCE } from "./utils";

type ProcessorScriptFormModalProps = {
  open: boolean;
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: ProcessorScriptFormValues) => Promise<void>;
};

const statusOptions: Array<{ label: string; value: ProcessorScriptStatus }> = [
  { label: "active", value: "active" },
  { label: "draft", value: "draft" },
];

const defaultValues: ProcessorScriptFormValues = {
  script_key: "",
  name: "",
  entry_module: "main",
  status: "active",
  modules: [
    {
      module_name: "main",
      source: DEFAULT_PROCESSOR_SOURCE,
    },
  ],
};

export function ProcessorScriptFormModal({
  open,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: ProcessorScriptFormModalProps) {
  const [form] = Form.useForm<ProcessorScriptFormValues>();

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(defaultValues);
  }, [form, open]);

  const handleOk = async () => {
    const values = await form.validateFields();
    await onSubmit({
      ...values,
      script_key: values.script_key.trim(),
      name: values.name.trim(),
      entry_module: values.entry_module.trim(),
      modules: values.modules.map((module) => ({
        module_name: module.module_name.trim(),
        source: module.source,
      })),
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={900}
      title="创建 Processor Script"
      okText="创建"
      cancelText="取消"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<ProcessorScriptFormValues>
        form={form}
        layout="vertical"
        initialValues={defaultValues}
      >
        <Space style={{ display: "flex" }} align="start">
          <Form.Item<ProcessorScriptFormValues>
            label="Script Key"
            name="script_key"
            rules={[
              { required: true, message: "请输入 script_key" },
              { whitespace: true, message: "script_key 不能为空" },
            ]}
          >
            <Input placeholder="例如：payment_pipeline" style={{ width: 260 }} />
          </Form.Item>
          <Form.Item<ProcessorScriptFormValues>
            label="展示名"
            name="name"
            rules={[
              { required: true, message: "请输入展示名" },
              { whitespace: true, message: "展示名不能为空" },
            ]}
          >
            <Input placeholder="例如：支付事件处理脚本" style={{ width: 260 }} />
          </Form.Item>
          <Form.Item<ProcessorScriptFormValues>
            label="Entry Module"
            name="entry_module"
            rules={[
              { required: true, message: "请输入 entry_module" },
              { whitespace: true, message: "entry_module 不能为空" },
            ]}
          >
            <Input placeholder="main" style={{ width: 160 }} />
          </Form.Item>
          <Form.Item<ProcessorScriptFormValues>
            label="状态"
            name="status"
            rules={[{ required: true, message: "请选择状态" }]}
          >
            <Select options={statusOptions} style={{ width: 130 }} />
          </Form.Item>
        </Space>

        <Form.List name="modules">
          {(fields, { add, remove }) => (
            <Space direction="vertical" size={12} style={{ display: "flex" }}>
              {fields.map((field) => (
                <div
                  key={field.key}
                  style={{
                    border: "1px solid #f0f0f0",
                    borderRadius: 8,
                    padding: 12,
                  }}
                >
                  <Space
                    align="start"
                    style={{ display: "flex", justifyContent: "space-between" }}
                  >
                    <Form.Item
                      label="Module Name"
                      name={[field.name, "module_name"]}
                      rules={[
                        { required: true, message: "请输入 module_name" },
                        { whitespace: true, message: "module_name 不能为空" },
                      ]}
                    >
                      <Input placeholder="main" style={{ width: 220 }} />
                    </Form.Item>
                    <Button
                      danger
                      disabled={fields.length <= 1}
                      onClick={() => remove(field.name)}
                    >
                      删除 Module
                    </Button>
                  </Space>
                  <Form.Item
                    label="Rhai Source"
                    name={[field.name, "source"]}
                    rules={[
                      { required: true, message: "请输入 Rhai source" },
                      { whitespace: true, message: "source 不能为空" },
                    ]}
                  >
                    <Input.TextArea
                      rows={12}
                      spellCheck={false}
                      style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}
                    />
                  </Form.Item>
                </div>
              ))}
              <Button
                onClick={() =>
                  add({
                    module_name: "custom",
                    source: "fn transform(event) {\n    return event;\n}",
                  })
                }
              >
                添加 Module
              </Button>
            </Space>
          )}
        </Form.List>
      </Form>
    </Modal>
  );
}
