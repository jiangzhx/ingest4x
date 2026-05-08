import { useEffect } from "react";
import {
  Button,
  Form,
  Input,
  Modal,
  Popconfirm,
  Select,
  Space,
  Typography,
} from "antd";
import type {
  ProcessorScriptFormValues,
  ProcessorScriptStatus,
} from "./types";
import { RhaiEditor } from "./RhaiEditor";
import { DEFAULT_PROCESSOR_SOURCE } from "./utils";

type ProcessorScriptFormModalProps = {
  open: boolean;
  mode?: "create" | "edit";
  initialValues?: ProcessorScriptFormValues;
  confirmLoading?: boolean;
  validateLoading?: boolean;
  validationError?: string | null;
  loading?: boolean;
  onCancel: () => void;
  onValidate: (
    values: ProcessorScriptFormValues,
    options?: { notify?: boolean },
  ) => Promise<void>;
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

function extractValidationModuleName(error: string | null): string | null {
  if (!error) {
    return null;
  }

  return /Rhai module `([^`]+)`/.exec(error)?.[1] ?? null;
}

function renderRhaiSourceLabel(error: string | null) {
  return (
    <Space size={8} align="center" wrap>
      <span>Rhai Source</span>
      {error ? (
        <Typography.Text
          type="danger"
          style={{ fontSize: 12, maxWidth: 620 }}
          ellipsis={{ tooltip: error }}
        >
          {error}
        </Typography.Text>
      ) : null}
    </Space>
  );
}

export function ProcessorScriptFormModal({
  open,
  mode = "create",
  initialValues,
  confirmLoading = false,
  validateLoading = false,
  validationError = null,
  loading = false,
  onCancel,
  onValidate,
  onSubmit,
}: ProcessorScriptFormModalProps) {
  const [form] = Form.useForm<ProcessorScriptFormValues>();
  const watchedModules = Form.useWatch("modules", form);
  const validationModuleName = extractValidationModuleName(validationError);

  const sourceErrorForField = (fieldName: number) => {
    if (!validationError) {
      return null;
    }

    const moduleName = watchedModules?.[fieldName]?.module_name?.trim() ?? "";
    if (validationModuleName) {
      return validationModuleName === moduleName ? validationError : null;
    }

    return fieldName === 0 ? validationError : null;
  };

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    if (mode === "edit" && initialValues === undefined) {
      return;
    }

    form.setFieldsValue(initialValues ?? defaultValues);
  }, [form, initialValues, mode, open]);

  const validatedValues = async () => {
    const values = await form.validateFields();
    return {
      ...values,
      script_key: values.script_key.trim(),
      name: values.name.trim(),
      entry_module: values.entry_module.trim(),
      modules: values.modules.map((module) => ({
        module_name: module.module_name.trim(),
        source: module.source,
      })),
    };
  };

  const validateScript = async (notify = false) => {
    const values = await validatedValues();
    await onValidate(values, { notify });
    return values;
  };

  const handleOk = async () => {
    const values = await validateScript();
    await onSubmit(values);
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={900}
      title={mode === "edit" ? "编辑 Processor Script" : "创建 Processor Script"}
      loading={loading}
      onCancel={onCancel}
      footer={
        <Space style={{ display: "flex", justifyContent: "flex-end" }}>
          <Button onClick={onCancel}>取消</Button>
          <Button
            loading={validateLoading}
            onClick={() => {
              void validateScript(true).catch(() => {});
            }}
          >
            检查
          </Button>
          <Button
            type="primary"
            loading={confirmLoading}
            onClick={() => {
              void handleOk().catch(() => {});
            }}
          >
            {mode === "edit" ? "保存" : "创建"}
          </Button>
        </Space>
      }
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
            <Input
              disabled={mode === "edit"}
              placeholder="例如：payment_pipeline"
              style={{ width: 260 }}
            />
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
                    <Popconfirm
                      title="确认删除这个 Module？"
                      okText="删除"
                      cancelText="取消"
                      disabled={fields.length <= 1}
                      okButtonProps={{ danger: true }}
                      onConfirm={() => remove(field.name)}
                    >
                      <Button
                        danger
                        disabled={fields.length <= 1}
                        style={{ marginTop: 30 }}
                      >
                        删除 Module
                      </Button>
                    </Popconfirm>
                  </Space>
                  <Form.Item
                    label={renderRhaiSourceLabel(sourceErrorForField(field.name))}
                    name={[field.name, "source"]}
                    rules={[
                      { required: true, message: "请输入 Rhai source" },
                      { whitespace: true, message: "source 不能为空" },
                    ]}
                  >
                    <RhaiEditor />
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
