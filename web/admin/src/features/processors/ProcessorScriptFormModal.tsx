import { useEffect, useState, type ChangeEvent } from "react";
import {
  Button,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Tabs,
  Typography,
} from "antd";
import type { KeyboardEvent, MouseEvent } from "react";
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

type EditableTabTargetKey = MouseEvent | KeyboardEvent | string;

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

function nextModuleName(modules: ProcessorScriptFormValues["modules"] | undefined) {
  const moduleNameCandidates = new Set(
    (modules ?? [])
      .map((module) => module.module_name.trim())
      .filter(Boolean),
  );

  let index = 1;
  while (moduleNameCandidates.has(`module${index}`)) {
    index += 1;
  }

  return `module${index}`;
}

function extractValidationModuleName(error: string | null): string | null {
  if (!error) {
    return null;
  }

  return /Rhai module `([^`]+)`/.exec(error)?.[1] ?? null;
}

function renderRhaiSourceLabel(error: string | null) {
  return error ? (
    <Typography.Text
      type="danger"
      style={{ fontSize: 12, maxWidth: 620 }}
      ellipsis={{ tooltip: error }}
    >
      {error}
    </Typography.Text>
  ) : null;
}

type ModuleNameTabLabelProps = {
  tabKey: string;
  placeholder: string;
  value?: string;
  isEditing: boolean;
  isActive: boolean;
  onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
  onActivate: () => void;
  onStartEditing: () => void;
  onEditingComplete: () => void;
};

function ModuleNameTabLabel({
  tabKey,
  placeholder,
  value,
  isEditing,
  isActive,
  onChange,
  onActivate,
  onStartEditing,
  onEditingComplete,
}: ModuleNameTabLabelProps) {
  const displayValue = value?.trim() || placeholder;

  if (isEditing) {
    return (
      <Input
        aria-label="Module Name"
        autoFocus
        placeholder={placeholder}
        size="small"
        style={{ width: 140 }}
        value={value}
        onBlur={onEditingComplete}
        onChange={onChange}
        onClick={(event) => event.stopPropagation()}
        onFocus={onActivate}
        onKeyDown={(event) => {
          event.stopPropagation();
          if (event.key === "Enter") {
            event.currentTarget.blur();
          }
          if (event.key === "Escape") {
            onEditingComplete();
          }
        }}
      />
    );
  }

  return (
    <span
      data-module-tab-key={tabKey}
      style={{
        alignItems: "center",
        display: "inline-flex",
        gap: 4,
        cursor: isActive ? "text" : "pointer",
      }}
      onFocus={onActivate}
      onClick={(event) => {
        if (isActive) {
          event.stopPropagation();
          onStartEditing();
          return;
        }

        onActivate();
      }}
    >
      <Typography.Text
        ellipsis={{ tooltip: displayValue }}
        style={{
          color: isActive ? "#0958d9" : undefined,
          fontWeight: isActive ? 600 : 400,
          maxWidth: 96,
        }}
      >
        {displayValue}
      </Typography.Text>
    </span>
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
  const [activeModuleTab, setActiveModuleTab] = useState<string>();
  const [editingModuleTab, setEditingModuleTab] = useState<string>();
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
      setActiveModuleTab(undefined);
      setEditingModuleTab(undefined);
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
          {(fields, { add, remove }) => {
            const fieldModuleName = (field: { name: number }) =>
              watchedModules?.[field.name]?.module_name?.trim() ?? "";
            const orderedFields = [...fields].sort((left, right) => {
              const leftIsMain = fieldModuleName(left) === "main";
              const rightIsMain = fieldModuleName(right) === "main";
              if (leftIsMain === rightIsMain) {
                return 0;
              }

              return leftIsMain ? -1 : 1;
            });
            const activeKey = fields.some(
              (field) => String(field.key) === activeModuleTab,
            )
              ? activeModuleTab
              : String(orderedFields[0]?.key ?? "");
            const handleAddModule = () => {
              add({
                module_name: nextModuleName(watchedModules),
                source: "fn transform(event) {\n    return event;\n}",
              });
              setEditingModuleTab(undefined);
            };
            const handleRemoveModule = (targetKey: EditableTabTargetKey) => {
              const tabKey = String(targetKey);
              const targetField = fields.find(
                (field) => String(field.key) === tabKey,
              );
              if (!targetField || fields.length <= 1) {
                return;
              }

              const remainingFields = orderedFields.filter(
                (field) => String(field.key) !== tabKey,
              );
              if (tabKey === activeKey) {
                const targetIndex = orderedFields.findIndex(
                  (field) => String(field.key) === tabKey,
                );
                const nextActiveField =
                  remainingFields[
                    targetIndex === remainingFields.length
                      ? targetIndex - 1
                      : targetIndex
                  ];
                setActiveModuleTab(String(nextActiveField?.key ?? ""));
              }
              if (editingModuleTab === tabKey) {
                setEditingModuleTab(undefined);
              }
              remove(targetField.name);
            };
            const tabItems = orderedFields.map((field) => {
              const tabKey = String(field.key);

              return {
                key: tabKey,
                closable: fields.length > 1,
                label: (
                  <Form.Item
                    noStyle
                    name={[field.name, "module_name"]}
                    rules={[
                      { required: true, message: "请输入 module_name" },
                      { whitespace: true, message: "module_name 不能为空" },
                    ]}
                  >
                    <ModuleNameTabLabel
                      tabKey={tabKey}
                      placeholder={`Module ${field.name + 1}`}
                      isEditing={editingModuleTab === tabKey}
                      isActive={activeKey === tabKey}
                      onActivate={() => setActiveModuleTab(tabKey)}
                      onStartEditing={() => setEditingModuleTab(tabKey)}
                      onEditingComplete={() => setEditingModuleTab(undefined)}
                    />
                  </Form.Item>
                ),
                children: (
                  <Space
                    direction="vertical"
                    size={12}
                    style={{ display: "flex" }}
                  >
                    <Form.Item
                      label={renderRhaiSourceLabel(
                        sourceErrorForField(field.name),
                      )}
                      name={[field.name, "source"]}
                      rules={[
                        { required: true, message: "请输入 Rhai source" },
                        { whitespace: true, message: "source 不能为空" },
                      ]}
                    >
                      <RhaiEditor />
                    </Form.Item>
                  </Space>
                ),
              };
            });

            return (
              <Tabs
                items={tabItems}
                activeKey={activeKey}
                onChange={setActiveModuleTab}
                type="editable-card"
                hideAdd
                onEdit={(targetKey, action) => {
                  if (action === "add") {
                    handleAddModule();
                    return;
                  }

                  handleRemoveModule(targetKey);
                }}
                tabBarGutter={8}
                tabBarExtraContent={
                  <Space>
                    <Button onClick={handleAddModule}>添加 Module</Button>
                  </Space>
                }
              />
            );
          }}
        </Form.List>
      </Form>
    </Modal>
  );
}
