import { useEffect } from "react";
import { Form, Input, Modal, Select, Switch } from "antd";
import type {
  DeliveryTarget,
  DeliveryTargetFormValues,
  SinkTypeMetadata,
} from "./types";
import {
  deliveryTargetToFormValues,
  parseJsonObject,
  stringifyJsonObject,
} from "./utils";

type DeliveryTargetFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  target?: DeliveryTarget | null;
  sinkTypes: SinkTypeMetadata[];
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: DeliveryTargetFormValues) => Promise<void>;
};

export function DeliveryTargetFormModal({
  open,
  mode,
  target,
  sinkTypes,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: DeliveryTargetFormModalProps) {
  const [form] = Form.useForm<DeliveryTargetFormValues>();
  const targetTypeOptions = sinkTypes.map((sinkType) => ({
    label: sinkType.label,
    value: sinkType.target_type,
  }));

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    const nextValues = deliveryTargetToFormValues(target);
    if (!target && sinkTypes[0]) {
      nextValues.target_type = sinkTypes[0].target_type;
    }
    form.setFieldsValue(nextValues);
  }, [form, open, sinkTypes, target]);

  const handleOk = async () => {
    const values = await form.validateFields();
    const config = parseJsonObject(values.config_json, "连接配置");
    await onSubmit({
      ...values,
      target_id: values.target_id.trim(),
      name: values.name.trim(),
      config_json: stringifyJsonObject(config),
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={680}
      title={mode === "create" ? "创建 Delivery Target" : "编辑 Delivery Target"}
      okText={mode === "create" ? "创建" : "保存"}
      cancelText="取消"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<DeliveryTargetFormValues>
        form={form}
        layout="vertical"
        initialValues={deliveryTargetToFormValues(target)}
      >
        <Form.Item<DeliveryTargetFormValues>
          label="Target ID"
          name="target_id"
          rules={[
            { required: true, message: "请输入 target_id" },
            { whitespace: true, message: "target_id 不能为空" },
          ]}
        >
          <Input placeholder="例如：kafka_main" disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="展示名"
          name="name"
          rules={[
            { required: true, message: "请输入展示名" },
            { whitespace: true, message: "展示名不能为空" },
          ]}
        >
          <Input placeholder="例如：主 Kafka 集群" maxLength={120} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="类型"
          name="target_type"
          rules={[{ required: true, message: "请选择类型" }]}
        >
          <Select options={targetTypeOptions} disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="连接配置 JSON"
          name="config_json"
          rules={[
            {
              validator: (_, value: string | undefined) => {
                parseJsonObject(value ?? "", "连接配置");
                return Promise.resolve();
              },
            },
          ]}
        >
          <Input.TextArea
            rows={12}
            spellCheck={false}
            onBlur={() => {
              try {
                const config = parseJsonObject(
                  form.getFieldValue("config_json") ?? "",
                  "连接配置",
                );
                form.setFieldValue("config_json", stringifyJsonObject(config));
              } catch {
                // The form validator renders the actionable error on save.
              }
            }}
          />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
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
