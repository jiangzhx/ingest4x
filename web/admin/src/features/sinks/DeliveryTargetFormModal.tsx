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
    const config = parseJsonObject(values.config_json, "Connection config");
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
      title={mode === "create" ? "Create Delivery Target" : "Edit Delivery Target"}
      okText={mode === "create" ? "Create" : "Save"}
      cancelText="Cancel"
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
            { required: true, message: "Please enter target_id" },
            { whitespace: true, message: "target_id cannot be empty" },
          ]}
        >
          <Input placeholder="For example: kafka_main" disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="Display Name"
          name="name"
          rules={[
            { required: true, message: "Please enter display name" },
            { whitespace: true, message: "Display name cannot be empty" },
          ]}
        >
          <Input placeholder="For example: primary Kafka cluster" maxLength={120} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="Type"
          name="target_type"
          rules={[{ required: true, message: "Please select a type" }]}
        >
          <Select options={targetTypeOptions} disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
          label="Connection Config JSON"
          name="config_json"
          rules={[
            {
              validator: (_, value: string | undefined) => {
                parseJsonObject(value ?? "", "Connection config");
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
                  "Connection config",
                );
                form.setFieldValue("config_json", stringifyJsonObject(config));
              } catch {
                // The form validator renders the actionable error on save.
              }
            }}
          />
        </Form.Item>
        <Form.Item<DeliveryTargetFormValues>
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
