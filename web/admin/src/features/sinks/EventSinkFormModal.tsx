import { useEffect } from "react";
import { Form, Input, Modal, Select, Switch } from "antd";
import type {
  AutoOffsetReset,
  DeliveryTarget,
  EventSink,
  EventSinkFormValues,
  SinkTypeMetadata,
} from "./types";
import {
  eventSinkToFormValues,
  getDeliveryTargetTypeLabel,
  parseJsonObject,
  stringifyJsonObject,
} from "./utils";

type EventSinkFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  sink?: EventSink | null;
  targets: DeliveryTarget[];
  sinkTypes: SinkTypeMetadata[];
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: EventSinkFormValues) => Promise<void>;
};

const autoOffsetResetOptions: Array<{ label: string; value: AutoOffsetReset }> = [
  { label: "latest", value: "latest" },
  { label: "earliest", value: "earliest" },
];

export function EventSinkFormModal({
  open,
  mode,
  sink,
  targets,
  sinkTypes,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: EventSinkFormModalProps) {
  const [form] = Form.useForm<EventSinkFormValues>();
  const targetOptions = targets.map((target) => ({
    label: `${target.name} (${target.target_id}, ${getDeliveryTargetTypeLabel(
      target.target_type,
      sinkTypes,
    )})${target.enabled ? "" : " (disabled)"}`,
    value: target.id,
  }));

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(eventSinkToFormValues(sink));
  }, [form, open, sink]);

  const handleOk = async () => {
    const values = await form.validateFields();
    const destination = parseJsonObject(values.destination_json, "Destination config");
    await onSubmit({
      ...values,
      sink_id: values.sink_id.trim(),
      name: values.name.trim(),
      destination_json: stringifyJsonObject(destination),
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={640}
      title={mode === "create" ? "Create Event Sink" : "Edit Event Sink"}
      okText={mode === "create" ? "Create" : "Save"}
      cancelText="Cancel"
      confirmLoading={confirmLoading}
      onCancel={onCancel}
      onOk={() => {
        void handleOk().catch(() => {});
      }}
    >
      <Form<EventSinkFormValues>
        form={form}
        layout="vertical"
        initialValues={eventSinkToFormValues(sink)}
      >
        <Form.Item<EventSinkFormValues>
          label="Sink ID"
          name="sink_id"
          rules={[
            { required: true, message: "Please enter sink_id" },
            { whitespace: true, message: "sink_id cannot be empty" },
          ]}
        >
          <Input placeholder="For example: events" disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Display Name"
          name="name"
          rules={[
            { required: true, message: "Please enter display name" },
            { whitespace: true, message: "Display name cannot be empty" },
          ]}
        >
          <Input placeholder="For example: main event stream" maxLength={120} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Delivery Target"
          name="delivery_target_id"
          rules={[{ required: true, message: "Please select a delivery target" }]}
        >
          <Select
            showSearch
            placeholder="Select delivery target"
            options={targetOptions}
            optionFilterProp="label"
          />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Destination Config JSON"
          name="destination_json"
          rules={[
            {
              validator: (_, value: string | undefined) => {
                parseJsonObject(value ?? "", "Destination config");
                return Promise.resolve();
              },
            },
          ]}
        >
          <Input.TextArea
            rows={10}
            spellCheck={false}
            onBlur={() => {
              try {
                const destination = parseJsonObject(
                  form.getFieldValue("destination_json") ?? "",
                  "Destination config",
                );
                form.setFieldValue(
                  "destination_json",
                  stringifyJsonObject(destination),
                );
              } catch {
                // The form validator renders the actionable error on save.
              }
            }}
          />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Auto Offset Reset"
          name="auto_offset_reset"
          rules={[{ required: true, message: "Please select auto_offset_reset" }]}
        >
          <Select options={autoOffsetResetOptions} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
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
