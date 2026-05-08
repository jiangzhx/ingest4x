import { useEffect } from "react";
import { Form, Input, Modal, Select, Switch } from "antd";
import type {
  AutoOffsetReset,
  DeliveryTarget,
  EventSink,
  EventSinkFormValues,
} from "./types";
import { eventSinkToFormValues, getDeliveryTargetTypeLabel } from "./utils";

type EventSinkFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  sink?: EventSink | null;
  targets: DeliveryTarget[];
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
  confirmLoading = false,
  onCancel,
  onSubmit,
}: EventSinkFormModalProps) {
  const [form] = Form.useForm<EventSinkFormValues>();
  const selectedTargetId = Form.useWatch("delivery_target_id", form);
  const selectedTarget =
    targets.find((target) => target.id === selectedTargetId) ?? null;
  const targetOptions = targets.map((target) => ({
    label: `${target.name} (${target.target_id}, ${getDeliveryTargetTypeLabel(
      target.target_type,
    )})${target.enabled ? "" : "（已停用）"}`,
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
    await onSubmit({
      ...values,
      sink_id: values.sink_id.trim(),
      name: values.name.trim(),
      topic: values.topic.trim(),
    });
  };

  return (
    <Modal
      destroyOnHidden
      open={open}
      width={640}
      title={mode === "create" ? "创建 Event Sink" : "编辑 Event Sink"}
      okText={mode === "create" ? "创建" : "保存"}
      cancelText="取消"
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
            { required: true, message: "请输入 sink_id" },
            { whitespace: true, message: "sink_id 不能为空" },
          ]}
        >
          <Input placeholder="例如：events" disabled={mode === "edit"} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="展示名"
          name="name"
          rules={[
            { required: true, message: "请输入展示名" },
            { whitespace: true, message: "展示名不能为空" },
          ]}
        >
          <Input placeholder="例如：主事件流" maxLength={120} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Delivery Target"
          name="delivery_target_id"
          rules={[{ required: true, message: "请选择 delivery target" }]}
        >
          <Select
            showSearch
            placeholder="选择 delivery target"
            options={targetOptions}
            optionFilterProp="label"
          />
        </Form.Item>
        {selectedTarget?.target_type === "kafka" ? (
          <Form.Item<EventSinkFormValues>
            label="Kafka Topic"
            name="topic"
            rules={[
              { required: true, message: "请输入 Kafka topic" },
              { whitespace: true, message: "topic 不能为空" },
            ]}
          >
            <Input placeholder="ingest4x-events" />
          </Form.Item>
        ) : null}
        <Form.Item<EventSinkFormValues>
          label="投递目标附加配置"
          name="destination_json"
        >
          <Input.TextArea rows={4} spellCheck={false} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
          label="Auto Offset Reset"
          name="auto_offset_reset"
          rules={[{ required: true, message: "请选择 auto_offset_reset" }]}
        >
          <Select options={autoOffsetResetOptions} />
        </Form.Item>
        <Form.Item<EventSinkFormValues>
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
