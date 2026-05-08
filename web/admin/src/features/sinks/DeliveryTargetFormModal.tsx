import { useEffect } from "react";
import { Form, Input, Modal, Select, Switch } from "antd";
import type {
  DeliveryTarget,
  DeliveryTargetFormValues,
  DeliveryTargetType,
} from "./types";
import { deliveryTargetToFormValues } from "./utils";

type DeliveryTargetFormModalProps = {
  open: boolean;
  mode: "create" | "edit";
  target?: DeliveryTarget | null;
  confirmLoading?: boolean;
  onCancel: () => void;
  onSubmit: (values: DeliveryTargetFormValues) => Promise<void>;
};

const targetTypeOptions: Array<{ label: string; value: DeliveryTargetType }> = [
  { label: "Kafka", value: "kafka" },
  { label: "stdout", value: "stdout" },
];

export function DeliveryTargetFormModal({
  open,
  mode,
  target,
  confirmLoading = false,
  onCancel,
  onSubmit,
}: DeliveryTargetFormModalProps) {
  const [form] = Form.useForm<DeliveryTargetFormValues>();
  const targetType = Form.useWatch("target_type", form) ?? "kafka";

  useEffect(() => {
    if (!open) {
      form.resetFields();
      return;
    }

    form.setFieldsValue(deliveryTargetToFormValues(target));
  }, [form, open, target]);

  const handleOk = async () => {
    const values = await form.validateFields();
    await onSubmit({
      ...values,
      target_id: values.target_id.trim(),
      name: values.name.trim(),
      bootstrap_servers: values.bootstrap_servers.trim(),
      delivery_timeout_ms: values.delivery_timeout_ms.trim(),
      queue_buffering_max_ms: values.queue_buffering_max_ms.trim(),
      batch_num_messages: values.batch_num_messages.trim(),
      queue_buffering_max_messages: values.queue_buffering_max_messages.trim(),
      linger_ms: values.linger_ms.trim(),
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
        {targetType === "kafka" ? (
          <>
            <Form.Item<DeliveryTargetFormValues>
              label="Bootstrap Servers"
              name="bootstrap_servers"
              rules={[
                { required: true, message: "请输入 Kafka bootstrap_servers" },
                { whitespace: true, message: "bootstrap_servers 不能为空" },
              ]}
            >
              <Input placeholder="127.0.0.1:9092" />
            </Form.Item>
            <Form.Item<DeliveryTargetFormValues>
              label="Delivery Timeout"
              name="delivery_timeout_ms"
              rules={[{ required: true, message: "请输入 delivery_timeout_ms" }]}
            >
              <Input placeholder="3000" />
            </Form.Item>
            <Form.Item<DeliveryTargetFormValues>
              label="Queue Buffering Max"
              name="queue_buffering_max_ms"
              rules={[{ required: true, message: "请输入 queue_buffering_max_ms" }]}
            >
              <Input placeholder="0" />
            </Form.Item>
            <Form.Item<DeliveryTargetFormValues>
              label="Batch Messages"
              name="batch_num_messages"
              rules={[{ required: true, message: "请输入 batch_num_messages" }]}
            >
              <Input placeholder="100" />
            </Form.Item>
            <Form.Item<DeliveryTargetFormValues>
              label="Queue Messages"
              name="queue_buffering_max_messages"
              rules={[
                { required: true, message: "请输入 queue_buffering_max_messages" },
              ]}
            >
              <Input placeholder="300" />
            </Form.Item>
            <Form.Item<DeliveryTargetFormValues>
              label="Linger"
              name="linger_ms"
              rules={[{ required: true, message: "请输入 linger_ms" }]}
            >
              <Input placeholder="100" />
            </Form.Item>
          </>
        ) : null}
        <Form.Item<DeliveryTargetFormValues> label="附加连接配置" name="config_json">
          <Input.TextArea rows={4} spellCheck={false} />
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
