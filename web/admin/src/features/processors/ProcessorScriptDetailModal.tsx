import { Modal, Space, Tabs, Typography } from "antd";
import { RhaiEditor } from "./RhaiEditor";
import type { ProcessorScriptDetail } from "./types";
import { formatProcessorTimestamp } from "./utils";

type ProcessorScriptDetailModalProps = {
  open: boolean;
  detail?: ProcessorScriptDetail | null;
  loading?: boolean;
  onCancel: () => void;
};

export function ProcessorScriptDetailModal({
  open,
  detail,
  loading = false,
  onCancel,
}: ProcessorScriptDetailModalProps) {
  const orderedModules = [...(detail?.modules ?? [])].sort((left, right) => {
    const leftIsMain = left.module_name === "main";
    const rightIsMain = right.module_name === "main";
    if (leftIsMain === rightIsMain) {
      return 0;
    }

    return leftIsMain ? -1 : 1;
  });
  const tabItems = orderedModules.map((module) => ({
    key: module.module_name,
    label: module.module_name,
    closable: false,
    children: <RhaiEditor value={module.source} height="360px" readOnly />,
  }));

  return (
    <Modal
      open={open}
      width={900}
      title={detail ? `${detail.script_key} v${detail.version}` : "Processor Script"}
      footer={null}
      loading={loading}
      onCancel={onCancel}
    >
      {detail ? (
        <Space direction="vertical" size={12} style={{ display: "flex" }}>
          <Space size={24} wrap>
            <Typography.Text>
              Status: <Typography.Text code>{detail.status}</Typography.Text>
            </Typography.Text>
            <Typography.Text>
              Entry：<Typography.Text code>{detail.entry_module}</Typography.Text>
            </Typography.Text>
            <Typography.Text>
              Checksum：<Typography.Text code>{detail.checksum}</Typography.Text>
            </Typography.Text>
            <Typography.Text type="secondary">
              Activated At: {formatProcessorTimestamp(detail.activated_at)}
            </Typography.Text>
          </Space>
          <Tabs items={tabItems} type="editable-card" hideAdd tabBarGutter={8} />
        </Space>
      ) : null}
    </Modal>
  );
}
