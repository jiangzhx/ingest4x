import { Modal, Space, Tabs, Typography } from "antd";
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
  const tabItems =
    detail?.modules.map((module) => ({
      key: module.module_name,
      label: module.module_name,
      children: (
        <Typography.Paragraph
          style={{
            margin: 0,
            whiteSpace: "pre-wrap",
            fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
          }}
        >
          {module.source}
        </Typography.Paragraph>
      ),
    })) ?? [];

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
              状态：<Typography.Text code>{detail.status}</Typography.Text>
            </Typography.Text>
            <Typography.Text>
              Entry：<Typography.Text code>{detail.entry_module}</Typography.Text>
            </Typography.Text>
            <Typography.Text>
              Checksum：<Typography.Text code>{detail.checksum}</Typography.Text>
            </Typography.Text>
            <Typography.Text type="secondary">
              激活时间：{formatProcessorTimestamp(detail.activated_at)}
            </Typography.Text>
          </Space>
          <Tabs items={tabItems} />
        </Space>
      ) : null}
    </Modal>
  );
}
