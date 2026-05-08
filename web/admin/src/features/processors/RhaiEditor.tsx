import { javascript } from "@codemirror/lang-javascript";
import CodeMirror from "@uiw/react-codemirror";

type RhaiEditorProps = {
  value?: string;
  height?: string;
  readOnly?: boolean;
  onChange?: (value: string) => void;
};

export function RhaiEditor({
  value = "",
  height = "320px",
  readOnly = false,
  onChange,
}: RhaiEditorProps) {
  return (
    <div
      style={{
        border: "1px solid #d9d9d9",
        borderRadius: 8,
        overflow: "hidden",
      }}
    >
      <CodeMirror
        value={value}
        height={height}
        editable={!readOnly}
        readOnly={readOnly}
        basicSetup={{
          foldGutter: true,
          lineNumbers: true,
          highlightActiveLine: true,
          highlightActiveLineGutter: true,
        }}
        extensions={[javascript()]}
        onChange={(nextValue) => onChange?.(nextValue)}
        style={{
          fontSize: 14,
        }}
      />
    </div>
  );
}
