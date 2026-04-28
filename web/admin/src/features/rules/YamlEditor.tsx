import CodeMirror from "@uiw/react-codemirror";
import { yaml } from "@codemirror/lang-yaml";

type YamlEditorProps = {
  value?: string;
  onChange?: (value: string) => void;
};

export function YamlEditor({ value = "", onChange }: YamlEditorProps) {
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
        height="320px"
        basicSetup={{
          foldGutter: true,
          lineNumbers: true,
          highlightActiveLine: true,
          highlightActiveLineGutter: true,
        }}
        extensions={[yaml()]}
        onChange={(nextValue) => onChange?.(nextValue)}
        style={{
          fontSize: 14,
        }}
      />
    </div>
  );
}
