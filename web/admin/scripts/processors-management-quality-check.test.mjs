import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { extname, dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { registerHooks, stripTypeScriptTypes } from "node:module";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));

function resolveModuleUrl(relativePath) {
  return pathToFileURL(resolve(scriptDirectory, relativePath)).href;
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      context.parentURL?.startsWith("file:") &&
      specifier.startsWith(".") &&
      !extname(specifier)
    ) {
      return nextResolve(`${specifier}.ts`, context);
    }

    return nextResolve(specifier, context);
  },
  load(url, context, nextLoad) {
    if (url.endsWith(".ts") || url.endsWith(".tsx")) {
      const source = readFileSync(new URL(url), "utf8");

      return {
        format: "module",
        shortCircuit: true,
        source: stripTypeScriptTypes(source, {
          mode: "transform",
        }),
      };
    }

    return nextLoad(url, context);
  },
});

const {
  normalizeProcessorScriptResponse,
} = await import(resolveModuleUrl("../src/features/processors/api.ts"));
const {
  toCreateProcessorScriptPayload,
  toUpdateProcessorScriptPayload,
  toValidateProcessorScriptPayload,
} = await import(resolveModuleUrl("../src/features/processors/utils.ts"));

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);
const shellSource = readFileSync(
  new URL("../src/layouts/AdminShell.tsx", import.meta.url),
  "utf8",
);
const processorsPageSource = readFileSync(
  new URL("../src/features/processors/ProcessorsPage.tsx", import.meta.url),
  "utf8",
);
const scriptFormSource = readFileSync(
  new URL("../src/features/processors/ProcessorScriptFormModal.tsx", import.meta.url),
  "utf8",
);
const processorApiSource = readFileSync(
  new URL("../src/features/processors/api.ts", import.meta.url),
  "utf8",
);
const scriptDetailSource = readFileSync(
  new URL("../src/features/processors/ProcessorScriptDetailModal.tsx", import.meta.url),
  "utf8",
);
const rhaiEditorSource = readFileSync(
  new URL("../src/features/processors/RhaiEditor.tsx", import.meta.url),
  "utf8",
);
const packageSource = readFileSync(
  new URL("../package.json", import.meta.url),
  "utf8",
);

test("admin shell and router expose the processor management page", () => {
  assert.match(routerSource, /path: "\/processors"/);
  assert.match(routerSource, /component: ProcessorsPage/);
  assert.match(shellSource, /key: "\/processors"/);
  assert.match(shellSource, /Processor Management/);
});

test("processors page manages script versions only", () => {
  assert.match(processorsPageSource, /useProcessorScriptsQuery\(\)/);
  assert.match(processorsPageSource, /<ProcessorScriptsTable/);
  assert.doesNotMatch(processorsPageSource, /useProjectProcessorsQuery\(\)/);
  assert.doesNotMatch(processorsPageSource, /useProjectsQuery\(\)/);
  assert.doesNotMatch(processorsPageSource, /ProjectProcessorsTable/);
});

test("processor script form supports multi-module Rhai scripts", () => {
  assert.match(scriptFormSource, /Form\.List name="modules"/);
  assert.match(scriptFormSource, /\bTabs\b/);
  assert.match(scriptFormSource, /<Tabs[\s\S]*items=\{tabItems\}/);
  assert.match(scriptFormSource, /type="editable-card"/);
  assert.match(scriptFormSource, /hideAdd/);
  assert.match(scriptFormSource, /onEdit=\{\(targetKey, action\) =>/);
  assert.match(scriptFormSource, /tabBarGutter=\{8\}/);
  assert.match(scriptFormSource, /const orderedFields = \[\.\.\.fields\]\.sort/);
  assert.match(scriptFormSource, /fieldModuleName\(left\) === "main"/);
  assert.match(scriptFormSource, /const tabItems = orderedFields\.map/);
  assert.match(scriptFormSource, /closable: fields\.length > 1/);
  assert.match(scriptFormSource, /label:\s*\([\s\S]*<Form\.Item[\s\S]*noStyle[\s\S]*name=\{\[field\.name, "module_name"\]\}/);
  assert.match(scriptFormSource, /<ModuleNameTabLabel/);
  assert.match(scriptFormSource, /editingModuleTab/);
  assert.match(scriptFormSource, /isActive=\{activeKey === tabKey\}/);
  assert.match(scriptFormSource, /setEditingModuleTab\(tabKey\)/);
  assert.doesNotMatch(scriptFormSource, /backgroundColor: isActive/);
  assert.doesNotMatch(scriptFormSource, /border: isActive/);
  assert.doesNotMatch(scriptFormSource, /borderRadius: 4/);
  assert.match(scriptFormSource, /fontWeight: isActive \? 600 : 400/);
  assert.doesNotMatch(scriptFormSource, /hoveredModuleTab/);
  assert.doesNotMatch(scriptFormSource, /minWidth: 132/);
  assert.doesNotMatch(scriptFormSource, />\s*Edit\s*<\/Button>/);
  assert.doesNotMatch(scriptFormSource, />Rhai Source</);
  assert.doesNotMatch(scriptFormSource, /label="Module Name"[\s\S]{0,80}name=\{\[field\.name, "module_name"\]\}/);
  assert.match(scriptFormSource, /tabBarExtraContent=\{[\s\S]*<Space/);
  assert.match(scriptFormSource, /tabBarExtraContent=\{[\s\S]*Add Module/);
  assert.doesNotMatch(scriptFormSource, />\s*Delete Module\s*</);
  assert.match(scriptFormSource, /nextModuleName/);
  assert.match(scriptFormSource, /moduleNameCandidates\.has\(`module\$\{index\}`\)/);
  assert.match(scriptFormSource, /module_name: nextModuleName\(watchedModules\)/);
  assert.doesNotMatch(scriptFormSource, /module_name: "custom"/);
  assert.match(scriptFormSource, /entry_module/);
  assert.match(scriptFormSource, /DEFAULT_PROCESSOR_SOURCE/);
  assert.match(scriptFormSource, /mode === "edit"/);
  assert.match(scriptFormSource, /initialValues/);
});

test("processor script source uses codemirror with javascript highlighting", () => {
  assert.match(packageSource, /"@uiw\/react-codemirror"/);
  assert.match(packageSource, /"@codemirror\/lang-javascript"/);
  assert.match(scriptFormSource, /<RhaiEditor/);
  assert.doesNotMatch(scriptFormSource, /Input\.TextArea/);
  assert.match(rhaiEditorSource, /from "@uiw\/react-codemirror"/);
  assert.match(rhaiEditorSource, /from "@codemirror\/lang-javascript"/);
  assert.match(rhaiEditorSource, /javascript\(/);
});

test("processor script detail uses readonly highlighted source viewer", () => {
  assert.match(scriptDetailSource, /<RhaiEditor/);
  assert.match(scriptDetailSource, /readOnly/);
  assert.match(scriptDetailSource, /const orderedModules = \[\.\.\.\(detail\?\.modules \?\? \[\]\)\]\.sort/);
  assert.match(scriptDetailSource, /left\.module_name === "main"/);
  assert.match(scriptDetailSource, /const tabItems = orderedModules\.map/);
  assert.match(scriptDetailSource, /closable: false/);
  assert.match(scriptDetailSource, /<Tabs[\s\S]*items=\{tabItems\}[\s\S]*type="editable-card"[\s\S]*hideAdd[\s\S]*tabBarGutter=\{8\}/);
  assert.doesNotMatch(scriptDetailSource, /<Typography\.Paragraph[\s\S]*module\.source/);
  assert.match(rhaiEditorSource, /readOnly\?: boolean/);
  assert.match(rhaiEditorSource, /editable=\{!readOnly\}/);
});

test("processor script payload trims identities and preserves source", () => {
  assert.deepEqual(
    toCreateProcessorScriptPayload({
      script_key: " payment ",
      name: " Payment ",
      entry_module: " main ",
      status: "active",
      modules: [
        {
          module_name: " main ",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    }),
    {
      script_key: "payment",
      name: "Payment",
      entry_module: "main",
      status: "active",
      modules: [
        {
          module_name: "main",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    },
  );
});

test("processor script edit updates existing script through put endpoint", () => {
  assert.match(processorsPageSource, /editingScript/);
  assert.match(processorsPageSource, /useUpdateProcessorScriptMutation\(\)/);
  assert.match(processorsPageSource, /onEdit=/);
  assert.match(processorsPageSource, /toProcessorScriptFormValues/);
  assert.match(processorsPageSource, /toUpdateProcessorScriptPayload/);
  assert.match(processorApiSource, /updateProcessorScript/);
  assert.match(processorApiSource, /method: "PUT"/);
  assert.match(scriptFormSource, /title=\{mode === "edit"/);
  assert.match(scriptFormSource, /disabled=\{mode === "edit"\}/);

  assert.deepEqual(
    toUpdateProcessorScriptPayload({
      script_key: " ignored ",
      name: " Updated ",
      entry_module: " main ",
      status: "active",
      modules: [
        {
          module_name: " main ",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    }),
    {
      name: "Updated",
      entry_module: "main",
      status: "active",
      modules: [
        {
          module_name: "main",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    },
  );
});

test("processor script edit keeps draft source when validation rerenders the page", () => {
  assert.match(processorsPageSource, /useMemo/);
  assert.match(processorsPageSource, /const editingInitialValues = useMemo/);
  assert.match(
    processorsPageSource,
    /toProcessorScriptFormValues\(editingDetail\)[\s\S]*\[\s*editingDetail\s*\]/,
  );
  assert.match(processorsPageSource, /initialValues=\{editingInitialValues\}/);
  assert.doesNotMatch(
    processorsPageSource,
    /initialValues=\{\s*editingDetail === null[\s\S]*toProcessorScriptFormValues\(editingDetail\)[\s\S]*\}/,
  );
});

test("processor script form validates Rhai syntax before create and update", () => {
  assert.match(processorApiSource, /validateProcessorScript/);
  assert.match(processorApiSource, /\/api\/admin\/processor-scripts\/validate/);
  assert.match(processorsPageSource, /useValidateProcessorScriptMutation\(\)/);
  assert.match(processorsPageSource, /handleValidateScript/);
  assert.match(processorsPageSource, /scriptValidationError/);
  assert.match(processorsPageSource, /setScriptValidationError/);
  assert.match(processorsPageSource, /validationError=\{scriptValidationError\}/);
  assert.match(scriptFormSource, /onValidate/);
  assert.match(scriptFormSource, /validationError/);
  assert.match(scriptFormSource, /extractValidationModuleName/);
  assert.match(scriptFormSource, /Rhai module `\(\[\^`\]\+\)`/);
  assert.match(scriptFormSource, /renderRhaiSourceLabel/);
  assert.match(scriptFormSource, /label=\{renderRhaiSourceLabel/);
  assert.match(scriptFormSource, /sourceErrorForField/);
  assert.doesNotMatch(scriptFormSource, /<Alert/);
  assert.match(scriptFormSource, /footer=/);
  assert.match(
    scriptFormSource,
    /Cancel[\s\S]*\{mode === "edit" \? "Save" : "Create"\}/,
  );
  assert.doesNotMatch(
    scriptFormSource,
    /label="Module Name"[\s\S]*label="Rhai Source"/,
  );
  assert.doesNotMatch(scriptFormSource, /Popconfirm/);
  assert.doesNotMatch(scriptFormSource, /title="Confirm to delete this module\?"/);
  assert.match(scriptFormSource, /const handleRemoveModule = \(targetKey: EditableTabTargetKey\) =>/);
  assert.match(scriptFormSource, /remove\(targetField\.name\)/);
  assert.match(scriptFormSource, /await onValidate/);
  assert.match(scriptFormSource, /await validateScript\(\)/);

  assert.deepEqual(
    toValidateProcessorScriptPayload({
      script_key: " ignored ",
      name: " Ignored ",
      entry_module: " main ",
      status: "active",
      modules: [
        {
          module_name: " main ",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    }),
    {
      entry_module: "main",
      modules: [
        {
          module_name: "main",
          source: "fn process(event, request) {\n    emit(\"events\", event);\n}",
        },
      ],
    },
  );
});

test("processor api normalizes script responses at runtime", () => {
  assert.deepEqual(
    normalizeProcessorScriptResponse({
      id: 1,
      script_key: " default ",
      name: " Default ",
      entry_module: " main ",
      version: 1,
      status: "active",
      checksum: " abc123 ",
      created_at: 10.8,
      updated_at: 11.2,
      activated_at: null,
    }),
    {
      id: 1,
      script_key: "default",
      name: "Default",
      entry_module: "main",
      version: 1,
      status: "active",
      checksum: "abc123",
      created_at: 10,
      updated_at: 11,
      activated_at: null,
    },
  );

  assert.throws(
    () =>
      normalizeProcessorScriptResponse({
        id: 1,
        script_key: "default",
        name: "Default",
        entry_module: "main",
        version: 1,
        status: "deleted",
        checksum: "abc123",
        created_at: 10,
        updated_at: 11,
        activated_at: null,
      }),
    /Invalid Processor API response: status is not a supported value/,
  );
});
