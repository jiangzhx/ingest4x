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
const { toCreateProcessorScriptPayload } = await import(
  resolveModuleUrl("../src/features/processors/utils.ts")
);

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

test("admin shell and router expose the processor management page", () => {
  assert.match(routerSource, /path: "\/processors"/);
  assert.match(routerSource, /component: ProcessorsPage/);
  assert.match(shellSource, /key: "\/processors"/);
  assert.match(shellSource, /Processor 管理/);
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
  assert.match(scriptFormSource, /entry_module/);
  assert.match(scriptFormSource, /DEFAULT_PROCESSOR_SOURCE/);
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
    /Processor 接口响应无效：status 不是支持的值/,
  );
});
