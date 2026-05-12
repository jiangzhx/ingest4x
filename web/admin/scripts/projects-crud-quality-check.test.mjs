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

const { createProject, normalizeProjectResponse, normalizeProjectsResponse } = await import(
  resolveModuleUrl("../src/features/projects/api.ts")
);
const { HttpError, requestJson } = await import(
  resolveModuleUrl("../src/shared/http.ts")
);

const projectsPageSource = readFileSync(
  new URL("../src/features/projects/ProjectsPage.tsx", import.meta.url),
  "utf8",
);
const projectsTableSource = readFileSync(
  new URL("../src/features/projects/ProjectsTable.tsx", import.meta.url),
  "utf8",
);
const projectFormSource = readFileSync(
  new URL("../src/features/projects/ProjectFormModal.tsx", import.meta.url),
  "utf8",
);
const projectProcessorPanelSource = readFileSync(
  new URL("../src/features/processors/ProjectProcessorPanel.tsx", import.meta.url),
  "utf8",
);

test("projects page serializes delete flow around a single deletingProjectId", () => {
  assert.match(
    projectsPageSource,
    /const \[deletingProjectId, setDeletingProjectId\] = useState<number \| null>\(null\);/,
  );
  assert.match(projectsPageSource, /if \(deletingProjectId\) \{\s*return;\s*\}/);
  assert.match(
    projectsPageSource,
    /setDeletingProjectId\(project\.id\);[\s\S]*await deleteProjectMutation\.mutateAsync\(project\.id\);[\s\S]*finally \{\s*setDeletingProjectId\(null\);\s*\}/,
  );
  assert.match(
    projectsPageSource,
    /<ProjectsTable[\s\S]*deletingProjectId=\{deletingProjectId\}[\s\S]*actionsDisabled=\{isDeletePending\}/,
  );
});

test("projects page resets create and update mutation state when modal lifecycle changes", () => {
  assert.match(
    projectsPageSource,
    /const resetFormMutationState = \(\) => \{\s*createProjectMutation\.reset\(\);\s*updateProjectMutation\.reset\(\);\s*\};/,
  );
  assert.match(
    projectsPageSource,
    /const handleCreateClick = \(\) => \{[\s\S]*resetFormMutationState\(\);[\s\S]*setModalMode\("create"\);/,
  );
  assert.match(
    projectsPageSource,
    /const handleEditClick = \(project: Project\) => \{[\s\S]*resetFormMutationState\(\);[\s\S]*setModalMode\("edit"\);/,
  );
  assert.match(
    projectsPageSource,
    /const handleCloseModal = \(\) => \{[\s\S]*resetFormMutationState\(\);[\s\S]*setIsFormOpen\(false\);/,
  );
});

test("project management owns processor binding and defaults to default", () => {
  assert.match(projectsPageSource, /useProcessorScriptsQuery\(\)/);
  assert.match(projectsPageSource, /useProjectProcessorsQuery\(\)/);
  assert.match(projectsPageSource, /useAssignProjectProcessorMutation\(\)/);
  assert.doesNotMatch(projectsPageSource, /useDeleteProjectProcessorMutation\(\)/);
  assert.match(projectsPageSource, /<ProjectProcessorPanel/);
  assert.match(
    projectsPageSource,
    /<ProjectsTable[\s\S]*processorScripts=\{processorScripts\}[\s\S]*processorBindings=\{processorBindings\}/,
  );
  assert.match(projectsTableSource, /<Tag>default<\/Tag>/);
  assert.match(projectFormSource, /processorSection/);
  assert.match(projectProcessorPanelSource, /script_key === "default"/);
  assert.match(projectProcessorPanelSource, /status === "active"/);
  assert.doesNotMatch(projectProcessorPanelSource, /__default__/);
  assert.doesNotMatch(projectProcessorPanelSource, /onUseDefault/);
});

test("projects api normalizes valid response payloads at runtime", () => {
  assert.deepEqual(
    normalizeProjectResponse({
      id: 7,
      name: "  Demo Project ",
      enabled: true,
      ingest_token: "igx_demo_token",
      ingest_token_prefix: "igx_demo...",
      created_at: 1700000000.8,
      updated_at: 1700000001.2,
    }),
    {
      id: 7,
      name: "Demo Project",
      enabled: true,
      ingest_token: "igx_demo_token",
      ingest_token_prefix: "igx_demo...",
      created_at: 1700000000,
      updated_at: 1700000001,
    },
  );

  assert.deepEqual(
    normalizeProjectsResponse([
      {
        id: 1,
        name: "A",
        enabled: false,
        ingest_token: "igx_a_token",
        ingest_token_prefix: "igx_a",
        created_at: 1,
        updated_at: 2,
      },
    ]),
    [
      {
        id: 1,
        name: "A",
        enabled: false,
        ingest_token: "igx_a_token",
        ingest_token_prefix: "igx_a",
        created_at: 1,
        updated_at: 2,
      },
    ],
  );
});

test("project creation keeps full ingest token visible and copyable", async () => {
  assert.doesNotMatch(projectsPageSource, /projectTokens/);
  assert.doesNotMatch(projectsPageSource, /setProjectTokens/);
  assert.match(projectsTableSource, /const tokenText = project\.ingest_token;/);
  assert.match(projectsTableSource, /\{tokenText\}\s*<\/Typography\.Text>/);
  assert.match(projectsTableSource, /CopyOutlined/);
  assert.match(projectsTableSource, /aria-label="Copy token"/);
  assert.doesNotMatch(projectsTableSource, /disabled=\{!tokenText\}/);
  assert.match(projectsTableSource, /handleCopyToken\(tokenText\)/);
  assert.doesNotMatch(projectsTableSource, /copyable=/);
  assert.match(projectFormSource, /regenerate_ingest_token/);
  assert.match(projectFormSource, /Regenerate token when saving/);

  const originalFetch = globalThis.fetch;

  try {
    globalThis.fetch = async () =>
      new Response(
        JSON.stringify({
          id: 3,
          name: "Created Project",
          enabled: true,
          ingest_token: "igx_created_full_token",
          ingest_token_prefix: "igx_created_...",
          created_at: 10,
          updated_at: 11,
        }),
        {
          status: 201,
          headers: {
            "content-type": "application/json",
          },
        },
      );

    assert.deepEqual(
      await createProject({
        name: "Created Project",
        enabled: true,
      }),
      {
        id: 3,
        name: "Created Project",
        enabled: true,
        ingest_token: "igx_created_full_token",
        ingest_token_prefix: "igx_created_...",
        created_at: 10,
        updated_at: 11,
      },
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("project token copy falls back when browser clipboard is unavailable", () => {
  assert.match(projectsTableSource, /async function copyTextToClipboard/);
  assert.match(projectsTableSource, /navigator\.clipboard/);
  assert.match(projectsTableSource, /clipboard\.writeText\(text\)/);
  assert.match(projectsTableSource, /document\.execCommand\("copy"\)/);
  assert.match(projectsTableSource, /message\.success\("Token copied"\)/);
  assert.match(projectsTableSource, /message\.error\("Failed to copy token, please copy manually"\)/);
});

  test("projects api rejects invalid response payloads at runtime", () => {
    assert.throws(
      () => normalizeProjectsResponse({ items: [] }),
      /Invalid project API response: project list is not an array/,
    );
  assert.throws(
    () =>
      normalizeProjectResponse({
        id: 1,
        name: "Demo Project",
        enabled: "yes",
        ingest_token: "igx_demo_token",
        ingest_token_prefix: "igx_demo",
        created_at: 1,
        updated_at: 2,
      }),
    /Invalid project API response: enabled is missing or not a boolean/,
  );
  assert.throws(
    () =>
      normalizeProjectResponse({
        id: 1,
        name: "Demo Project",
        enabled: true,
        ingest_token: "igx_demo_token",
        ingest_token_prefix: "   ",
        created_at: 1,
        updated_at: 2,
      }),
    /Invalid project API response: ingest_token_prefix cannot be empty/,
  );
  assert.throws(
    () =>
      normalizeProjectResponse({
        id: 1,
        name: "Demo Project",
        enabled: true,
        ingest_token: "igx_demo_token",
        ingest_token_prefix: "igx_demo",
        created_at: -1,
        updated_at: 2,
      }),
    /Invalid project API response: created_at is missing or not a valid timestamp/,
  );
});

test("shared http requestJson throws stable runtime errors for non-json and invalid json responses", async () => {
  const originalFetch = globalThis.fetch;

  try {
    globalThis.fetch = async () =>
      new Response("plain text", {
        status: 200,
        headers: {
          "content-type": "text/plain",
        },
      });

    await assert.rejects(
      requestJson("/api/admin/projects"),
      (error) =>
        error instanceof HttpError &&
        error.status === 200 &&
        error.message === "Response is not JSON",
      );

    globalThis.fetch = async () =>
      new Response("{", {
        status: 200,
        headers: {
          "content-type": "application/json; charset=utf-8",
        },
      });

    await assert.rejects(
      requestJson("/api/admin/projects"),
      (error) =>
        error instanceof HttpError &&
        error.status === 200 &&
        error.message === "Failed to parse JSON response",
      );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
