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
  normalizeServiceNodeResponse,
  normalizeServiceNodesResponse,
} = await import(resolveModuleUrl("../src/features/service-nodes/api.ts"));

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);
const shellSource = readFileSync(
  new URL("../src/layouts/AdminShell.tsx", import.meta.url),
  "utf8",
);
const homeSource = readFileSync(
  new URL("../src/pages/HomePage.tsx", import.meta.url),
  "utf8",
);
const pageSource = readFileSync(
  new URL("../src/features/service-nodes/ServiceNodesPage.tsx", import.meta.url),
  "utf8",
);
const tableSource = readFileSync(
  new URL("../src/features/service-nodes/ServiceNodesTable.tsx", import.meta.url),
  "utf8",
);

test("admin shell and router expose the service nodes page", () => {
  assert.match(routerSource, /path: "\/service-nodes"/);
  assert.match(routerSource, /component: ServiceNodesPage/);
  assert.match(shellSource, /key: "\/service-nodes"/);
  assert.match(shellSource, /节点管理/);
  assert.match(homeSource, /to="\/service-nodes"/);
});

test("service nodes page uses query data with manual refresh", () => {
  assert.match(pageSource, /useServiceNodesQuery\(\)/);
  assert.match(pageSource, /<ServiceNodesTable/);
  assert.match(pageSource, /serviceNodesQuery\.refetch\(\)/);
  assert.match(pageSource, /节点管理/);
});

test("service nodes table shows node identity, addresses, status and heartbeat", () => {
  const nodeIdColumnSource = tableSource.slice(
    tableSource.indexOf('title: "Node ID"'),
    tableSource.indexOf('title: "状态"'),
  );

  assert.match(tableSource, /node_id/);
  assert.doesNotMatch(nodeIdColumnSource, /width:/);
  assert.match(nodeIdColumnSource, /whiteSpace:\s*"nowrap"/);
  assert.doesNotMatch(nodeIdColumnSource, /wordBreak:\s*"break-all"/);
  assert.match(tableSource, /ingest_bind_address/);
  assert.match(tableSource, /management_bind_address/);
  assert.match(tableSource, /last_seen_at/);
  assert.match(tableSource, /getServiceNodeStatusLabel/);
});

test("service nodes api normalizes valid response payloads at runtime", () => {
  assert.deepEqual(
    normalizeServiceNodeResponse({
      node_id: " node-a ",
      hostname: " ingest-a ",
      machine_ip: "10.0.0.1",
      ingest_bind_address: "0.0.0.0:8090",
      management_bind_address: "127.0.0.1:18090",
      version: "0.0.1",
      status: "running",
      started_at: 10.8,
      last_seen_at: 20.2,
      updated_at: 21,
      metadata_json: { zone: "az-a" },
    }),
    {
      node_id: "node-a",
      hostname: "ingest-a",
      machine_ip: "10.0.0.1",
      ingest_bind_address: "0.0.0.0:8090",
      management_bind_address: "127.0.0.1:18090",
      version: "0.0.1",
      status: "running",
      started_at: 10,
      last_seen_at: 20,
      updated_at: 21,
      metadata_json: { zone: "az-a" },
    },
  );

  assert.equal(normalizeServiceNodesResponse([]).length, 0);
});

test("service nodes api rejects invalid response payloads at runtime", () => {
  assert.throws(
    () => normalizeServiceNodesResponse({ items: [] }),
    /节点接口响应无效：节点列表不是数组/,
  );
  assert.throws(
    () =>
      normalizeServiceNodeResponse({
        node_id: "node-a",
        hostname: null,
        machine_ip: null,
        ingest_bind_address: "0.0.0.0:8090",
        management_bind_address: "127.0.0.1:18090",
        version: "0.0.1",
        status: "unknown",
        started_at: 10,
        last_seen_at: 20,
        updated_at: 21,
        metadata_json: null,
      }),
    /节点接口响应无效：status 不是支持的值/,
  );
});
