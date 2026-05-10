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
  normalizeDeliveryTargetResponse,
  normalizeDeliveryTargetsResponse,
  normalizeEventSinkResponse,
  normalizeEventSinksResponse,
  normalizeSinkTypesResponse,
} = await import(resolveModuleUrl("../src/features/sinks/api.ts"));
const {
  getDeliveryTargetTypeLabel,
  toCreateDeliveryTargetPayload,
  toUpdateEventSinkPayload,
} = await import(resolveModuleUrl("../src/features/sinks/utils.ts"));

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);
const shellSource = readFileSync(
  new URL("../src/layouts/AdminShell.tsx", import.meta.url),
  "utf8",
);
const sinksPageSource = readFileSync(
  new URL("../src/features/sinks/SinksPage.tsx", import.meta.url),
  "utf8",
);
const targetFormSource = readFileSync(
  new URL("../src/features/sinks/DeliveryTargetFormModal.tsx", import.meta.url),
  "utf8",
);
const sinkFormSource = readFileSync(
  new URL("../src/features/sinks/EventSinkFormModal.tsx", import.meta.url),
  "utf8",
);

test("admin shell and router expose the sink management page", () => {
  assert.match(routerSource, /path: "\/sinks"/);
  assert.match(routerSource, /component: SinksPage/);
  assert.match(shellSource, /key: "\/sinks"/);
  assert.match(shellSource, /Sink 管理/);
});

test("sinks page manages delivery targets and event sinks together", () => {
  assert.match(sinksPageSource, /useSinkTypesQuery\(\)/);
  assert.match(sinksPageSource, /useDeliveryTargetsQuery\(sinkTypes\)/);
  assert.match(sinksPageSource, /useEventSinksQuery\(\)/);
  assert.match(sinksPageSource, /<DeliveryTargetsTable/);
  assert.match(sinksPageSource, /<DeliveryTargetsTable[\s\S]*sinkTypes=\{sinkTypes\}/);
  assert.match(sinksPageSource, /<EventSinksTable/);
  assert.match(sinksPageSource, /<EventSinksTable[\s\S]*sinkTypes=\{sinkTypes\}/);
  assert.match(sinksPageSource, /createDeliveryTargetMutation/);
  assert.match(sinksPageSource, /createEventSinkMutation/);
});

test("sink forms only expose json configuration controls", () => {
  assert.match(targetFormSource, /target_type/);
  assert.match(targetFormSource, /config_json/);
  assert.match(sinkFormSource, /auto_offset_reset/);
  assert.match(sinkFormSource, /destination_json/);
  assert.match(sinkFormSource, /delivery_target_id/);
  assert.doesNotMatch(targetFormSource, /字段配置/);
  assert.doesNotMatch(targetFormSource, /bootstrap_servers/);
  assert.doesNotMatch(targetFormSource, /delivery_timeout_ms/);
  assert.doesNotMatch(targetFormSource, /queue_buffering_max_ms/);
  assert.doesNotMatch(targetFormSource, /batch_num_messages/);
  assert.doesNotMatch(targetFormSource, /queue_buffering_max_messages/);
  assert.doesNotMatch(targetFormSource, /linger_ms/);
  assert.doesNotMatch(sinkFormSource, /字段配置/);
  assert.doesNotMatch(sinkFormSource, /Kafka Topic/);
  assert.doesNotMatch(sinkFormSource, /topic/);
  assert.doesNotMatch(sinkFormSource, /<Select[\s\S]*disabled=\{mode === "edit"\}/);
});

test("sinks api normalizes registered sink type metadata", () => {
  assert.deepEqual(
    normalizeSinkTypesResponse([
      {
        target_type: "kafka",
        label: " Kafka "
      },
      {
        target_type: "clickhouse",
        label: "ClickHouse"
      },
    ]),
    [
      {
        target_type: "kafka",
        label: "Kafka"
      },
      {
        target_type: "clickhouse",
        label: "ClickHouse"
      },
    ],
  );
});

test("event sink update payload can change delivery target", () => {
  assert.deepEqual(
    toUpdateEventSinkPayload(
      {
        sink_id: "events",
        name: "Events",
        delivery_target_id: 2,
        destination_json: JSON.stringify({ topic: "ingest4x-events" }),
        auto_offset_reset: "latest",
        enabled: true,
      },
      [
        {
          id: 2,
          target_id: "kafka_main",
          name: "Main Kafka",
          target_type: "kafka",
          config_json: {},
          enabled: true,
          created_at: 1,
          updated_at: 1,
        },
      ],
    ),
    {
      name: "Events",
      delivery_target_id: 2,
      destination_json: { topic: "ingest4x-events" },
      auto_offset_reset: "latest",
      enabled: true,
    },
  );
});

test("non-kafka delivery target payload uses json config without kafka fields", () => {
  assert.deepEqual(
    toCreateDeliveryTargetPayload({
      target_id: "clickhouse_main",
      name: "ClickHouse",
      target_type: "clickhouse",
      config_json: JSON.stringify({
        endpoint: "http://127.0.0.1:8123",
        database: "events",
      }),
      enabled: true,
    }),
    {
      target_id: "clickhouse_main",
      name: "ClickHouse",
      target_type: "clickhouse",
      config_json: {
        endpoint: "http://127.0.0.1:8123",
        database: "events",
      },
      enabled: true,
    },
  );

  assert.equal(
    getDeliveryTargetTypeLabel("clickhouse", [
      { target_type: "clickhouse", label: "ClickHouse" },
    ]),
    "ClickHouse",
  );
  assert.equal(getDeliveryTargetTypeLabel("doris"), "doris");
});

test("sinks api normalizes valid response payloads at runtime", () => {
  assert.deepEqual(
    normalizeDeliveryTargetResponse(
      {
        id: 1,
        target_id: "  kafka_main ",
        name: " Main Kafka ",
        target_type: "kafka",
        config_json: { bootstrap_servers: "127.0.0.1:9092" },
        enabled: true,
        created_at: 10.8,
        updated_at: 11.2,
      },
      [
        {
          target_type: "kafka",
          label: "Kafka"
        },
      ],
    ),
    {
      id: 1,
      target_id: "kafka_main",
      name: "Main Kafka",
      target_type: "kafka",
      config_json: { bootstrap_servers: "127.0.0.1:9092" },
      enabled: true,
      created_at: 10,
      updated_at: 11,
    },
  );

  assert.deepEqual(
    normalizeEventSinkResponse({
      id: 2,
      sink_id: " events ",
      name: " Events ",
      delivery_target_id: 1,
      destination_json: { topic: "ingest4x-events" },
      auto_offset_reset: "latest",
      enabled: false,
      created_at: 20,
      updated_at: 21,
    }),
    {
      id: 2,
      sink_id: "events",
      name: "Events",
      delivery_target_id: 1,
      destination_json: { topic: "ingest4x-events" },
      auto_offset_reset: "latest",
      enabled: false,
      created_at: 20,
      updated_at: 21,
    },
  );

  assert.equal(normalizeDeliveryTargetsResponse([]).length, 0);
  assert.equal(normalizeEventSinksResponse([]).length, 0);
});

test("sinks api rejects invalid response payloads at runtime", () => {
  assert.throws(
    () => normalizeDeliveryTargetsResponse({ items: [] }),
    /Sink 接口响应无效：delivery target 列表不是数组/,
  );
  assert.throws(
    () =>
      normalizeSinkTypesResponse([
        {
          target_type: "kafka",
          label: "",
        },
      ]),
    /Sink 接口响应无效：label 不能为空/,
  );
  assert.throws(
    () =>
      normalizeDeliveryTargetResponse(
        {
          id: 1,
          target_id: "kafka_main",
          name: "Main Kafka",
          target_type: "redis",
          config_json: {},
          enabled: true,
          created_at: 1,
          updated_at: 2,
        },
        [
          {
            target_type: "kafka",
            label: "Kafka"
          },
        ],
      ),
    /Sink 接口响应无效：target_type 不是已注册的类型/,
  );
  assert.throws(
    () =>
      normalizeEventSinkResponse({
        id: 2,
        sink_id: "events",
        name: "Events",
        delivery_target_id: 1,
        destination_json: {},
        auto_offset_reset: "none",
        enabled: true,
        created_at: 1,
        updated_at: 2,
      }),
    /Sink 接口响应无效：auto_offset_reset 不是支持的值/,
  );
});
