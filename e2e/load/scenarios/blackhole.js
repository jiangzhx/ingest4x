import http from "k6/http";
import { check } from "k6";

const rate = Number(__ENV.LOAD_RATE || "100");
const duration = __ENV.LOAD_DURATION || "1m";
const preAllocatedVUs = Number(__ENV.LOAD_PRE_ALLOCATED_VUS || "20");
const maxVUs = Number(__ENV.LOAD_MAX_VUS || "200");
const p95ThresholdMs = Number(__ENV.LOAD_P95_THRESHOLD_MS || "100");
const failRateThreshold = __ENV.LOAD_FAIL_RATE_THRESHOLD || "0.001";

export const options = {
  scenarios: {
    ingest_blackhole: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    http_req_failed: [`rate<${failRateThreshold}`],
    http_req_duration: [`p(95)<${p95ThresholdMs}`],
  },
};

const ingestUrl = (__ENV.INGEST_URL || "http://127.0.0.1:18091").replace(/\/$/, "");
const ingestToken = __ENV.INGEST_TOKEN || "igx_loadtest_token";
const appid = __ENV.LOADTEST_APPID || "LOADTEST_APP";
const runId = __ENV.LOADTEST_RUN_ID || `${Date.now()}`;
const eventName = __ENV.LOADTEST_EVENT || "startup";

export default function () {
  const sequence = `${runId}-${__VU}-${__ITER}`;
  const payload = JSON.stringify({
    appid,
    xwhat: eventName,
    xcontext: {
      installid: `loadtest-${sequence}`,
      os: "ios",
      idfa: `idfa-${sequence}`,
      test_run_id: runId,
    },
  });

  const response = http.post(`${ingestUrl}/ingest`, payload, {
    headers: {
      "content-type": "application/json",
      "x-ingest-token": ingestToken,
    },
    tags: {
      endpoint: "post_ingest",
      event: eventName,
    },
  });

  check(response, {
    "ingest status is 200": (res) => res.status === 200,
    "ingest body is 200": (res) => res.body === "200",
  });
}
