import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const storageSource = readFileSync(
  new URL("../src/features/auth/storage.ts", import.meta.url),
  "utf8",
);

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);

const loginPageSource = readFileSync(
  new URL("../src/features/auth/LoginPage.tsx", import.meta.url),
  "utf8",
);

const httpSource = readFileSync(
  new URL("../src/shared/http.ts", import.meta.url),
  "utf8",
);

test("admin password is scoped to browser session storage", () => {
  assert.match(storageSource, /sessionStorage/);
  assert.doesNotMatch(storageSource, /localStorage/);
});

test("router auth gate uses reusable auth state helper", () => {
  assert.match(routerSource, /hasAdminPassword/);
});

test("login page copy explains session scoped login", () => {
  assert.match(loginPageSource, /当前浏览器会话/);
  assert.doesNotMatch(loginPageSource, /刷新后需要重新登录/);
});

test("login page returns to sanitized redirect path after successful login", () => {
  assert.match(loginPageSource, /function getLoginRedirectPath\(\)/);
  assert.match(loginPageSource, /new URLSearchParams\(window\.location\.search\)/);
  assert.match(loginPageSource, /redirectPath\.startsWith\("\/"\)/);
  assert.match(loginPageSource, /!redirectPath\.startsWith\("\/\/"\)/);
  assert.match(loginPageSource, /await navigate\(\{ to: getLoginRedirectPath\(\) as "\/" \}\)/);
});

test("shared http exports reusable request helpers", () => {
  assert.match(httpSource, /export async function request\(/);
  assert.match(httpSource, /export async function requestJson</);
});
