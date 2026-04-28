import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);

const viteConfigSource = readFileSync(
  new URL("../vite.config.ts", import.meta.url),
  "utf8",
);

test("router config matches /admin deployment base path", () => {
  assert.match(viteConfigSource, /base:\s*"\/admin\/"/);
  assert.match(routerSource, /basepath:\s*"\/admin"/);
});

test("root route remains shell-free for future login pages", () => {
  assert.doesNotMatch(routerSource, /createRootRoute\(\{\s*component:\s*\(\)\s*=>\s*\(\s*<AdminShell>/s);
  assert.match(routerSource, /getParentRoute:\s*\(\)\s*=>\s*rootRoute/);
  assert.match(routerSource, /getParentRoute:\s*\(\)\s*=>\s*shellRoute/);
});

test("auth redirect preserves the originally requested admin route", () => {
  assert.match(routerSource, /beforeLoad:\s*\(\{\s*location\s*\}\)/);
  assert.match(routerSource, /search:\s*\{\s*redirect:\s*location\.href\s*\}/);
});

test("unknown admin routes redirect to the admin home page", () => {
  assert.match(routerSource, /notFoundComponent:\s*\(\)\s*=>\s*<Navigate\s+to="\/"\s+replace\s*\/>/);
});
