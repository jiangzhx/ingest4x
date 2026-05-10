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
  normalizeProjectRuleSetAssignmentsResponse,
  normalizeRuleResponse,
  normalizeRuleSetResponse,
} = await import(resolveModuleUrl("../src/features/rules/api.ts"));

const routerSource = readFileSync(
  new URL("../src/app/router.tsx", import.meta.url),
  "utf8",
);
const shellSource = readFileSync(
  new URL("../src/layouts/AdminShell.tsx", import.meta.url),
  "utf8",
);
const rulesPageSource = readFileSync(
  new URL("../src/features/rules/RulesPage.tsx", import.meta.url),
  "utf8",
);
const projectsPageSource = readFileSync(
  new URL("../src/features/projects/ProjectsPage.tsx", import.meta.url),
  "utf8",
);
const ruleFormModalSource = readFileSync(
  new URL("../src/features/rules/RuleFormModal.tsx", import.meta.url),
  "utf8",
);
const ruleSetFormModalSource = readFileSync(
  new URL("../src/features/rules/RuleSetFormModal.tsx", import.meta.url),
  "utf8",
);
const rulesTableSource = readFileSync(
  new URL("../src/features/rules/RulesTable.tsx", import.meta.url),
  "utf8",
);
const projectRuleSetsPanelSource = readFileSync(
  new URL("../src/features/rules/ProjectRuleSetsPanel.tsx", import.meta.url),
  "utf8",
);
const rulesPackageSource = readFileSync(
  new URL("../package.json", import.meta.url),
  "utf8",
);

test("admin shell and router expose the rules management page", () => {
  assert.match(routerSource, /path: "\/rules"/);
  assert.match(routerSource, /component: RulesPage/);
  assert.match(shellSource, /key: "\/rules"/);
  assert.match(shellSource, /规则管理/);
  assert.doesNotMatch(routerSource, /path: "\/project-rules"/);
  assert.doesNotMatch(routerSource, /component: ProjectRulesPage/);
  assert.doesNotMatch(shellSource, /key: "\/project-rules"/);
  assert.doesNotMatch(shellSource, /项目规则配置/);
});

test("rules page manages rule sets and a single Rhai validation script", () => {
  assert.match(rulesPageSource, /useRuleSetsQuery\(\)/);
  assert.match(rulesPageSource, /<Select/);
  assert.match(rulesPageSource, /ruleSetOptions/);
  assert.match(rulesPageSource, /Rhai 校验脚本/);
  assert.match(rulesPageSource, /useSaveValidationRuleMutation/);
  assert.doesNotMatch(rulesPageSource, /<RuleSetsTable/);
  assert.doesNotMatch(rulesPageSource, /<RulesTable/);
  assert.doesNotMatch(rulesPageSource, /新建规则\s*</);
  assert.doesNotMatch(rulesPageSource, /规则继承树/);
  assert.doesNotMatch(rulesPageSource, /useProjectsQuery\(\)/);
  assert.doesNotMatch(rulesPageSource, /useProjectRuleSetAssignmentsQuery/);
  assert.doesNotMatch(rulesPageSource, /<ProjectRuleSetsPanel/);
});

test("project edit owns project rule set assignment", () => {
  assert.match(projectsPageSource, /useRuleSetsQuery\(\)/);
  assert.match(projectsPageSource, /useProjectRuleSetAssignmentsQuery/);
  assert.match(projectsPageSource, /<ProjectRuleSetsPanel/);
  assert.match(projectsPageSource, /ruleSetsSection=/);
  assert.match(projectsPageSource, /规则集绑定/);
});

test("project rule set assignment is a single selected rule set", () => {
  assert.match(projectRuleSetsPanelSource, /currentAssignment/);
  assert.match(projectRuleSetsPanelSource, /value=\{currentAssignment\?\.rule_set_id\}/);
  assert.match(projectRuleSetsPanelSource, /placeholder="选择启用规则集"/);
  assert.doesNotMatch(projectRuleSetsPanelSource, /<Table/);
  assert.doesNotMatch(projectRuleSetsPanelSource, /assignedRuleSetIds/);
});

test("rules UI no longer exposes legacy rule tree fields", () => {
  assert.doesNotMatch(rulesPageSource, /父规则/);
  assert.doesNotMatch(rulesPageSource, /事件名/);
  assert.doesNotMatch(rulesPageSource, /默认规则/);
  assert.doesNotMatch(ruleFormModalSource, /label="父规则"/);
  assert.doesNotMatch(ruleFormModalSource, /label="规则名称"/);
  assert.doesNotMatch(ruleFormModalSource, /label="事件名"/);
  assert.doesNotMatch(rulesTableSource, /title:\s*"事件名"/);
  assert.doesNotMatch(rulesTableSource, /title:\s*"xwhat"/);
  assert.doesNotMatch(ruleFormModalSource, /事件名 xwhat/);
});

test("rules UI does not configure wildcard from rule set editor", () => {
  assert.doesNotMatch(ruleSetFormModalSource, /label="通配规则"/);
  assert.doesNotMatch(ruleSetFormModalSource, /wildcard_rule_id/);
  assert.doesNotMatch(ruleSetFormModalSource, /rules\s*=\s*\[\]/);
  assert.doesNotMatch(ruleSetFormModalSource, /\.filter\(\(rule\) => !rule\.xwhat\)/);
  assert.doesNotMatch(ruleFormModalSource, /label="充当通配规则"/);
});

test("rules UI hides wildcard display state from operators", () => {
  assert.doesNotMatch(rulesPageSource, /通配/);
  assert.doesNotMatch(rulesTableSource, /wildcardRuleId === rule\.id/);
});

test("rules UI does not expose manual sort order", () => {
  assert.doesNotMatch(rulesTableSource, /title:\s*"排序"/);
  assert.doesNotMatch(ruleFormModalSource, /label="排序"/);
  assert.doesNotMatch(ruleFormModalSource, /name="sort_order"/);
});

test("rule content uses an open source Rhai editor", () => {
  assert.match(rulesPackageSource, /"@uiw\/react-codemirror"/);
  assert.match(rulesPageSource, /const EMPTY_RHAI_RULE_CONTENT/);
  assert.match(rulesPageSource, /function LazyRhaiEditor/);
  assert.match(rulesPageSource, /<RhaiEditor value=\{value\} onChange=\{onChange\}/);
  assert.match(rulesPageSource, /<LazyRhaiEditor/);
  assert.doesNotMatch(ruleFormModalSource, /<Input\.TextArea[\s\S]*name="content"/);
});

test("rules api normalizes rule set and rule responses at runtime", () => {
  assert.deepEqual(
    normalizeRuleSetResponse({
      id: 1,
      name: "  Default Rules ",
      description: " shared ",
      enabled: true,
      wildcard_rule_id: null,
      created_at: 10.8,
      updated_at: 11.2,
    }),
    {
      id: 1,
      name: "Default Rules",
      description: "shared",
      enabled: true,
      wildcard_rule_id: null,
      created_at: 10,
      updated_at: 11,
    },
  );

  assert.deepEqual(
    normalizeRuleResponse({
      id: 2,
      rule_set_id: 1,
      parent_id: null,
      name: "Install",
      xwhat: "install",
      content: "fields: {}\n",
      enabled: true,
      created_at: 1,
      updated_at: 2,
    }),
    {
      id: 2,
      rule_set_id: 1,
      parent_id: null,
      name: "Install",
      xwhat: "install",
      content: "fields: {}\n",
      enabled: true,
      created_at: 1,
      updated_at: 2,
    },
  );
});

test("rules api rejects invalid project rule set assignment payloads", () => {
  assert.throws(
    () => normalizeProjectRuleSetAssignmentsResponse({ items: [] }),
    /规则接口响应无效：项目规则集绑定列表不是数组/,
  );
  assert.throws(
    () =>
      normalizeProjectRuleSetAssignmentsResponse([
        {
          id: 1,
          project_id: 1,
          rule_set_id: "2",
          enabled: true,
          created_at: 1,
          updated_at: 2,
        },
      ]),
    /规则接口响应无效：rule_set_id 缺失或不是有效整数/,
  );
});
