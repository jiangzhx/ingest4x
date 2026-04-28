# Admin Console Projects MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `ingest4x` 增加第一版管理后台骨架、最小登录鉴权，并基于现有 `/api/admin/projects` 接口交付可用的项目管理 CRUD 页面。

**Architecture:** 采用“前端独立构建、后端静态托管、同源调用管理 API”的方式。前端放在 `web/admin`，使用 `React + Vite + Ant Design + TanStack Query + TanStack Router`；后端继续保留 `Actix Web` 主服务，并增加 `/admin` 入口来托管前端构建产物。鉴权采用最小实现：前端提供登录页让用户输入密码，后端对固定密码进行校验，并要求后续 `/api/admin/*` 请求携带该密码。第一版只覆盖 `projects`，`tokens` 和 `rules` 等对应接口准备好之后再单独拆计划。

**Tech Stack:** React 18, Vite, TypeScript, Ant Design 5, TanStack Query, TanStack Router, Actix Web 4, actix-files

---

## File Structure

### New / Updated Responsibilities

- `web/admin/package.json`
  - 前端工程依赖、脚本和构建命令
- `web/admin/tsconfig.json`
  - TypeScript 编译配置
- `web/admin/vite.config.ts`
  - Vite 构建配置
  - 开发期将 `/api` 代理到本地 `Actix Web`
- `web/admin/index.html`
  - Vite 应用入口 HTML
- `web/admin/src/main.tsx`
  - React 启动入口
- `web/admin/src/app/App.tsx`
  - 全局应用壳
- `web/admin/src/app/providers.tsx`
  - `Ant Design`、`TanStack Query`、`TanStack Router` 等全局 provider
- `web/admin/src/app/router.tsx`
  - 管理后台 SPA 路由定义
- `web/admin/src/layouts/AdminShell.tsx`
  - 侧边导航和主布局
- `web/admin/src/features/auth/LoginPage.tsx`
  - 登录页和密码输入表单
- `web/admin/src/features/auth/storage.ts`
  - 本地保存和读取管理员密码
- `web/admin/src/shared/http.ts`
  - 同源 API 请求封装
  - 自动附带管理员密码请求头
- `web/admin/src/features/projects/types.ts`
  - `projects` 页面使用的 DTO 和表单类型
- `web/admin/src/features/projects/api.ts`
  - `/api/admin/projects` 请求封装
- `web/admin/src/features/projects/hooks.ts`
  - `TanStack Query` 查询和 mutation hooks
- `web/admin/src/features/projects/ProjectsPage.tsx`
  - 项目列表页容器
- `web/admin/src/features/projects/ProjectsTable.tsx`
  - 项目表格和行操作
- `web/admin/src/features/projects/ProjectFormModal.tsx`
  - 新建/编辑项目弹窗表单
- `src/admin_ui.rs`
  - `/admin` 与 `/admin/*` 静态文件和 SPA fallback 托管
- `src/admin/auth.rs`
  - 固定密码校验
  - 管理 API 登录探测和鉴权辅助函数
- `src/admin/mod.rs`
  - 注册管理接口和登录校验
- `src/lib.rs`
  - 导出 `admin_ui` 模块
- `src/server.rs`
  - 注册管理后台静态资源路由
- `tests/test_admin_ui_static.rs`
  - 覆盖 `/admin` 首页和 SPA fallback
- `README.md`
  - 追加管理后台本地开发和生产构建说明
- `.gitignore`
  - 忽略 `web/admin/node_modules` 和 `web/admin/dist`

### Implementation Notes

- 本计划只交付 `projects` 页面，不强行提前实现 `tokens` / `rules` 页面壳。
- 前端不引入 `Refine`、`Next.js`、`React Router` 或额外 UI 体系。
- 前端 API 客户端先手写，不做 OpenAPI 代码生成。
- `/admin` 静态托管需要支持 SPA 深链，例如 `/admin/projects` 刷新仍返回 `index.html`。
- 若生产环境未构建前端资源，`/admin` 可返回 `404`，但不能影响现有 API 和 ingest 路由启动。
- 固定密码优先从环境变量读取；若未设置，使用仅供本地开发的默认值，并在 README 中明确说明。
- 鉴权只保护 `/api/admin/*`；登录页和静态资源可公开访问。

## Task 1: Scaffold The Admin Frontend Workspace

**Files:**
- Create: `web/admin/package.json`
- Create: `web/admin/tsconfig.json`
- Create: `web/admin/vite.config.ts`
- Create: `web/admin/index.html`
- Create: `web/admin/src/main.tsx`
- Create: `web/admin/src/app/App.tsx`
- Create: `web/admin/src/app/providers.tsx`
- Create: `web/admin/src/app/router.tsx`
- Create: `web/admin/src/layouts/AdminShell.tsx`
- Modify: `.gitignore`

- [ ] **Step 1: Create the failing frontend build command**

```json
{
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build"
  }
}
```

- [ ] **Step 2: Run the build to verify the workspace does not exist yet**

Run: `cd web/admin && npm run build`  
Expected: FAIL with “No such file or directory” or missing `package.json`, proving the admin workspace has not been created yet.

- [ ] **Step 3: Add the minimal Vite + React + TypeScript scaffold**

```tsx
// web/admin/src/main.tsx
import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app/App";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
```

```tsx
// web/admin/src/app/App.tsx
import { Providers } from "./providers";

export function App() {
  return <Providers />;
}
```

- [ ] **Step 4: Add the global providers and admin shell**

```tsx
// web/admin/src/app/providers.tsx
import { ConfigProvider, App as AntApp } from "antd";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { router } from "./router";

const queryClient = new QueryClient();

export function Providers() {
  return (
    <ConfigProvider>
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  );
}
```

- [ ] **Step 5: Define the initial SPA routes**

```tsx
// web/admin/src/app/router.tsx
import { createRootRoute, createRoute, createRouter, Outlet } from "@tanstack/react-router";
import { AdminShell } from "../layouts/AdminShell";

const rootRoute = createRootRoute({
  component: () => (
    <AdminShell>
      <Outlet />
    </AdminShell>
  ),
});

const projectsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/projects",
  component: () => <div>projects</div>,
});

export const router = createRouter({
  routeTree: rootRoute.addChildren([projectsRoute]),
});
```

- [ ] **Step 6: Ignore frontend build artifacts**

```gitignore
web/admin/node_modules/
web/admin/dist/
```

- [ ] **Step 7: Re-run the frontend build**

Run: `cd web/admin && npm install && npm run build`  
Expected: PASS, producing `web/admin/dist`, proving the admin workspace and base shell are wired correctly.

- [ ] **Step 8: Commit the frontend scaffold**

```bash
git add .gitignore web/admin
git commit -m "feat: scaffold admin frontend workspace"
```

## Task 2: Add `/admin` Static Hosting In Actix Web

**Files:**
- Create: `src/admin_ui.rs`
- Modify: `src/lib.rs`
- Modify: `src/server.rs`
- Create: `tests/test_admin_ui_static.rs`

- [ ] **Step 1: Write the failing static hosting test for `/admin`**

```rust
#[actix_rt::test]
async fn admin_ui_serves_index_html_from_dist_dir() {
    let temp = tempfile::tempdir().expect("temp dir");
    std::fs::write(temp.path().join("index.html"), "<html>admin</html>").expect("index");
    std::env::set_var("INGEST4X_ADMIN_UI_DIST_DIR", temp.path());

    let app = test::init_service(App::new().configure(|cfg| {
        ingest4x::admin_ui::configure(cfg);
    }))
    .await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/admin").to_request()).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run the static hosting test to verify it fails**

Run: `cargo test --test test_admin_ui_static -q`  
Expected: FAIL because `admin_ui` module and `/admin` route do not exist yet.

- [ ] **Step 3: Add the static hosting module with SPA fallback**

```rust
pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/admin", web::get().to(index))
        .route("/admin/{tail:.*}", web::get().to(index));
}

async fn index() -> actix_web::Result<NamedFile> {
    let dist_dir = admin_dist_dir();
    NamedFile::open(dist_dir.join("index.html")).map_err(Into::into)
}
```

- [ ] **Step 4: Register admin UI routes in the main server**

```rust
pub fn configure_app(cfg: &mut ServiceConfig, state: AppState) {
    cfg.app_data(Data::new(state.settings.clone()))
        .configure(admin::configure)
        .configure(admin_ui::configure);
}
```

- [ ] **Step 5: Add SPA deep-link coverage**

```rust
#[actix_rt::test]
async fn admin_ui_deep_link_returns_index_html() {
    let resp = test::call_service(
        &app,
        test::TestRequest::get().uri("/admin/projects").to_request(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 6: Re-run the static hosting tests**

Run: `cargo test --test test_admin_ui_static -q`  
Expected: PASS, proving `/admin` and `/admin/projects` both resolve to the SPA entry HTML.

- [ ] **Step 7: Commit the backend hosting route**

```bash
git add src/admin_ui.rs src/lib.rs src/server.rs tests/test_admin_ui_static.rs
git commit -m "feat: serve admin ui from actix"
```

## Task 3: Implement Projects API Client And List Page

**Files:**
- Create: `web/admin/src/shared/http.ts`
- Create: `web/admin/src/features/projects/types.ts`
- Create: `web/admin/src/features/projects/api.ts`
- Create: `web/admin/src/features/projects/hooks.ts`
- Create: `web/admin/src/features/projects/ProjectsPage.tsx`
- Create: `web/admin/src/features/projects/ProjectsTable.tsx`
- Modify: `web/admin/src/app/router.tsx`
- Modify: `web/admin/src/layouts/AdminShell.tsx`

- [ ] **Step 1: Make the `/projects` route fail with a real page import**

```tsx
import { ProjectsPage } from "../features/projects/ProjectsPage";

const projectsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/projects",
  component: ProjectsPage,
});
```

- [ ] **Step 2: Run the frontend build to verify it fails on missing modules**

Run: `cd web/admin && npm run build`  
Expected: FAIL because `ProjectsPage` and the `projects` feature files do not exist yet.

- [ ] **Step 3: Add the typed request layer for `/api/admin/projects`**

```ts
// web/admin/src/features/projects/types.ts
export interface Project {
  appid: string;
  name: string;
  enabled: boolean;
  created_at: number;
  updated_at: number;
}
```

```ts
// web/admin/src/features/projects/api.ts
import { request } from "../../shared/http";
import type { Project } from "./types";

export function listProjects() {
  return request<Project[]>("/api/admin/projects");
}
```

- [ ] **Step 4: Add the query hook and list page**

```tsx
// web/admin/src/features/projects/hooks.ts
export function useProjectsQuery() {
  return useQuery({
    queryKey: ["projects"],
    queryFn: listProjects,
  });
}
```

```tsx
// web/admin/src/features/projects/ProjectsPage.tsx
export function ProjectsPage() {
  const projectsQuery = useProjectsQuery();
  return <ProjectsTable loading={projectsQuery.isLoading} data={projectsQuery.data ?? []} />;
}
```

- [ ] **Step 5: Add the Ant Design table and menu entry**

```tsx
// web/admin/src/layouts/AdminShell.tsx
const items = [{ key: "/projects", label: "项目管理" }];
```

```tsx
// web/admin/src/features/projects/ProjectsTable.tsx
const columns: ColumnsType<Project> = [
  { title: "AppID", dataIndex: "appid" },
  { title: "名称", dataIndex: "name" },
  { title: "状态", dataIndex: "enabled", render: (enabled) => enabled ? "启用" : "禁用" },
];
```

- [ ] **Step 6: Re-run the frontend build**

Run: `cd web/admin && npm run build`  
Expected: PASS, proving the page skeleton, typed API client, and projects list route are wired together.

- [ ] **Step 7: Smoke-check against the live backend**

Run: `curl -s http://localhost:8090/api/admin/projects`  
Expected: `200 OK` with JSON array, matching the list page request contract.

- [ ] **Step 8: Commit the projects list page**

```bash
git add web/admin/src/app/router.tsx web/admin/src/layouts/AdminShell.tsx web/admin/src/shared/http.ts web/admin/src/features/projects
git commit -m "feat: add projects admin list page"
```

## Task 4: Add Minimal Admin Login And API Auth Guard

**Files:**
- Create: `src/admin/auth.rs`
- Modify: `src/admin/mod.rs`
- Modify: `src/admin/projects.rs`
- Create: `web/admin/src/features/auth/LoginPage.tsx`
- Create: `web/admin/src/features/auth/storage.ts`
- Modify: `web/admin/src/app/router.tsx`
- Modify: `web/admin/src/shared/http.ts`
- Modify: `web/admin/src/layouts/AdminShell.tsx`

- [ ] **Step 1: Write the failing backend auth test**

```rust
#[actix_rt::test]
async fn admin_projects_requires_password_header() {
    let app = test::init_service(App::new().configure(server_config)).await;

    let resp = test::call_service(
        &app,
        test::TestRequest::get().uri("/api/admin/projects").to_request(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
```

- [ ] **Step 2: Run the auth test to verify it fails**

Run: `cargo test --test test_admin_projects_endpoint -q`  
Expected: FAIL because the current admin API does not enforce authentication.

- [ ] **Step 3: Add minimal backend password verification**

```rust
pub const ADMIN_PASSWORD_HEADER: &str = "x-admin-password";

pub fn expected_admin_password() -> String {
    std::env::var("INGEST4X_ADMIN_PASSWORD").unwrap_or_else(|_| "admin123".to_string())
}

pub fn authorize_admin_request(req: &HttpRequest) -> Result<(), HttpResponse> {
    match req.headers().get(ADMIN_PASSWORD_HEADER).and_then(|v| v.to_str().ok()) {
        Some(value) if value == expected_admin_password() => Ok(()),
        _ => Err(HttpResponse::Unauthorized().finish()),
    }
}
```

- [ ] **Step 4: Add the login page and client-side password storage**

```tsx
export function LoginPage() {
  return <Form>{/* password input + submit */}</Form>;
}
```

```ts
export function saveAdminPassword(password: string) {
  window.localStorage.setItem("ingest4x.admin.password", password);
}
```

- [ ] **Step 5: Require login before entering the projects page**

```tsx
if (!loadAdminPassword()) {
  return <LoginPage />;
}
```

- [ ] **Step 6: Make all admin API requests send the password header**

```ts
headers: {
  "Content-Type": "application/json",
  "x-admin-password": loadAdminPassword() ?? "",
}
```

- [ ] **Step 7: Re-run frontend and backend checks**

Run: `cargo test --test test_admin_projects_endpoint -q && cd web/admin && npm run build`  
Expected: PASS, proving unauthorized requests are rejected and the login flow type-checks.

- [ ] **Step 8: Commit the auth flow**

```bash
git add src/admin web/admin/src/features/auth web/admin/src/shared/http.ts web/admin/src/app/router.tsx web/admin/src/layouts/AdminShell.tsx
git commit -m "feat: add admin login flow"
```

## Task 5: Add Create, Edit, And Delete Project Flows

**Files:**
- Create: `web/admin/src/features/projects/ProjectFormModal.tsx`
- Modify: `web/admin/src/features/projects/api.ts`
- Modify: `web/admin/src/features/projects/hooks.ts`
- Modify: `web/admin/src/features/projects/ProjectsPage.tsx`
- Modify: `web/admin/src/features/projects/ProjectsTable.tsx`

- [ ] **Step 1: Extend the API layer to fail on missing mutations**

```ts
export interface CreateProjectInput {
  appid: string;
  name: string;
  enabled: boolean;
}

export function createProject(input: CreateProjectInput) {
  return request<Project>("/api/admin/projects", {
    method: "POST",
    body: JSON.stringify(input),
  });
}
```

- [ ] **Step 2: Run the frontend build to verify mutation imports are still missing**

Run: `cd web/admin && npm run build`  
Expected: FAIL because the modal component and mutation hooks are not implemented yet.

- [ ] **Step 3: Add create/update/delete query mutations**

```ts
export function useCreateProjectMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: createProject,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["projects"] }),
  });
}
```

- [ ] **Step 4: Add the project form modal**

```tsx
export function ProjectFormModal(props: {
  open: boolean;
  mode: "create" | "edit";
  initialValue?: Project;
  onCancel: () => void;
  onSubmit: (value: CreateProjectInput | UpdateProjectInput) => Promise<void>;
}) {
  return (
    <Modal open={props.open} title={props.mode === "create" ? "新建项目" : "编辑项目"}>
      <Form layout="vertical">{/* appid / name / enabled */}</Form>
    </Modal>
  );
}
```

- [ ] **Step 5: Add row actions and destructive confirmation**

```tsx
render: (_, record) => (
  <Space>
    <Button onClick={() => onEdit(record)}>编辑</Button>
    <Popconfirm title="确认删除该项目？" onConfirm={() => onDelete(record.appid)}>
      <Button danger>删除</Button>
    </Popconfirm>
  </Space>
)
```

- [ ] **Step 6: Re-run the frontend build**

Run: `cd web/admin && npm run build`  
Expected: PASS, proving the create/edit/delete flows type-check and bundle successfully.

- [ ] **Step 7: Manual acceptance against the local backend**

Run: `cd web/admin && npm run dev`  
Expected: In the browser, `/projects` can create a project, update its name/status, and delete it; every successful mutation refreshes the table without manual reload.

- [ ] **Step 8: Commit the project CRUD UI**

```bash
git add web/admin/src/features/projects
git commit -m "feat: add projects admin crud flows"
```

## Task 6: Document The Workflow And Verify End-To-End Delivery

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a failing documentation checklist**

```md
- [ ] 说明如何启动管理后台前端开发服务器
- [ ] 说明如何访问 `/swagger-ui/`
- [ ] 说明如何设置管理员密码
- [ ] 说明如何构建并由 `Actix Web` 托管 `/admin`
```

- [ ] **Step 2: Update the README with admin console instructions**

```md
### 管理后台开发

```bash
cd web/admin
npm install
npm run dev
```

后端保持本地运行后，可访问：

- `http://localhost:8090/swagger-ui/`
- `http://localhost:5173/projects`

管理员密码默认从环境变量 `INGEST4X_ADMIN_PASSWORD` 读取。
```

- [ ] **Step 3: Verify the frontend production build**

Run: `cd web/admin && npm run build`  
Expected: PASS and `web/admin/dist` exists.

- [ ] **Step 4: Verify backend tests**

Run: `cargo test --test test_admin_ui_static -q`  
Expected: PASS, proving static hosting works independently of the ingest path.

- [ ] **Step 5: Verify end-to-end local startup**

Run: `cargo run -- server -c config/development.toml`  
Expected: The backend starts successfully; visiting `http://localhost:8090/admin` loads the built SPA when `web/admin/dist/index.html` exists.

- [ ] **Step 6: Commit the docs and verification pass**

```bash
git add README.md
git commit -m "docs: add admin console workflow"
```
