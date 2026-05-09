import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
  Outlet,
  redirect,
} from "@tanstack/react-router";
import { AdminShell } from "../layouts/AdminShell";
import { hasAdminPassword } from "../features/auth/storage";
import { LoginPage } from "../features/auth/LoginPage";
import { ProcessorsPage } from "../features/processors/ProcessorsPage";
import { ProjectsPage } from "../features/projects/ProjectsPage";
import { RulesPage } from "../features/rules/RulesPage";
import { ServiceNodesPage } from "../features/service-nodes/ServiceNodesPage";
import { SinksPage } from "../features/sinks/SinksPage";
import { HomePage } from "../pages/HomePage";

const rootRoute = createRootRoute({
  component: Outlet,
  notFoundComponent: () => <Navigate to="/" replace />,
});

const shellRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "admin-shell",
  beforeLoad: ({ location }) => {
    if (!hasAdminPassword()) {
      throw redirect({
        to: "/login",
        search: { redirect: location.href },
      });
    }
  },
  component: () => (
    <AdminShell>
      <Outlet />
    </AdminShell>
  ),
});

const indexRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/",
  component: HomePage,
});

const projectsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/projects",
  component: ProjectsPage,
});

const rulesRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/rules",
  component: RulesPage,
});

const sinksRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/sinks",
  component: SinksPage,
});

const processorsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/processors",
  component: ProcessorsPage,
});

const serviceNodesRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/service-nodes",
  component: ServiceNodesPage,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: LoginPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  shellRoute.addChildren([
    indexRoute,
    projectsRoute,
    rulesRoute,
    sinksRoute,
    processorsRoute,
    serviceNodesRoute,
  ]),
]);

export const router = createRouter({
  basepath: "/admin",
  routeTree,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
