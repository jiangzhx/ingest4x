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
import { ProjectsPage } from "../features/projects/ProjectsPage";
import { RulesPage } from "../features/rules/RulesPage";
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

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: LoginPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  shellRoute.addChildren([indexRoute, projectsRoute, rulesRoute]),
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
