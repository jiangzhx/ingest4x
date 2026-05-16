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
import { ServiceNodesPage } from "../features/service-nodes/ServiceNodesPage";
import { DeliveryTargetsPage } from "../features/sinks/DeliveryTargetsPage";
import { EventSinksPage } from "../features/sinks/EventSinksPage";
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

const deliveryTargetsRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/delivery-targets",
  component: DeliveryTargetsPage,
});

const eventSinksRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/event-sinks",
  component: EventSinksPage,
});

const legacySinksRoute = createRoute({
  getParentRoute: () => shellRoute,
  path: "/sinks",
  component: () => <Navigate to="/delivery-targets" replace />,
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
    deliveryTargetsRoute,
    eventSinksRoute,
    legacySinksRoute,
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
