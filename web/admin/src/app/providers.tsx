import { App as AntApp, ConfigProvider } from "antd";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { router } from "./router";

const queryClient = new QueryClient();

export function Providers() {
  return (
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: "#111827",
          borderRadius: 10,
        },
      }}
    >
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  );
}
