import { AdminUnauthorizedError, request } from "../../shared/http";
import { clearAdminPassword } from "./storage";

export async function loginWithPassword(password: string): Promise<void> {
  clearAdminPassword();

  try {
    await request("/api/admin/auth/login", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ password }),
      attachAdminPassword: false,
    });
  } catch (error) {
    clearAdminPassword();

    if (error instanceof AdminUnauthorizedError) {
      throw new Error("管理员密码错误");
    }

    throw error;
  }
}
