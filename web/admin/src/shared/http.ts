import { clearAdminPassword, getAdminPassword } from "../features/auth/storage";

type RequestOptions = RequestInit & {
  attachAdminPassword?: boolean;
};

export class HttpError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export class AdminUnauthorizedError extends HttpError {
  constructor() {
    super(401, "Admin authentication expired, please sign in again");
  }
}

export function withAdminPasswordHeader(headers?: HeadersInit): Headers {
  const nextHeaders = new Headers(headers);
  const password = getAdminPassword();

  if (password) {
    nextHeaders.set("x-admin-password", password);
  }

  return nextHeaders;
}

function handleUnauthorizedState(): void {
  clearAdminPassword();

  if (window.location.pathname !== "/admin/login") {
    window.location.assign("/admin/login");
  }
}

function extractJsonErrorMessage(text: string): string {
  if (!text.trim()) {
    return "";
  }

  try {
    const body = JSON.parse(text) as {
      message?: string;
      error?: string;
    };

    return body.message ?? body.error ?? "";
  } catch {
    return "";
  }
}

async function buildHttpError(response: Response): Promise<HttpError> {
  const contentType = response.headers.get("content-type") ?? "";
  const text = await response.text();
  let message = "";

  if (contentType.includes("application/json")) {
    message = extractJsonErrorMessage(text);
  }

  if (!message) {
    message = text.trim();
  }

  if (!message) {
    message = `Request failed (HTTP ${response.status})`;
  }

  return new HttpError(response.status, message);
}

export async function request(
  input: RequestInfo | URL,
  options: RequestOptions = {},
) {
  const { attachAdminPassword = true, headers, ...init } = options;
  const response = await fetch(input, {
    ...init,
    headers: attachAdminPassword
      ? withAdminPasswordHeader(headers)
      : new Headers(headers),
  });

  if (response.status === 401) {
    handleUnauthorizedState();
    throw new AdminUnauthorizedError();
  }

  if (!response.ok) {
    throw await buildHttpError(response);
  }

  return response;
}

export async function requestJson<T>(
  input: RequestInfo | URL,
  options: RequestOptions = {},
): Promise<T> {
  const response = await request(input, options);

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    throw new HttpError(response.status, "Response is not JSON");
  }

  const text = await response.text();

  try {
    return JSON.parse(text) as T;
  } catch {
    throw new HttpError(response.status, "Failed to parse JSON response");
  }
}
