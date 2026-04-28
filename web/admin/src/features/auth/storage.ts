const ADMIN_PASSWORD_SESSION_KEY = "ingest4x.admin.password";

let adminPassword: string | null = readSessionPassword();

function readSessionPassword(): string | null {
  try {
    return window.sessionStorage.getItem(ADMIN_PASSWORD_SESSION_KEY);
  } catch {
    return null;
  }
}

export function getAdminPassword(): string | null {
  return adminPassword;
}

export function hasAdminPassword(): boolean {
  return adminPassword !== null;
}

export function setAdminPassword(password: string): void {
  adminPassword = password;

  try {
    window.sessionStorage.setItem(ADMIN_PASSWORD_SESSION_KEY, password);
  } catch {
    // Keep the in-memory value when browser storage is unavailable.
  }
}

export function clearAdminPassword(): void {
  adminPassword = null;

  try {
    window.sessionStorage.removeItem(ADMIN_PASSWORD_SESSION_KEY);
  } catch {
    // Nothing else to clear when browser storage is unavailable.
  }
}
