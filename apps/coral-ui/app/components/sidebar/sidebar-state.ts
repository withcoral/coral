export const SIDEBAR_COOKIE_NAME = 'coral_ui_sidebar_collapsed'

export function readSidebarCollapsedCookiePreference(
  cookieHeader: string | null | undefined,
): boolean | null {
  if (!cookieHeader) return null

  const cookie = cookieHeader
    .split(';')
    .map((pair) => pair.trim())
    .find((pair) => pair.startsWith(`${SIDEBAR_COOKIE_NAME}=`))

  if (!cookie) return null

  try {
    return decodeURIComponent(cookie.slice(SIDEBAR_COOKIE_NAME.length + 1)) === 'true'
  } catch {
    return null
  }
}

export function readSidebarCollapsedCookieValue(cookieHeader: string | null | undefined): boolean {
  return readSidebarCollapsedCookiePreference(cookieHeader) ?? false
}

export function readSidebarCollapsedCookie(request: Request): boolean {
  return readSidebarCollapsedCookieValue(request.headers.get('cookie'))
}
