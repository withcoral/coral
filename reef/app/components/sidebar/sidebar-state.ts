export const SIDEBAR_COOKIE_NAME = 'reef_sidebar_collapsed'

export function readSidebarCollapsedCookieValue(cookieHeader: string | null | undefined): boolean {
  if (!cookieHeader) return false

  const cookie = cookieHeader
    .split(';')
    .map((pair) => pair.trim())
    .find((pair) => pair.startsWith(`${SIDEBAR_COOKIE_NAME}=`))

  if (!cookie) return false

  try {
    return decodeURIComponent(cookie.slice(SIDEBAR_COOKIE_NAME.length + 1)) === 'true'
  } catch {
    return false
  }
}

export function readSidebarCollapsedCookie(request: Request): boolean {
  return readSidebarCollapsedCookieValue(request.headers.get('cookie'))
}
