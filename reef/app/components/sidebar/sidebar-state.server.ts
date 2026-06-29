import { SIDEBAR_COOKIE_NAME } from './sidebar-state'

export function readSidebarCollapsedCookie(request: Request): boolean {
  const cookieHeader = request.headers.get('cookie')
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
