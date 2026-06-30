import { useState } from 'react'

import { readSidebarCollapsedCookiePreference, SIDEBAR_COOKIE_NAME } from './sidebar-state'

const COOKIE_MAX_AGE_SECONDS = 365 * 24 * 60 * 60

function saveSidebarState(isMinimized: boolean) {
  const value = encodeURIComponent(String(isMinimized))
  document.cookie = `${SIDEBAR_COOKIE_NAME}=${value}; Max-Age=${COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax`
}

// Only the user's explicit preference lives in React state — it comes from a
// cookie that is read during SSR, so the server and client render the same
// markup (no hydration mismatch). Collapsing on small viewports is handled
// entirely in CSS via media queries, which avoids the flash of an expanded
// sidebar that a client-side viewport check would cause before hydration.
export function useSidebarState(initialIsMinimized: boolean) {
  const [isMinimized, setIsMinimized] = useState(() => {
    if (typeof document === 'undefined') return initialIsMinimized
    return readSidebarCollapsedCookiePreference(document.cookie) ?? initialIsMinimized
  })

  const toggleSidebar = () => {
    const next = !isMinimized
    saveSidebarState(next)
    setIsMinimized(next)
  }

  return { isMinimized, toggleSidebar }
}
