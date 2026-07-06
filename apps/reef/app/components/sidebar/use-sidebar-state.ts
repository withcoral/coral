import { useState } from 'react'

import { coralDesktopApi } from '@/lib/coral-desktop'

import { readSidebarCollapsedCookiePreference, SIDEBAR_COOKIE_NAME } from './sidebar-state'

const COOKIE_MAX_AGE_SECONDS = 365 * 24 * 60 * 60

// Electron never registers custom app:// schemes as cookieable, so document.cookie
// is inert in the packaged desktop shell. Persist via localStorage there; the web
// build keeps cookies for SSR seeding.
function isDesktopShell(): boolean {
  return coralDesktopApi() !== null
}

function saveSidebarState(isMinimized: boolean) {
  if (isDesktopShell()) {
    try {
      localStorage.setItem(SIDEBAR_COOKIE_NAME, String(isMinimized))
    } catch {
      // Ignore storage failures (quota, restricted mode).
    }
    return
  }
  const value = encodeURIComponent(String(isMinimized))
  document.cookie = `${SIDEBAR_COOKIE_NAME}=${value}; Max-Age=${COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax`
}

function readSavedPreference(): boolean | null {
  if (isDesktopShell()) {
    try {
      const stored = localStorage.getItem(SIDEBAR_COOKIE_NAME)
      return stored === null ? null : stored === 'true'
    } catch {
      return null
    }
  }
  return readSidebarCollapsedCookiePreference(document.cookie)
}

// Only the user's explicit preference lives in React state — on web it comes from
// a cookie read during SSR, so the server and client render the same markup (no
// hydration mismatch). Collapsing on small viewports is handled entirely in CSS
// via media queries, which avoids the flash of an expanded sidebar that a
// client-side viewport check would cause before hydration.
export function useSidebarState(initialIsMinimized: boolean) {
  const [isMinimized, setIsMinimized] = useState(() => {
    if (typeof document === 'undefined') return initialIsMinimized
    return readSavedPreference() ?? initialIsMinimized
  })

  const toggleSidebar = () => {
    const next = !isMinimized
    saveSidebarState(next)
    setIsMinimized(next)
  }

  return { isMinimized, toggleSidebar }
}
