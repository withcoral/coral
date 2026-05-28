import { useCallback, useEffect, useState } from 'react'

import { breakpoints } from '@/styles/theme'

const SIDEBAR_STATE_STORAGE_KEY = 'coral:sidebar-collapsed'
const SIDEBAR_COLLAPSE_QUERY = `(max-width: ${breakpoints.sidebarCollapse})`
const MOBILE_QUERY = `(max-width: ${breakpoints.mobile})`

function getStoredSidebarState() {
  if (typeof window === 'undefined') return undefined

  const stored = window.localStorage.getItem(SIDEBAR_STATE_STORAGE_KEY)
  if (stored === 'true') return true
  if (stored === 'false') return false
  return undefined
}

function saveSidebarState(isCollapsed: boolean) {
  window.localStorage.setItem(SIDEBAR_STATE_STORAGE_KEY, String(isCollapsed))
  window.dispatchEvent(
    new StorageEvent('storage', {
      key: SIDEBAR_STATE_STORAGE_KEY,
      newValue: String(isCollapsed),
    }),
  )
}

function resolveSidebarState(
  shouldCollapseForViewport: boolean,
  storedValue = getStoredSidebarState(),
) {
  return shouldCollapseForViewport || storedValue === true
}

function useMediaQuery(query: string) {
  const [matches, setMatches] = useState(
    () => typeof window !== 'undefined' && window.matchMedia(query).matches,
  )

  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia(query)
    const handleChange = () => setMatches(mediaQuery.matches)

    handleChange()
    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [query])

  return matches
}

export function useSidebarState() {
  const shouldCollapseForViewport = useMediaQuery(SIDEBAR_COLLAPSE_QUERY)
  const shouldHideSidebarToggle = useMediaQuery(MOBILE_QUERY)
  const [isCollapsed, setIsCollapsed] = useState(() =>
    resolveSidebarState(shouldCollapseForViewport),
  )

  useEffect(() => {
    setIsCollapsed(resolveSidebarState(shouldCollapseForViewport))
  }, [shouldCollapseForViewport])

  useEffect(() => {
    if (typeof window === 'undefined') return

    const handleStorage = (event: StorageEvent) => {
      if (event.key !== SIDEBAR_STATE_STORAGE_KEY) return

      const storedValue =
        event.newValue === 'true' ? true : event.newValue === 'false' ? false : undefined
      setIsCollapsed(resolveSidebarState(shouldCollapseForViewport, storedValue))
    }

    window.addEventListener('storage', handleStorage)
    return () => window.removeEventListener('storage', handleStorage)
  }, [shouldCollapseForViewport])

  const toggleSidebar = useCallback(() => {
    if (shouldHideSidebarToggle) return

    const nextValue = !isCollapsed
    setIsCollapsed(nextValue)
    saveSidebarState(nextValue)
  }, [isCollapsed, shouldHideSidebarToggle])

  return { isCollapsed, shouldHideSidebarToggle, toggleSidebar }
}
