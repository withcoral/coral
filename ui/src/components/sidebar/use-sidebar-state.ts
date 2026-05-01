import { useEffect, useState } from 'react'

const SMALL_SCREEN_BREAKPOINT = '(max-width: 963px)'
const MOBILE_BREAKPOINT = '(max-width: 640px)'
const SIDEBAR_STATE_KEY = 'sidebarCollapsed'

const isSmallScreen = () => window.matchMedia(SMALL_SCREEN_BREAKPOINT).matches
const isMobileScreen = () => window.matchMedia(MOBILE_BREAKPOINT).matches

const getSidebarState = () => true

export function useSidebarState() {
  const [isMinimized, setIsMinimized] = useState(getSidebarState)

  useEffect(() => {
    const onResize = () => {
      if (isSmallScreen()) {
        setIsMinimized(true)
      }
    }
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  const toggleSidebar = () => {
    if (isMobileScreen()) return
    const newState = !isMinimized
    setIsMinimized(newState)
    if (!isSmallScreen()) {
      localStorage.setItem(SIDEBAR_STATE_KEY, String(newState))
    }
  }

  return { isMinimized, toggleSidebar }
}
