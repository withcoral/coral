import { useEffect, useSyncExternalStore } from 'react'
import { useLocation } from 'react-router'

const KEY = 'coral:settings:return-to'
// The stored page changes only while you are outside the settings, where nothing reads it.
const never = () => () => {}

function remembered() {
  try {
    return sessionStorage.getItem(KEY)
  } catch {
    // Without storage, "Back to workspace" uses its fallback.
    return null
  }
}

/**
 * Where "Back to workspace" goes: the page you opened the settings from. Many links
 * lead into the settings, so the app remembers the last page outside them, for
 * this tab. `fallback` serves a tab that opened on the settings.
 */
export function useReturnTo(inSettings: boolean, fallback: string) {
  const { pathname, search, hash } = useLocation()
  useEffect(() => {
    if (inSettings) return
    try {
      sessionStorage.setItem(KEY, pathname + search + hash)
    } catch {
      // "Back to workspace" uses its fallback.
    }
  }, [inSettings, pathname, search, hash])
  return useSyncExternalStore(never, remembered, () => null) ?? fallback
}
