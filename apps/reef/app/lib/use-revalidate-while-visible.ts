import { useEffect, useRef } from 'react'
import { useRevalidator } from 'react-router'

/** Rerun the page's loaders when the tab returns to view, and every `intervalMs` while it is
 * in view. A hidden tab reads nothing, and a reload in flight is not doubled.
 */
export function useRevalidateWhileVisible(intervalMs: number) {
  const { revalidate, state } = useRevalidator()
  // A reload the router starts, as after an action, counts as in flight too.
  const idle = useRef(state === 'idle')
  useEffect(() => {
    idle.current = state === 'idle'
  }, [state])
  useEffect(() => {
    // Focus and a visibility change often fire together; the second must not restart the first.
    let reloading = false
    const refresh = () => {
      if (reloading || !idle.current || document.visibilityState !== 'visible') return
      reloading = true
      void revalidate().finally(() => {
        reloading = false
      })
    }
    const timer = window.setInterval(refresh, intervalMs)
    window.addEventListener('focus', refresh)
    document.addEventListener('visibilitychange', refresh)
    return () => {
      window.clearInterval(timer)
      window.removeEventListener('focus', refresh)
      document.removeEventListener('visibilitychange', refresh)
    }
  }, [intervalMs, revalidate])
}
