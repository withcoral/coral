import { useEffect, useRef } from 'react'
import { useRevalidator } from 'react-router'

/** The browser's own timeline entry, from one refresh's start to React committing its data.
 * It stays in this browser. It carries no detail, and only the latest stays in the buffer. */
const REFRESH_MEASURE = 'reef.visible_refresh'

/** Rerun the page's loaders when the tab returns to view, and every `intervalMs` while it is
 * in view. A hidden tab reads nothing, and a reload in flight is not doubled.
 */
export function useRevalidateWhileVisible(intervalMs: number) {
  const { revalidate, state } = useRevalidator()
  // A reload the router starts, as after an action, counts as in flight too.
  const idle = useRef(state === 'idle')
  const started = useRef<number | undefined>(undefined)
  useEffect(() => {
    idle.current = state === 'idle'
    if (state !== 'idle' || started.current === undefined) return
    performance.clearMeasures(REFRESH_MEASURE)
    performance.measure(REFRESH_MEASURE, { start: started.current })
    started.current = undefined
  }, [state])
  useEffect(() => {
    // Focus and a visibility change often fire together; the second must not restart the first.
    let reloading = false
    const refresh = () => {
      if (reloading || !idle.current || document.visibilityState !== 'visible') return
      reloading = true
      started.current = performance.now()
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
