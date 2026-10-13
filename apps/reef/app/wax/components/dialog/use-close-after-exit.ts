import { useRef, useState } from 'react'

/**
 * For a dialog whose open state lives outside it, such as in the URL. Pass the
 * key of what the route opens, or null when it opens nothing. A close request
 * plays the exit first; `onClose` runs once it ends, so the caller can navigate
 * away without cutting the transition short. The close belongs to the record
 * that was open, so a route that moves to another record meanwhile opens it and
 * is not navigated away. Spread the result on `Dialog.Root`.
 *
 * `reopen` keeps the record open after all, such as when the reader picks it
 * again during the exit. It returns whether `onClose` already ran, so the
 * caller can cancel the navigation that started.
 */
export function useCloseAfterExit(opened: string | null, onClose: () => void) {
  const [closing, setClosing] = useState<string | null>(null)
  // Read only in handlers: whether `onClose` ran for the close under way.
  const closed = useRef(false)
  if (closing !== null && closing !== opened) setClosing(null)
  return {
    open: opened !== null && opened !== closing,
    onOpenChange: (next: boolean) => {
      if (next) return
      closed.current = false
      setClosing(opened)
    },
    onOpenChangeComplete: (next: boolean) => {
      if (next || opened === null || opened !== closing || closed.current) return
      closed.current = true
      onClose()
    },
    reopen: () => {
      const navigated = closing !== null && closed.current
      closed.current = false
      setClosing(null)
      return navigated
    },
  }
}
