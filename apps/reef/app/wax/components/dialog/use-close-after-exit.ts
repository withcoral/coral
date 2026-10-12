import { useState } from 'react'

/**
 * For a dialog whose open state lives outside it, such as in the URL. Pass the
 * key of what the route opens, or null when it opens nothing. A close request
 * plays the exit first; `onClose` runs once it ends, so the caller can navigate
 * away without cutting the transition short. The close belongs to the record
 * that was open, so a route that moves to another record meanwhile opens it and
 * is not navigated away. Spread the result on `Dialog.Root`.
 */
export function useCloseAfterExit(opened: string | null, onClose: () => void) {
  const [closing, setClosing] = useState<string | null>(null)
  if (closing !== null && closing !== opened) setClosing(null)
  return {
    open: opened !== null && opened !== closing,
    onOpenChange: (next: boolean) => {
      if (!next) setClosing(opened)
    },
    onOpenChangeComplete: (next: boolean) => {
      if (!next && opened !== null && opened === closing) onClose()
    },
  }
}
