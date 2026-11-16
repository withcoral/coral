import { useCallback, useEffect, useRef, useState } from 'react'

import { addToast } from '@/wax/components/toast'

const COPIED_MS = 3000

/**
 * Copies text to the clipboard for a control that says so afterwards. Returns
 * whether the last copy happened in the last few seconds, and the function
 * that copies. A browser without a clipboard, such as one on an address that
 * is not secure, gets the failure toast like any other refusal.
 */
export function useCopy({
  failureDescription,
  onCopy,
}: {
  /** What the failure toast tells the reader to do. */
  failureDescription?: string
  onCopy?: () => void
} = {}) {
  const [copied, setCopied] = useState(false)
  const reset = useRef<ReturnType<typeof setTimeout>>(undefined)
  // The feedback of a control that left the page has nothing to reset.
  useEffect(() => () => clearTimeout(reset.current), [])
  const copy = useCallback(
    async (text: string) => {
      try {
        await navigator.clipboard.writeText(text)
        setCopied(true)
        onCopy?.()
        // A second copy starts the feedback again; the first must not end it early.
        clearTimeout(reset.current)
        reset.current = setTimeout(() => setCopied(false), COPIED_MS)
      } catch {
        clearTimeout(reset.current)
        setCopied(false)
        addToast('error', { title: 'Copy failed', description: failureDescription })
      }
    },
    [failureDescription, onCopy],
  )
  return [copied, copy] as const
}
