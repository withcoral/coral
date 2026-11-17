import { useEffect, useState } from 'react'

/**
 * The latest streamed read that settled, and its value. A failed read is null: the page
 * shows it as unavailable, never as zeros. Suspense would hold the page's own update until
 * the read settles, so the read settles in an effect, and a read that a newer one replaced
 * never shows.
 */
export function useSettled<Value>(read: Promise<Value | null>) {
  const [settled, setSettled] = useState<{ read: Promise<Value | null>; value: Value | null }>()
  useEffect(() => {
    let current = true
    const show = (value: Value | null) => {
      if (current) setSettled({ read, value })
    }
    read.then(show, () => show(null))
    return () => {
      current = false
    }
  }, [read])
  return settled
}
