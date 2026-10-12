import { useEffect } from 'react'
import { useRevalidator } from 'react-router'

/** Revalidate server loaders while consent takes place in the system browser. */
export function useAuthorizationProgress(pending: boolean) {
  const revalidator = useRevalidator()
  useEffect(() => {
    if (!pending) return
    const timer = window.setInterval(() => {
      if (revalidator.state === 'idle') void revalidator.revalidate()
    }, 2000)
    return () => window.clearInterval(timer)
  }, [pending, revalidator])
}
