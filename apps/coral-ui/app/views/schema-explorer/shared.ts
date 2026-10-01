import { useLocation, useNavigate } from 'react-router'

export function formatError(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

// Retry from a route ErrorBoundary. Render-time errors (a rejected loader
// promise unwrapped with `use()`) don't reset on revalidation alone, so re-run
// the navigation to the current location instead — that re-executes the
// clientLoader and remounts the boundary.
export function useRouteRetry(): () => void {
  const navigate = useNavigate()
  const location = useLocation()
  return () => void navigate(location.pathname + location.search, { replace: true })
}
