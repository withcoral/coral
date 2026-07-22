import { data } from 'react-router'

import {
  CORAL_UNAVAILABLE_STATUS,
  CORAL_UNAVAILABLE_STATUS_TEXT,
  isCoralUnavailableError,
} from './coral-unavailable'

// React Router converts loader failures into rendered responses before route
// middleware regains control. Call this from the loader catch itself so the
// typed 503 survives production error sanitization and reaches the boundary.
export function rethrowAsCoralUnavailableRouteError(request: Request, error: unknown): never {
  if (request.signal.aborted || !isCoralUnavailableError(error)) throw error

  console.error('Coral request failed:', error)
  throw data(null, {
    status: CORAL_UNAVAILABLE_STATUS,
    statusText: CORAL_UNAVAILABLE_STATUS_TEXT,
  })
}
