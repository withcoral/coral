import { Code, ConnectError } from '@connectrpc/connect'

export const CORAL_UNAVAILABLE_STATUS = 503
export const CORAL_UNAVAILABLE_STATUS_TEXT = 'Coral unavailable'

export function isCoralUnavailableError(error: unknown): boolean {
  return error instanceof ConnectError && error.code === Code.Unavailable
}
