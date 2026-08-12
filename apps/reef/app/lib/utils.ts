import { DEFAULT_DEV_CORAL_PORT } from './constants'

export function errorMessage(error: unknown): string {
  if (error instanceof Response) throw error
  return error instanceof Error ? error.message : String(error)
}

export function isLocalDevOrigin(url: URL): boolean {
  return (
    (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
    url.port !== DEFAULT_DEV_CORAL_PORT
  )
}

export function trimTrailingSlash(value: string): string {
  return value.endsWith('/') ? value.slice(0, -1) : value
}
