// The desktop IPC bridge contract is owned by the Electron shared types; reef
// re-exports them (type-only, erased at build) so the renderer and preload can
// never drift. Only the reef-side runtime helpers live here.
import type {
  CoralDesktopApi,
  McpClientDescriptor,
  McpClientId,
  McpLaunchConfig,
} from '../../../desktop/src/shared/types'

export type { CoralDesktopApi, McpClientDescriptor, McpClientId, McpLaunchConfig }

declare global {
  interface Window {
    coralDesktop?: CoralDesktopApi
  }
}

export function isCoralDesktopBuild(): boolean {
  return import.meta.env.CORAL_DESKTOP_APP
}

export function coralDesktopApi(): CoralDesktopApi | null {
  if (typeof window === 'undefined') return null
  return window.coralDesktop ?? null
}

export function desktopErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
