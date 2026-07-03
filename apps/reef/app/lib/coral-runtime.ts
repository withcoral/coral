import { coralDesktopApi, type SidecarInfo } from './coral-desktop'

let runtimeInfo: SidecarInfo | null = null
let runtimePromise: Promise<SidecarInfo> | null = null

function fallbackRuntimeInfo(): SidecarInfo {
  const envUrl = import.meta.env.VITE_CORAL_GRPC_WEB_URL
  // Guard the window deref so SSR/test paths (no window) don't hard-crash before
  // any network call — require an explicit URL there instead.
  const url = envUrl ?? (typeof window === 'undefined' ? '' : window.location.origin)
  return { packaged: false, url }
}

export function ensureCoralRuntime(): Promise<SidecarInfo> {
  if (runtimeInfo) return Promise.resolve(runtimeInfo)
  if (runtimePromise) return runtimePromise
  const promise = loadCoralRuntime().then((info) => {
    runtimeInfo = info
    return info
  })
  // Drop the cached promise on failure so a later call (e.g. a Retry) can
  // re-attempt instead of re-resolving the same rejection forever.
  promise.catch(() => {
    if (runtimePromise === promise) runtimePromise = null
  })
  runtimePromise = promise
  return promise
}

async function loadCoralRuntime(): Promise<SidecarInfo> {
  const desktop = coralDesktopApi()
  if (!desktop) return fallbackRuntimeInfo()
  return desktop.awaitInitialization()
}
