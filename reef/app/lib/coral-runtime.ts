import { coralDesktopApi, type SidecarInfo } from './coral-desktop'

let runtimeInfo: SidecarInfo | null = null
let runtimePromise: Promise<SidecarInfo> | null = null

function fallbackRuntimeInfo(): SidecarInfo {
  return {
    packaged: false,
    url: import.meta.env.VITE_CORAL_GRPC_WEB_URL ?? window.location.origin,
  }
}

export function ensureCoralRuntime(): Promise<SidecarInfo> {
  if (runtimeInfo) return Promise.resolve(runtimeInfo)
  runtimePromise ??= loadCoralRuntime().then((info) => {
    runtimeInfo = info
    return info
  })
  return runtimePromise
}

async function loadCoralRuntime(): Promise<SidecarInfo> {
  const desktop = coralDesktopApi()
  if (!desktop) return fallbackRuntimeInfo()
  return desktop.awaitInitialization()
}
