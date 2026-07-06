import type { Transport } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { coralDesktopApi, type SidecarInfo } from './coral-desktop'

let runtimePromise: Promise<SidecarInfo> | null = null
let transportPromise: Promise<Transport> | null = null

function fallbackRuntimeInfo(): SidecarInfo {
  const envUrl = import.meta.env.VITE_CORAL_GRPC_WEB_URL
  if (envUrl) return { packaged: false, grpcBaseUrl: envUrl }
  // No desktop bridge and no window (SSR/test): fail fast with a clear config
  // error instead of building a transport with an empty baseUrl that only breaks
  // later at request time.
  if (typeof window === 'undefined') {
    throw new Error(
      'Coral runtime URL unavailable: no desktop bridge, no window, and VITE_CORAL_GRPC_WEB_URL is unset.',
    )
  }
  return { packaged: false, grpcBaseUrl: window.location.origin }
}

async function loadCoralRuntime(): Promise<SidecarInfo> {
  const desktop = coralDesktopApi()
  if (!desktop) return fallbackRuntimeInfo()
  return desktop.awaitInitialization()
}

export function ensureCoralRuntime(): Promise<SidecarInfo> {
  if (runtimePromise) return runtimePromise
  const promise = loadCoralRuntime()
  // Drop the cached promise on failure so a later call (e.g. a Retry) can
  // re-attempt instead of re-resolving the same rejection forever.
  promise.catch(() => {
    if (runtimePromise === promise) runtimePromise = null
  })
  runtimePromise = promise
  return promise
}

// Single memoized transport shared by every gRPC-web client. createClient() is a
// cheap sync wrapper, so callers build their per-service client on top of this.
export function getCoralTransport(): Promise<Transport> {
  if (transportPromise) return transportPromise
  const promise = ensureCoralRuntime().then((runtime) =>
    createGrpcWebTransport({ baseUrl: runtime.grpcBaseUrl }),
  )
  promise.catch(() => {
    if (transportPromise === promise) transportPromise = null
  })
  transportPromise = promise
  return promise
}
