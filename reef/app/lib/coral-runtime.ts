import { coralDesktopApi, type SidecarInfo } from './coral-desktop'

const CORAL_GRPC_PATH_RE = /^\/coral\.v1\.[^/]+\/[^/]+$/

let fetchBridgeInstalled = false
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

function requestUrl(input: RequestInfo | URL): URL | null {
  try {
    const href = input instanceof Request ? input.url : input
    return new URL(href, window.location.href)
  } catch {
    return null
  }
}

function requestMethod(input: RequestInfo | URL, init?: RequestInit): string {
  return (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase()
}

function shouldBridge(input: RequestInfo | URL, init?: RequestInit): URL | null {
  const url = requestUrl(input)
  if (!url) return null
  if (url.origin !== window.location.origin) return null
  if (requestMethod(input, init) !== 'POST') return null
  if (!CORAL_GRPC_PATH_RE.test(url.pathname)) return null
  return url
}

export function installCoralRuntimeFetchBridge() {
  if (typeof window === 'undefined') return
  if (fetchBridgeInstalled) return

  fetchBridgeInstalled = true
  const originalFetch = window.fetch.bind(window)

  window.fetch = async (input, init) => {
    const bridgedUrl = shouldBridge(input, init)
    if (!bridgedUrl) return originalFetch(input, init)

    const runtime = await ensureCoralRuntime()
    const targetUrl = new URL(`${bridgedUrl.pathname}${bridgedUrl.search}`, runtime.url)
    if (input instanceof Request)
      return originalFetch(new Request(targetUrl, new Request(input, init)))
    return originalFetch(targetUrl, init)
  }
}
