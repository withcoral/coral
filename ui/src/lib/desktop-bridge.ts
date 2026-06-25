export type DesktopMcpClientId = 'codex' | 'claude-desktop' | 'cursor' | 'vscode' | 'opencode'

export interface DesktopCliInstallResult {
  commandPath: string
  installKind: 'alias' | 'cmd'
  shellConfigPath?: string
  targetPath: string
  onPath: boolean
}

export interface DesktopMcpClientDescriptor {
  id: DesktopMcpClientId
  name: string
  configPath: string
}

export interface DesktopMcpConfigureResult {
  client: DesktopMcpClientDescriptor
  configPath: string
}

export interface DesktopMcpTestResult {
  client: DesktopMcpClientDescriptor
  launchUrl: string
  message: string
}

type DesktopAction =
  | { action: 'install-cli' }
  | { action: 'list-mcp-clients' }
  | { action: 'configure-mcp'; clientId: DesktopMcpClientId }
  | { action: 'test-mcp'; clientId: DesktopMcpClientId }

type DesktopBridgeRequest = DesktopAction & {
  id: string
  type: 'coral-desktop:request'
}

type DesktopBridgeResponse =
  | {
      id: string
      ok: true
      result: unknown
      type: 'coral-desktop:response'
    }
  | {
      error: string
      id: string
      ok: false
      type: 'coral-desktop:response'
    }

const RESPONSE_TIMEOUT_MS = 12_000

export function isDesktopBridgeLikelyAvailable(): boolean {
  return window.parent !== window
}

function isDesktopBridgeResponse(value: unknown, id: string): value is DesktopBridgeResponse {
  if (!value || typeof value !== 'object') return false
  const candidate = value as { id?: unknown; type?: unknown }
  return candidate.type === 'coral-desktop:response' && candidate.id === id
}

function requestDesktop<T>(action: DesktopAction): Promise<T> {
  if (!isDesktopBridgeLikelyAvailable()) {
    return Promise.reject(new Error('Coral Desktop is required for local integrations.'))
  }

  const id = crypto.randomUUID()
  const message: DesktopBridgeRequest = {
    id,
    type: 'coral-desktop:request',
    ...action,
  }

  return new Promise<T>((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      window.removeEventListener('message', onMessage)
      reject(new Error('Coral Desktop did not respond.'))
    }, RESPONSE_TIMEOUT_MS)

    function onMessage(event: MessageEvent) {
      if (!isDesktopBridgeResponse(event.data, id)) return
      window.clearTimeout(timeout)
      window.removeEventListener('message', onMessage)

      if (event.data.ok) {
        resolve(event.data.result as T)
      } else {
        reject(new Error(event.data.error))
      }
    }

    window.addEventListener('message', onMessage)
    window.parent.postMessage(message, '*')
  })
}

export function installDesktopCliAlias(): Promise<DesktopCliInstallResult> {
  return requestDesktop<DesktopCliInstallResult>({ action: 'install-cli' })
}

export function listDesktopMcpClients(): Promise<DesktopMcpClientDescriptor[]> {
  return requestDesktop<DesktopMcpClientDescriptor[]>({ action: 'list-mcp-clients' })
}

export function configureDesktopMcpClient(
  clientId: DesktopMcpClientId,
): Promise<DesktopMcpConfigureResult> {
  return requestDesktop<DesktopMcpConfigureResult>({ action: 'configure-mcp', clientId })
}

export function testDesktopMcpClient(clientId: DesktopMcpClientId): Promise<DesktopMcpTestResult> {
  return requestDesktop<DesktopMcpTestResult>({ action: 'test-mcp', clientId })
}
