import './styles.css'
import type { CoralDesktopApi, McpClientId } from '../../shared/types'

declare global {
  interface Window {
    coralDesktop: CoralDesktopApi
  }
}

const startupDetail = document.querySelector<HTMLParagraphElement>('#startup-detail')
const runtimePanel = document.querySelector<HTMLElement>('#runtime-panel')
const frame = document.querySelector<HTMLIFrameElement>('#coral-frame')

let coralBaseUrl: URL | null = null

interface DesktopBridgeRequest {
  action: string
  clientId?: unknown
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

const MCP_CLIENT_IDS = new Set<McpClientId>([
  'codex',
  'claude-desktop',
  'cursor',
  'vscode',
  'opencode',
])

function requireElement<T extends Element>(element: T | null, name: string): T {
  if (!element) throw new Error(`Missing ${name}`)
  return element
}

const ui = {
  startupDetail: requireElement(startupDetail, 'startup detail'),
  runtimePanel: requireElement(runtimePanel, 'runtime panel'),
  frame: requireElement(frame, 'coral frame'),
}

function desktopApi(): CoralDesktopApi {
  if (!window.coralDesktop) {
    throw new Error('Electron preload bridge is unavailable. Restart the Electron window from `npm run dev --prefix desktop`.')
  }
  return window.coralDesktop
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function isDesktopBridgeRequest(value: unknown): value is DesktopBridgeRequest {
  return (
    isObject(value) &&
    value.type === 'coral-desktop:request' &&
    typeof value.id === 'string' &&
    typeof value.action === 'string'
  )
}

function isMcpClientId(value: unknown): value is McpClientId {
  return typeof value === 'string' && MCP_CLIENT_IDS.has(value as McpClientId)
}

function bridgeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

async function runDesktopBridgeAction(request: DesktopBridgeRequest): Promise<unknown> {
  switch (request.action) {
    case 'install-cli':
      return desktopApi().installCli()
    case 'list-mcp-clients':
      return desktopApi().listMcpClients()
    case 'configure-mcp':
      if (!isMcpClientId(request.clientId)) {
        throw new Error('Unknown MCP client.')
      }
      return desktopApi().configureMcp(request.clientId)
    case 'test-mcp':
      if (!isMcpClientId(request.clientId)) {
        throw new Error('Unknown MCP client.')
      }
      return desktopApi().testMcp(request.clientId)
    default:
      throw new Error(`Unsupported desktop action: ${request.action}`)
  }
}

function postDesktopBridgeResponse(source: Window, response: DesktopBridgeResponse) {
  source.postMessage(response, '*')
}

function handleDesktopBridgeRequest(event: MessageEvent) {
  const source = ui.frame.contentWindow
  if (!source || event.source !== source || !isDesktopBridgeRequest(event.data)) return
  const request = event.data

  void runDesktopBridgeAction(request)
    .then((result) =>
      postDesktopBridgeResponse(source, {
        id: request.id,
        ok: true,
        result,
        type: 'coral-desktop:response',
      }),
    )
    .catch((error: unknown) =>
      postDesktopBridgeResponse(source, {
        error: bridgeError(error),
        id: request.id,
        ok: false,
        type: 'coral-desktop:response',
      }),
    )
}

function setCoralRoute(hash: string) {
  if (!coralBaseUrl) return
  const nextUrl = new URL(coralBaseUrl.toString())
  nextUrl.hash = hash
  ui.frame.src = nextUrl.toString()
}

async function initialize() {
  try {
    const info = await desktopApi().awaitInitialization()
    coralBaseUrl = new URL(info.url)
    setCoralRoute('/sources')
    ui.frame.classList.remove('hidden')
    ui.runtimePanel.classList.add('hidden')
  } catch (error) {
    ui.startupDetail.textContent = error instanceof Error ? error.message : String(error)
    ui.startupDetail.hidden = false
  }
}

window.addEventListener('message', handleDesktopBridgeRequest)

void initialize()
