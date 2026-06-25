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
const onboardingPanel = document.querySelector<HTMLElement>('#onboarding-panel')
const onboardingSkip = document.querySelector<HTMLButtonElement>('#onboarding-skip')
const onboardingBrowse = document.querySelector<HTMLButtonElement>('#onboarding-browse')
const onboardingSources = document.querySelectorAll<HTMLButtonElement>('.onboarding__source')

const ONBOARDING_STORAGE_KEY = 'coral.desktop.onboarding.seen.v1'
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
  onboardingPanel: requireElement(onboardingPanel, 'onboarding panel'),
  onboardingSkip: requireElement(onboardingSkip, 'onboarding skip button'),
  onboardingBrowse: requireElement(onboardingBrowse, 'onboarding browse button'),
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

function hasSeenOnboarding(): boolean {
  try {
    return localStorage.getItem(ONBOARDING_STORAGE_KEY) === 'true'
  } catch {
    return false
  }
}

function markOnboardingSeen() {
  try {
    localStorage.setItem(ONBOARDING_STORAGE_KEY, 'true')
  } catch {
    // Best-effort preference only.
  }
}

function setCoralRoute(hash: string) {
  if (!coralBaseUrl) return
  const nextUrl = new URL(coralBaseUrl.toString())
  nextUrl.hash = hash
  ui.frame.src = nextUrl.toString()
}

function showOnboarding() {
  ui.onboardingPanel.hidden = false
  ui.onboardingPanel.classList.remove('hidden')
}

function hideOnboarding() {
  markOnboardingSeen()
  ui.onboardingPanel.hidden = true
  ui.onboardingPanel.classList.add('hidden')
}

function openSuggestedSource(sourceName: string) {
  setCoralRoute(`/sources?install=${encodeURIComponent(sourceName)}`)
  hideOnboarding()
}

async function initialize() {
  try {
    const info = await desktopApi().awaitInitialization()
    coralBaseUrl = new URL(info.url)
    setCoralRoute('/sources')
    ui.frame.classList.remove('hidden')
    ui.runtimePanel.classList.add('hidden')
    if (!hasSeenOnboarding()) showOnboarding()
  } catch (error) {
    ui.startupDetail.textContent = error instanceof Error ? error.message : String(error)
    ui.startupDetail.hidden = false
  }
}

ui.onboardingSkip.addEventListener('click', hideOnboarding)
ui.onboardingBrowse.addEventListener('click', () => {
  setCoralRoute('/sources')
  hideOnboarding()
})
for (const button of onboardingSources) {
  button.addEventListener('click', () => {
    const sourceName = button.dataset.source
    if (sourceName) openSuggestedSource(sourceName)
  })
}

window.coralDesktop?.onShowOnboarding(showOnboarding)
window.addEventListener('message', handleDesktopBridgeRequest)

void initialize()
