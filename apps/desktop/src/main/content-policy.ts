export type NavigationDecision = 'allow' | 'open-externally' | 'deny'

/**
 * Decides where a navigation from web content goes. The window shows only
 * Reef's origin. Other web pages open in the system browser; every other
 * scheme, and any URL with credentials in it, goes nowhere.
 */
export function navigationDecision(target: string, reefOrigin: string | null): NavigationDecision {
  let url: URL
  try {
    url = new URL(target)
  } catch {
    return 'deny'
  }
  // URL.origin leaves out credentials, so check them before the origin.
  if (url.username || url.password) return 'deny'
  if (reefOrigin !== null && url.origin === reefOrigin) return 'allow'
  if (url.protocol === 'http:' || url.protocol === 'https:') return 'open-externally'
  return 'deny'
}

export type WindowOpenDecision =
  | 'submit-hidden'
  | 'open-externally'
  | 'deny'
  /** Desktop offers to install this version. */
  | { offerUpdate: string }

/**
 * Decides where a request for a new window goes. A link opens in the system
 * browser, which reaches the same loopback Reef. The system browser cannot
 * repeat a form's POST, so a Reef form that targets a new tab, such as a
 * device sign-in, runs in a hidden window. Its consent redirect then reaches
 * the hand-off. A form for any other origin goes nowhere.
 *
 * `runningVersion` is Desktop's version when it can update itself, else null.
 * Reef's sidebar notice links to a newer release's notes, and Desktop offers
 * that update instead of opening the page. The notes of an older or equal
 * release open in the system browser.
 */
export function windowOpenDecision(
  target: string,
  submitsForm: boolean,
  reefOrigin: string | null,
  runningVersion: string | null,
): WindowOpenDecision {
  const decision = navigationDecision(target, reefOrigin)
  if (!submitsForm) {
    if (decision === 'deny') return 'deny'
    const release = releaseVersion(target)
    return runningVersion !== null && release !== null && isNewer(release, runningVersion)
      ? { offerUpdate: release }
      : 'open-externally'
  }
  return decision === 'allow' ? 'submit-hidden' : 'deny'
}

const RELEASE_NOTES = 'https://github.com/withcoral/coral/releases/tag/v'

/**
 * The stable `X.Y.Z` version that a Coral release notes URL names, or null for
 * any other URL. The runtime announces stable releases only.
 */
function releaseVersion(target: string): string | null {
  const url = URL.parse(target)
  if (url === null || url.username || url.password || url.search || url.hash) return null
  const href = url.href
  if (!href.startsWith(RELEASE_NOTES)) return null
  const version = href.slice(RELEASE_NOTES.length)
  return /^\d+\.\d+\.\d+$/.test(version) ? version : null
}

/**
 * Whether the stable `release` is newer than `running`. A prerelease or a build
 * of `X.Y.Z` is older than `X.Y.Z`. A `running` that is not a version is never
 * older.
 */
function isNewer(release: string, running: string): boolean {
  const match = /^(\d+)\.(\d+)\.(\d+)([-+].*)?$/.exec(running)
  if (match === null) return false
  const current = match.slice(1, 4).map(Number)
  const next = release.split('.').map(Number)
  for (let i = 0; i < 3; i++) if (next[i] !== current[i]) return next[i] > current[i]
  return match[4]?.startsWith('-') ?? false
}

/** The release notes page of a Coral version. */
export function releaseNotesUrl(version: string): string {
  return `${RELEASE_NOTES}${version}`
}

export type FormWindowStep = 'wait' | 'show' | 'close'

/**
 * What a hidden form window does with one main-frame response. It closes after
 * a consent hand-off, or when Desktop refuses one. It stays hidden through a
 * redirect, and it shows the first page Reef answers with, as a browser shows
 * the new tab.
 */
export function formWindowStep(
  handOff: ConsentHandOff['action'],
  statusCode: number,
): FormWindowStep {
  if (handOff !== 'pass') return 'close'
  return REDIRECTS.has(statusCode) ? 'wait' : 'show'
}

/**
 * Grants Reef the one browser permission it uses: writing copied text to the
 * clipboard. Everything else, from anywhere, is denied.
 */
export function permissionGranted(
  permission: string,
  requestingUrl: string,
  reefOrigin: string | null,
): boolean {
  return (
    permission === 'clipboard-sanitized-write' &&
    navigationDecision(requestingUrl, reefOrigin) === 'allow'
  )
}

/** What a response looks like to Electron's `onHeadersReceived`. */
export interface ReceivedResponse {
  method: string
  resourceType: string
  url: string
  statusCode: number
  headers: Record<string, string[]>
}

export type ConsentHandOff =
  | { action: 'pass' }
  /** Cancel the response: Reef marked it, but it breaks the hand-off contract. */
  | { action: 'refuse' }
  /** Open `consent` in the system browser and give the window this response instead. */
  | {
      action: 'hand-off'
      consent: string
      statusLine: string
      headers: Record<string, string[]>
    }

const RETURN_MARKER = 'x-coral-authorization-return-to'
/** The 3xx statuses that redirect; 304 and its kin do not. */
const REDIRECTS = new Set([301, 302, 303, 307, 308])

/**
 * Hands Provider consent to the system browser (ADR 0009). Chromium applies
 * Reef's `form-action` to a form's redirects, so the window cannot follow
 * Reef's redirect to an external consent page. When Reef marks a document
 * POST's redirect with `X-Coral-Authorization-Return-To`, the system browser
 * opens the redirect's target, and the window goes to the marked path on
 * Reef's own origin. This is not a second destination policy: Reef screened
 * the URL when it issued it. Desktop refuses only what the system browser
 * must never be asked to open, such as a `file:` URL or one with credentials.
 */
export function consentHandOff(response: ReceivedResponse, reefOrigin: string): ConsentHandOff {
  const header = (name: string) =>
    Object.entries(response.headers).find(([key]) => key.toLowerCase() === name)?.[1][0]
  const returnTo = header(RETURN_MARKER)
  if (
    returnTo === undefined ||
    response.method !== 'POST' ||
    response.resourceType !== 'mainFrame' ||
    navigationDecision(response.url, reefOrigin) !== 'allow'
  )
    return { action: 'pass' }

  const location = header('location')
  if (!REDIRECTS.has(response.statusCode) || location === undefined) return { action: 'refuse' }
  const consent = URL.parse(location, response.url)
  const back = URL.parse(returnTo, response.url)
  if (
    consent === null ||
    (consent.protocol !== 'http:' && consent.protocol !== 'https:') ||
    consent.username ||
    consent.password ||
    back === null ||
    navigationDecision(back.href, reefOrigin) !== 'allow'
  )
    return { action: 'refuse' }

  const headers = Object.fromEntries(
    Object.entries(response.headers).filter(
      ([key]) => ![RETURN_MARKER, 'location'].includes(key.toLowerCase()),
    ),
  )
  headers.location = [back.href]
  return {
    action: 'hand-off',
    consent: consent.href,
    statusLine: 'HTTP/1.1 303 See Other',
    headers,
  }
}
