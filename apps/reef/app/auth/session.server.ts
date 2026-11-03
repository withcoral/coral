import { randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'

import {
  authClientId,
  authCookieSecure,
  hostBoundCookieName,
  isIssuerEndpoint,
} from './config.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const OAUTH_COOKIE_NAME = 'coral_web_oauth'
const OAUTH_MAX_AGE_SECONDS = 10 * 60
const CLOCK_SKEW_SECONDS = 30
/**
 * How long the session cookie outlives its session. Reef then still sees the
 * cookie when Coral ends the session, and offers sign-in as an explicit step
 * instead of starting it again by itself.
 */
const SESSION_COOKIE_GRACE_SECONDS = 5 * 60

export interface OAuthTransaction {
  codeVerifier: string
  returnTo: string
  state: string
}

/**
 * What actually goes in the cookie: the transaction plus the moment it was
 * issued.
 *
 * `issuedAt` is stamped here rather than accepted from the caller, and stripped
 * again on the way out, so a transaction stays the three fields a login flow
 * cares about at both ends.
 */
interface StoredOAuthTransaction extends OAuthTransaction {
  issuedAt: number
}

export async function commitOAuthTransaction(
  transaction: OAuthTransaction,
  config: RequiredAuthConfig,
): Promise<string> {
  const stored: StoredOAuthTransaction = { ...transaction, issuedAt: unixTimestamp() }

  return oauthCookie(config).serialize(stored)
}

/**
 * Reads a transaction, treating one issued too long ago as absent.
 *
 * The signature proves the payload came from this server; it says nothing about
 * when. Without `issuedAt` the blob verifies forever, because it carries no
 * timestamp and nothing else in the value varies — `Max-Age` is the only bound,
 * and that one is the browser's to enforce. Anything replaying the cookie
 * outside a browser is not bound by it, so a captured `codeVerifier` would stay
 * usable for as long as its authorization code did. PKCE assumes the verifier
 * is short-lived as well as secret, so the server checks the age itself.
 *
 * A cookie dated in the future is *not* rejected. It cannot be forged without
 * the secret, so the only thing that produces one is a clock that disagrees
 * between the instance that wrote it and the instance reading it — which is why
 * the past side carries [`CLOCK_SKEW_SECONDS`] of slack too. Rejecting a
 * future-dated cookie would fail those logins and prevent no attack.
 */
export async function readOAuthTransaction(
  request: Request,
  config: RequiredAuthConfig,
): Promise<OAuthTransaction | null> {
  const value = await oauthCookie(config).parse(request.headers.get('cookie'))
  if (!isStoredOAuthTransaction(value)) return null
  if (unixTimestamp() - value.issuedAt > OAUTH_MAX_AGE_SECONDS + CLOCK_SKEW_SECONDS) return null

  const { codeVerifier, returnTo, state } = value

  return { codeVerifier, returnTo, state }
}

export async function clearOAuthTransaction(config: RequiredAuthConfig): Promise<string> {
  return oauthCookie(config).serialize('', { maxAge: 0 })
}

/**
 * What the session cookie carries: the session handle Coral issued, the
 * session endpoint discovered when the session started, and when the session
 * ends. The cookie is signed, and the endpoint is checked against the issuer
 * again on every use.
 */
interface StoredWebSession {
  endpoint: string
  handle: string
  /** When Coral ends the session, in Unix seconds. */
  endsAt: number
}

/** A web session Coral just started for this browser. */
export interface StartedWebSession {
  endpoint: string
  handle: string
  /** Seconds until Coral ends the session. */
  expiresIn: number
}

/** The most sign-outs one browser keeps to finish. The newest are kept. */
const MAX_PENDING_SIGN_OUTS = 4

/**
 * Writes the session cookie, refusing to write one that cannot be used.
 *
 * A session that ends within [`CLOCK_SKEW_SECONDS`] would be gone before the
 * redirect after sign-in reached a protected route, and the visitor would
 * return to `/login` in a loop with no failure in it. A small
 * `CORAL_WEB_SESSION_MAX_AGE_SECONDS` or a short Coral refresh-token lifetime
 * can cause it, so the message names both.
 */
export async function commitCoralWebSession(
  session: StartedWebSession,
  config: RequiredAuthConfig,
): Promise<string> {
  if (session.expiresIn <= CLOCK_SKEW_SECONDS) {
    throw new Error(
      `Refusing to establish a Coral web session that expires within ${CLOCK_SKEW_SECONDS}s: check CORAL_WEB_SESSION_MAX_AGE_SECONDS and Coral's refresh_token_ttl_seconds`,
    )
  }
  const stored: StoredWebSession = {
    endpoint: session.endpoint,
    endsAt: unixTimestamp() + session.expiresIn,
    handle: session.handle,
  }
  return sessionCookie(config).serialize(stored, {
    maxAge: session.expiresIn + SESSION_COOKIE_GRACE_SECONDS,
  })
}

/**
 * Resolves this request's web session into a Reef access token Coral mints
 * for it, or `null` when the request has no usable session.
 *
 * Coral answers from one database read and writes nothing, so concurrent
 * requests, tabs, and Reef processes resolve one session without
 * coordinating, and a retry repeats nothing.
 *
 * @throws a 503 `Response` when Coral does not answer. The session may still
 * be valid, so the cookie stays.
 */
export async function resolveCoralWebSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<AuthSession | null> {
  const stored = await readStoredSession(request, config)
  if (!stored) return null
  const response = await postSessionForm(`${stored.endpoint}/token`, {
    client_id: authClientId(config),
    session: stored.handle,
  })
  if (await isInvalidGrant(response)) return null
  if (response.status !== 200) throw sessionUnavailable()
  const body = (await response.json().catch(() => null)) as SessionTokenResponse | null
  if (
    !body ||
    typeof body.access_token !== 'string' ||
    !body.access_token ||
    !isPositiveSeconds(body.expires_in) ||
    !isPositiveSeconds(body.session_expires_in)
  ) {
    throw sessionUnavailable()
  }
  // Coral decides when the session ends and never mints a token past it, so
  // no local margin cuts the session short.
  const now = unixTimestamp()
  return {
    accessToken: body.access_token,
    expiresAt: now + body.expires_in,
    handle: stored.handle,
    sessionExpiresAt: now + body.session_expires_in,
  }
}

/** Whether a request carries a session cookie at all, valid or not. */
export async function hasCoralWebSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<boolean> {
  return (await readStoredSession(request, config)) !== null
}

/** The handle in this request's session cookie, unchecked with Coral. */
export async function coralWebSessionHandle(
  request: Request,
  config: RequiredAuthConfig,
): Promise<string | null> {
  return (await readStoredSession(request, config))?.handle ?? null
}

/** The cookies a sign-out step sets, and whether any sign-out is unfinished. */
export interface SignOutCookies {
  setCookies: string[]
  unfinished: boolean
}

/**
 * Ends this request's web session, and the grant behind it, at Coral, and
 * clears the session cookie. A session Coral does not answer for joins the
 * browser's pending sign-outs, which this retries too.
 *
 * Ending is idempotent at Coral, so retrying is safe. The browser loses the
 * session either way; until a pending sign-out finishes, a copy of the old
 * cookie elsewhere can still use it, so the caller reports it.
 */
export async function signOutCoralWebSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<SignOutCookies> {
  const current = await readStoredSession(request, config)
  const unanswered = current && !(await endAtCoral(current, config)) ? [current] : []
  const pending = await retryPendingSignOuts(request, config, unanswered)
  return {
    setCookies: [pending.setCookie, await clearCoralWebSession(config)],
    unfinished: pending.unfinished,
  }
}

/**
 * Retries the sign-outs this browser kept because Coral did not answer, and
 * only those: the browser's current session, if any, is left alone.
 *
 * Returns `null` when the browser has no sign-out pending.
 */
export async function finishPendingSignOuts(
  request: Request,
  config: RequiredAuthConfig,
): Promise<{ setCookie: string; unfinished: boolean } | null> {
  if ((await readPendingSignOuts(request, config)).length === 0) return null
  return retryPendingSignOuts(request, config, [])
}

export async function clearCoralWebSession(config: RequiredAuthConfig): Promise<string> {
  return sessionCookie(config).serialize('', { maxAge: 0 })
}

/**
 * Retries every pending sign-out, adds `unanswered`, and returns the pending
 * cookie that remains. A session past its own end needs no sign-out: Coral
 * has already ended it with its grant. The cookie lasts until the latest
 * pending session would have ended anyway.
 */
async function retryPendingSignOuts(
  request: Request,
  config: RequiredAuthConfig,
  unanswered: StoredWebSession[],
): Promise<{ setCookie: string; unfinished: boolean }> {
  const now = unixTimestamp()
  const remaining: StoredWebSession[] = []
  for (const pending of await readPendingSignOuts(request, config)) {
    if (pending.endsAt > now && !(await endAtCoral(pending, config))) remaining.push(pending)
  }
  remaining.push(...unanswered.filter((session) => session.endsAt > now))
  const kept = remaining.slice(-MAX_PENDING_SIGN_OUTS)
  if (kept.length === 0) {
    return {
      setCookie: await pendingSignOutCookie(config).serialize('', { maxAge: 0 }),
      unfinished: false,
    }
  }
  const maxAge = Math.max(...kept.map((session) => session.endsAt)) - now
  return {
    setCookie: await pendingSignOutCookie(config).serialize(kept, { maxAge }),
    unfinished: true,
  }
}

/** Whether Coral answered that the session is ended. */
async function endAtCoral(session: StoredWebSession, config: RequiredAuthConfig): Promise<boolean> {
  try {
    const response = await postSessionForm(`${session.endpoint}/end`, {
      client_id: authClientId(config),
      session: session.handle,
    })
    return response.status === 200
  } catch {
    return false
  }
}

interface SessionTokenResponse {
  access_token?: unknown
  expires_in?: unknown
  session_expires_in?: unknown
}

async function postSessionForm(url: string, form: Record<string, string>): Promise<Response> {
  try {
    return await fetch(url, {
      body: new URLSearchParams(form),
      headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
      method: 'POST',
      redirect: 'manual',
    })
  } catch {
    throw sessionUnavailable()
  }
}

async function isInvalidGrant(response: Response): Promise<boolean> {
  if (response.status !== 400) return false
  const body = (await response
    .clone()
    .json()
    .catch(() => null)) as { error?: unknown } | null
  return body?.error === 'invalid_grant'
}

function isPositiveSeconds(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0
}

function sessionUnavailable(): Response {
  return new Response('Sign-in is temporarily unavailable. Try again.', { status: 503 })
}

async function readStoredSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<StoredWebSession | null> {
  return storedSession(await sessionCookie(config).parse(request.headers.get('cookie')), config)
}

async function readPendingSignOuts(
  request: Request,
  config: RequiredAuthConfig,
): Promise<StoredWebSession[]> {
  const value = (await pendingSignOutCookie(config).parse(request.headers.get('cookie'))) as unknown
  if (!Array.isArray(value)) return []
  return value
    .map((entry) => storedSession(entry, config))
    .filter((entry) => entry !== null)
    .slice(-MAX_PENDING_SIGN_OUTS)
}

function storedSession(value: unknown, config: RequiredAuthConfig): StoredWebSession | null {
  if (!value || typeof value !== 'object') return null
  const candidate = value as Partial<StoredWebSession>
  if (
    typeof candidate.handle !== 'string' ||
    !candidate.handle ||
    typeof candidate.endpoint !== 'string' ||
    !isIssuerEndpoint(candidate.endpoint, config.issuer) ||
    typeof candidate.endsAt !== 'number' ||
    !Number.isFinite(candidate.endsAt)
  ) {
    return null
  }
  return { endpoint: candidate.endpoint, endsAt: candidate.endsAt, handle: candidate.handle }
}

export function randomToken(byteCount: number): string {
  return randomBytes(byteCount).toString('base64url')
}

/**
 * The OAuth transaction cookie's name, bound to Reef's host wherever the browser
 * allows it. Were a sibling host able to toss one, the transaction the callback
 * validates its `state` against would be one the attacker started. The session
 * cookie's name is bound the same way when the configuration is resolved; this
 * one is not configurable, so it is derived here. `hostBoundCookieName` explains
 * the prefix.
 */
export function oauthCookieName(config: RequiredAuthConfig): string {
  return hostBoundCookieName(OAUTH_COOKIE_NAME, authCookieSecure(config))
}

function oauthCookie(config: RequiredAuthConfig) {
  return createCookie(oauthCookieName(config), {
    httpOnly: true,
    maxAge: OAUTH_MAX_AGE_SECONDS,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

function sessionCookie(config: RequiredAuthConfig) {
  return createCookie(config.cookieName, {
    httpOnly: true,
    maxAge: config.sessionMaxAgeSeconds,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

function pendingSignOutCookie(config: RequiredAuthConfig) {
  return createCookie(`${config.cookieName}_signout`, {
    httpOnly: true,
    maxAge: config.sessionMaxAgeSeconds,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

function isStoredOAuthTransaction(value: unknown): value is StoredOAuthTransaction {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<StoredOAuthTransaction>

  return (
    typeof candidate.codeVerifier === 'string' &&
    typeof candidate.returnTo === 'string' &&
    typeof candidate.state === 'string' &&
    typeof candidate.issuedAt === 'number' &&
    Number.isFinite(candidate.issuedAt)
  )
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}
