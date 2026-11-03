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
 * What the session cookie carries: the session handle Coral issued, and the
 * session endpoint discovered when the session started. The cookie is signed,
 * and the endpoint is checked against the issuer again on every use.
 */
interface StoredWebSession {
  endpoint: string
  handle: string
}

/** A web session Coral just started for this browser. */
export interface StartedWebSession extends StoredWebSession {
  /** Seconds until Coral ends the session. */
  expiresIn: number
}

/**
 * Writes the session cookie, refusing to write one that cannot be read back.
 *
 * [`resolveCoralWebSession`] treats a session ending within
 * [`CLOCK_SKEW_SECONDS`] as already gone, so a shorter one is unusable from the
 * moment it is committed. Left to itself that is not an error anywhere: the
 * callback succeeds, a protected route finds no session, and the visitor
 * returns to `/login` in a loop with no failure in it. A small
 * `CORAL_WEB_SESSION_MAX_AGE_SECONDS` or a short Coral refresh-token lifetime can
 * cause it, so the message names both.
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
  const stored: StoredWebSession = { endpoint: session.endpoint, handle: session.handle }
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
  const stored = await readStoredSession(sessionCookie(config), request, config)
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
  return (await readStoredSession(sessionCookie(config), request, config)) !== null
}

/** The handle in this request's session cookie, unchecked with Coral. */
export async function coralWebSessionHandle(
  request: Request,
  config: RequiredAuthConfig,
): Promise<string | null> {
  return (await readStoredSession(sessionCookie(config), request, config))?.handle ?? null
}

/** What ending a web session at Coral came to. */
export type SignOutOutcome = 'ended' | 'unfinished'

/**
 * Ends the session this request's session cookie names, and the grant
 * behind it, at Coral. Without that cookie it finishes an earlier sign-out
 * that Coral did not answer, if one is pending.
 *
 * Ending is idempotent at Coral, so a repeat is safe. `unfinished` means
 * Coral did not answer: the caller keeps the pending sign-out so the next
 * visit retries it.
 */
export async function endCoralWebSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<SignOutOutcome> {
  const stored =
    (await readStoredSession(sessionCookie(config), request, config)) ??
    (await readStoredSession(pendingSignOutCookie(config), request, config))
  if (!stored) return 'ended'
  let response: Response
  try {
    response = await postSessionForm(`${stored.endpoint}/end`, {
      client_id: authClientId(config),
      session: stored.handle,
    })
  } catch {
    return 'unfinished'
  }
  return response.status === 200 ? 'ended' : 'unfinished'
}

export async function clearCoralWebSession(config: RequiredAuthConfig): Promise<string> {
  return sessionCookie(config).serialize('', { maxAge: 0 })
}

/**
 * Keeps the ended session's handle in a cookie of its own, so the next visit
 * can finish a sign-out Coral did not answer. No route reads it as a session.
 */
export async function commitPendingSignOut(
  request: Request,
  config: RequiredAuthConfig,
): Promise<string | null> {
  const stored = await readStoredSession(sessionCookie(config), request, config)
  if (!stored) return null
  return pendingSignOutCookie(config).serialize(stored)
}

/** Whether this request carries a sign-out that has not finished. */
export async function hasPendingSignOut(
  request: Request,
  config: RequiredAuthConfig,
): Promise<boolean> {
  return (await readStoredSession(pendingSignOutCookie(config), request, config)) !== null
}

export async function clearPendingSignOut(config: RequiredAuthConfig): Promise<string> {
  return pendingSignOutCookie(config).serialize('', { maxAge: 0 })
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
  cookie: ReturnType<typeof createCookie>,
  request: Request,
  config: RequiredAuthConfig,
): Promise<StoredWebSession | null> {
  const value = (await cookie.parse(request.headers.get('cookie'))) as unknown
  if (!value || typeof value !== 'object') return null
  const candidate = value as Partial<StoredWebSession>
  if (
    typeof candidate.handle !== 'string' ||
    !candidate.handle ||
    typeof candidate.endpoint !== 'string' ||
    !isIssuerEndpoint(candidate.endpoint, config.issuer)
  ) {
    return null
  }
  return { endpoint: candidate.endpoint, handle: candidate.handle }
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
