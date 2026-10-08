import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'

import { authCookieSecure } from './config.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const OAUTH_COOKIE_NAME = 'coral_ui_oauth'
const OAUTH_MAX_AGE_SECONDS = 10 * 60
const CLOCK_SKEW_SECONDS = 30

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
 * Writes a session cookie, refusing to write one that cannot be read back.
 *
 * `readCoralUISession` treats a session expiring within [`CLOCK_SKEW_SECONDS`] as
 * already gone, so a shorter-lived one is unreadable from the moment it is
 * committed. Left to itself that is not an error anywhere: the callback
 * succeeds, redirects to a protected route, the route finds no session and
 * sends the visitor back to `/login` — a loop with no failure in it. Two
 * unrelated settings can cause it, a Coral token with a tiny `expires_in` and a
 * small `CORAL_UI_SESSION_MAX_AGE_SECONDS`, so the message names both rather than
 * guessing.
 */
export async function commitCoralUISession(
  session: AuthSession,
  config: RequiredAuthConfig,
): Promise<string> {
  if (session.expiresAt <= unixTimestamp() + CLOCK_SKEW_SECONDS) {
    throw new Error(
      `Refusing to establish a Coral UI session that expires within ${CLOCK_SKEW_SECONDS}s: check the Coral access token lifetime and CORAL_UI_SESSION_MAX_AGE_SECONDS`,
    )
  }

  return sessionCookie(config).serialize(encryptSession(session, config.sessionSecret))
}

export async function readCoralUISession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<AuthSession | null> {
  const encrypted = await sessionCookie(config).parse(request.headers.get('cookie'))
  if (typeof encrypted !== 'string' || !encrypted) return null
  const session = decryptSession(encrypted, config.sessionSecret)
  if (!session || session.expiresAt <= unixTimestamp() + CLOCK_SKEW_SECONDS) return null

  return session
}

export async function clearCoralUISession(config: RequiredAuthConfig): Promise<string> {
  return sessionCookie(config).serialize('', { maxAge: 0 })
}

export function randomToken(byteCount: number): string {
  return randomBytes(byteCount).toString('base64url')
}

/**
 * The OAuth transaction cookie's name, which carries a `__Host-` prefix wherever
 * the browser will accept one.
 *
 * A signature keeps the value from being forged; it does nothing about a cookie
 * of the same name arriving from somewhere else. `Domain` is what makes that
 * possible: this cookie is host-only, so a sibling host never *receives* it, but
 * any host under the same registrable domain can *set* a domain-scoped
 * `coral_ui_oauth` that the browser will then send here. Both arrive, and
 * `parse` keeps whichever the browser listed first — which is the more specific
 * `Path`, and that is the attacker's to choose. The transaction the callback
 * validates its `state` against would be one the attacker started.
 *
 * `__Host-` forbids `Domain` at the browser, so the tossed cookie is refused at
 * the source rather than disambiguated here. It also requires `Secure`, hence
 * the fallback: the accepted all-loopback HTTP topology cannot carry the prefix,
 * and a cookie named with one there is silently discarded on every response.
 * `CORAL_UI_SESSION_COOKIE_NAME` is validated against the same rule in
 * `config.server.ts` — this is that reasoning applied to the cookie whose name
 * is not configurable.
 */
export function oauthCookieName(config: RequiredAuthConfig): string {
  return authCookieSecure(config) ? `__Host-${OAUTH_COOKIE_NAME}` : OAUTH_COOKIE_NAME
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

function encryptSession(session: AuthSession, secret: string): string {
  const iv = randomBytes(12)
  const cipher = createCipheriv('aes-256-gcm', encryptionKey(secret), iv)
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(session), 'utf8'), cipher.final()])
  const tag = cipher.getAuthTag()

  return [iv, tag, ciphertext].map((part) => part.toString('base64url')).join('.')
}

function decryptSession(value: string, secret: string): AuthSession | null {
  const [ivPart, tagPart, ciphertextPart] = value.split('.')
  if (!ivPart || !tagPart || !ciphertextPart) return null

  try {
    const decipher = createDecipheriv(
      'aes-256-gcm',
      encryptionKey(secret),
      Buffer.from(ivPart, 'base64url'),
    )
    decipher.setAuthTag(Buffer.from(tagPart, 'base64url'))
    const plaintext = Buffer.concat([
      decipher.update(Buffer.from(ciphertextPart, 'base64url')),
      decipher.final(),
    ]).toString('utf8')
    const session = JSON.parse(plaintext) as unknown

    return isAuthSession(session) ? session : null
  } catch {
    return null
  }
}

function encryptionKey(secret: string): Buffer {
  return createHash('sha256').update(secret).digest()
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

function isAuthSession(value: unknown): value is AuthSession {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<AuthSession>

  return (
    typeof candidate.accessToken === 'string' &&
    candidate.accessToken.length > 0 &&
    typeof candidate.expiresAt === 'number' &&
    Number.isFinite(candidate.expiresAt) &&
    typeof candidate.tokenType === 'string' &&
    candidate.tokenType.length > 0
  )
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}
