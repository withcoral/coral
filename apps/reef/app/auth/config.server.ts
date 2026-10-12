import { isExplicitLoopbackUrl, isLocalhostSubdomain } from '@/lib/loopback.server'
import type { AuthConfig, RequiredAuthConfig } from './types'

const DEFAULT_COOKIE_NAME = 'coral_web_session'
const DEFAULT_SESSION_MAX_AGE_SECONDS = 60 * 60

// RFC 6265 gives a cookie name the RFC 9110 `token` grammar: letters, digits,
// and `!#$%&'*+-.^_`|~`. Serialization does not enforce it — a name outside the
// grammar is written into `Set-Cookie` faithfully and rejected by the browser,
// so the only symptom is a session that never persists.
const COOKIE_NAME_TOKEN = /^[\w!#$%&'*+.^`|~-]+$/
// Browsers match a cookie-name prefix without regard to case.
const HOST_PREFIX = /^__host-/i

interface AuthConfigInput {
  env: NodeJS.ProcessEnv
}

export function coralWebAuthConfig(): AuthConfig {
  return resolveAuthConfig({ env: process.env })
}

export function resolveAuthConfig({ env }: AuthConfigInput): AuthConfig {
  const configuredMode = env.CORAL_WEB_AUTH_MODE?.trim().toLowerCase()
  if (!configuredMode) {
    if (env.NODE_ENV === 'production') {
      throw new Error('CORAL_WEB_AUTH_MODE must be set to disabled or required in production')
    }

    return { mode: 'disabled' }
  }
  if (configuredMode === 'disabled') return { mode: 'disabled' }
  if (configuredMode !== 'required') {
    throw new Error('CORAL_WEB_AUTH_MODE must be set to disabled or required')
  }

  return requiredAuthConfig(env)
}

function requiredAuthConfig(env: NodeJS.ProcessEnv): RequiredAuthConfig {
  const sessionSecret = env.CORAL_WEB_SESSION_SECRET?.trim()
  if (!sessionSecret || sessionSecret.length < 32) {
    throw new Error('CORAL_WEB_SESSION_SECRET must be at least 32 characters when auth is required')
  }

  const issuer = requiredString(env.CORAL_WEB_AUTH_ISSUER, 'CORAL_WEB_AUTH_ISSUER')
  const issuerUrl = httpUrl(issuer, 'CORAL_WEB_AUTH_ISSUER')
  if (issuerUrl.username || issuerUrl.password) {
    throw new Error('CORAL_WEB_AUTH_ISSUER must not include credentials')
  }
  if (issuerUrl.search || issuerUrl.hash) {
    throw new Error('CORAL_WEB_AUTH_ISSUER must not include a query string or fragment')
  }
  if (issuerUrl.protocol === 'http:' && !isExplicitLoopbackUrl(issuerUrl)) {
    throw new Error('CORAL_WEB_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP')
  }

  const publicUrl = publicOrigin(requiredString(env.CORAL_WEB_PUBLIC_URL, 'CORAL_WEB_PUBLIC_URL'))
  if (new URL(publicUrl).protocol === 'http:' && issuerUrl.protocol !== 'http:') {
    throw new Error(
      'CORAL_WEB_PUBLIC_URL may use HTTP only when CORAL_WEB_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  }

  return {
    cookieName: cookieNameEnv(env.CORAL_WEB_SESSION_COOKIE_NAME, publicUrl),
    issuer,
    mode: 'required',
    publicUrl,
    sessionMaxAgeSeconds: positiveIntegerEnv(
      env.CORAL_WEB_SESSION_MAX_AGE_SECONDS,
      'CORAL_WEB_SESSION_MAX_AGE_SECONDS',
      DEFAULT_SESSION_MAX_AGE_SECONDS,
    ),
    sessionSecret,
  }
}

export function authResource(config: RequiredAuthConfig): string {
  return config.publicUrl
}

export function authClientId(config: RequiredAuthConfig): string {
  return new URL('/.well-known/oauth-client', config.publicUrl).toString()
}

export function authRedirectUri(config: RequiredAuthConfig): string {
  return new URL('/auth/callback', config.publicUrl).toString()
}

/** Where an upstream Provider sends the browser back after Provider setup consent. */
export function providerSetupRedirectUri(config: RequiredAuthConfig): string {
  return new URL('/provider-setup/oauth/callback', config.publicUrl).toString()
}

export function authCookieSecure(config: RequiredAuthConfig): boolean {
  return isSecureOrigin(config.publicUrl)
}

/**
 * Prefixes a cookie name with `__Host-` wherever the browser will accept one.
 *
 * A signature keeps a cookie's value from being forged; it does nothing about a
 * cookie of the same name arriving from somewhere else. `Domain` is what makes
 * that possible: Coral web's cookies are host-only, so a sibling host never
 * *receives* them, but any host under the same registrable domain can *set* a
 * domain-scoped cookie of the same name that the browser will then send here.
 * Both arrive, the browser lists the one with the longer `Path` first — which
 * is the attacker's to choose — and the server reads that one. An attacker with
 * a Coral account of their own holds cookies this server signed, so no check on
 * the value tells theirs from the victim's.
 *
 * `__Host-` forbids `Domain` and any `Path` but `/` at the browser, so the
 * tossed cookie is refused at the source rather than disambiguated here. It also
 * requires `Secure`, hence the fallback: the accepted all-loopback HTTP topology
 * cannot carry the prefix, and a cookie named with one there is silently
 * discarded on every response. A name that already carries the prefix is left
 * alone.
 */
export function hostBoundCookieName(name: string, secure: boolean): string {
  if (!secure || HOST_PREFIX.test(name)) return name

  return `__Host-${name}`
}

function isSecureOrigin(publicUrl: string): boolean {
  return new URL(publicUrl).protocol === 'https:'
}

function requiredString(value: string | undefined, name: string): string {
  const configured = optionalString(value)
  if (!configured) throw new Error(`${name} must be set when auth is required`)
  return configured
}

function httpUrl(value: string, name: string): URL {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new Error(`${name} must be an absolute HTTP(S) URL`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${name} must be an absolute HTTP(S) URL`)
  }

  return url
}

function publicOrigin(value: string): string {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw invalidPublicUrl()
  }
  if (
    (url.protocol !== 'https:' && url.protocol !== 'http:') ||
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash ||
    isLocalhostSubdomain(url.hostname) ||
    (url.protocol === 'http:' && !isExplicitLoopbackUrl(url))
  ) {
    throw invalidPublicUrl()
  }

  return url.origin
}

function invalidPublicUrl(): Error {
  return new Error(
    'CORAL_WEB_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin without credentials, path, query, or fragment',
  )
}

// Validated here rather than left to `Set-Cookie`, because every way this value
// can be wrong fails silently and late. The name reaches serialization unchecked,
// the browser drops the cookie without telling anyone, and the first symptom is a
// callback that authenticated successfully and then landed on a page with no
// session — a login loop with nothing in any log to explain it. A configuration
// error belongs at boot.
//
// The configured value is the name's stem. Over HTTPS the session cookie is
// `__Host-<stem>` and the CSRF cookie `__Host-<stem>_csrf`, so that a hosted
// session stays bound to Reef's exact host (see `hostBoundCookieName`); over
// loopback HTTP both keep the bare stem.
function cookieNameEnv(value: string | undefined, publicUrl: string): string {
  const configured = optionalString(value) ?? DEFAULT_COOKIE_NAME

  if (!COOKIE_NAME_TOKEN.test(configured)) {
    throw new Error(
      "CORAL_WEB_SESSION_COOKIE_NAME must be an RFC 6265 cookie name: letters, digits, or !#$%&'*+-.^_`|~",
    )
  }
  // `__Host-` is an enforced prefix, not decoration: a browser discards a cookie
  // carrying it unless it was set with `Secure`. Coral web derives `Secure` from
  // CORAL_WEB_PUBLIC_URL, so over loopback HTTP such a name is accepted by the
  // grammar above and then silently discarded on every response.
  const secure = isSecureOrigin(publicUrl)
  if (HOST_PREFIX.test(configured) && !secure) {
    throw new Error(
      'CORAL_WEB_SESSION_COOKIE_NAME may use a __Host- prefix only when CORAL_WEB_PUBLIC_URL is HTTPS',
    )
  }

  return hostBoundCookieName(configured, secure)
}

function positiveIntegerEnv(value: string | undefined, name: string, fallback: number): number {
  const configured = optionalString(value)
  if (!configured) return fallback
  const parsed = Number.parseInt(configured, 10)
  if (!/^[0-9]+$/.test(configured) || !Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return parsed
}

function optionalString(value: string | undefined): string | null {
  return value?.trim() || null
}
