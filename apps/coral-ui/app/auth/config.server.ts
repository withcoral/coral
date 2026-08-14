import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { isExplicitLoopbackUrl, isLocalhostSubdomain } from '@/lib/loopback.server'
import type { AuthConfig, RequiredAuthConfig } from './types'

const DEFAULT_COOKIE_NAME = 'coral_ui_session'
const DEFAULT_SESSION_MAX_AGE_SECONDS = 60 * 60

// RFC 6265 gives a cookie name the RFC 9110 `token` grammar: letters, digits,
// and `!#$%&'*+-.^_`|~`. Serialization does not enforce it — a name outside the
// grammar is written into `Set-Cookie` faithfully and rejected by the browser,
// so the only symptom is a session that never persists.
const COOKIE_NAME_TOKEN = /^[\w!#$%&'*+.^`|~-]+$/

interface AuthConfigInput {
  env: NodeJS.ProcessEnv
  isDesktopBuild: boolean
}

export function coralUIAuthConfig(): AuthConfig {
  return resolveAuthConfig({
    env: process.env,
    isDesktopBuild: isCoralDesktopBuild(),
  })
}

export function resolveAuthConfig({ env, isDesktopBuild }: AuthConfigInput): AuthConfig {
  if (isDesktopBuild) return { mode: 'disabled' }

  const configuredMode = env.CORAL_UI_AUTH_MODE?.trim().toLowerCase()
  if (!configuredMode) {
    if (env.NODE_ENV === 'production') {
      throw new Error('CORAL_UI_AUTH_MODE must be set to disabled or required in production')
    }

    return { mode: 'disabled' }
  }
  if (configuredMode === 'disabled') return { mode: 'disabled' }
  if (configuredMode !== 'required') {
    throw new Error('CORAL_UI_AUTH_MODE must be set to disabled or required')
  }

  return requiredAuthConfig(env)
}

function requiredAuthConfig(env: NodeJS.ProcessEnv): RequiredAuthConfig {
  const sessionSecret = env.CORAL_UI_SESSION_SECRET?.trim()
  if (!sessionSecret || sessionSecret.length < 32) {
    throw new Error('CORAL_UI_SESSION_SECRET must be at least 32 characters when auth is required')
  }

  const issuer = requiredString(env.CORAL_UI_AUTH_ISSUER, 'CORAL_UI_AUTH_ISSUER')
  const issuerUrl = httpUrl(issuer, 'CORAL_UI_AUTH_ISSUER')
  if (issuerUrl.username || issuerUrl.password) {
    throw new Error('CORAL_UI_AUTH_ISSUER must not include credentials')
  }
  if (issuerUrl.search || issuerUrl.hash) {
    throw new Error('CORAL_UI_AUTH_ISSUER must not include a query string or fragment')
  }
  if (issuerUrl.protocol === 'http:' && !isExplicitLoopbackUrl(issuerUrl)) {
    throw new Error('CORAL_UI_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP')
  }

  const publicUrl = publicOrigin(requiredString(env.CORAL_UI_PUBLIC_URL, 'CORAL_UI_PUBLIC_URL'))
  if (new URL(publicUrl).protocol === 'http:' && issuerUrl.protocol !== 'http:') {
    throw new Error(
      'CORAL_UI_PUBLIC_URL may use HTTP only when CORAL_UI_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  }

  return {
    cookieName: cookieNameEnv(env.CORAL_UI_SESSION_COOKIE_NAME, publicUrl),
    issuer,
    mode: 'required',
    publicUrl,
    sessionMaxAgeSeconds: positiveIntegerEnv(
      env.CORAL_UI_SESSION_MAX_AGE_SECONDS,
      'CORAL_UI_SESSION_MAX_AGE_SECONDS',
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

export function authCookieSecure(config: RequiredAuthConfig): boolean {
  return isSecureOrigin(config.publicUrl)
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
    'CORAL_UI_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin without credentials, path, query, or fragment',
  )
}

// Validated here rather than left to `Set-Cookie`, because every way this value
// can be wrong fails silently and late. The name reaches serialization unchecked,
// the browser drops the cookie without telling anyone, and the first symptom is a
// callback that authenticated successfully and then landed on a page with no
// session — a login loop with nothing in any log to explain it. A configuration
// error belongs at boot.
function cookieNameEnv(value: string | undefined, publicUrl: string): string {
  const configured = optionalString(value)
  if (!configured) return DEFAULT_COOKIE_NAME

  if (!COOKIE_NAME_TOKEN.test(configured)) {
    throw new Error(
      "CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name: letters, digits, or !#$%&'*+-.^_`|~",
    )
  }
  // `__Host-` and `__Secure-` are enforced prefixes, not decoration: a browser
  // discards a cookie carrying either unless it was set with `Secure`. Coral UI
  // derives `Secure` from CORAL_UI_PUBLIC_URL, so over loopback HTTP such a name is
  // accepted by the grammar above and then silently discarded on every response.
  if (/^__(?:Host|Secure)-/.test(configured) && !isSecureOrigin(publicUrl)) {
    throw new Error(
      'CORAL_UI_SESSION_COOKIE_NAME may use a __Host- or __Secure- prefix only when CORAL_UI_PUBLIC_URL is HTTPS',
    )
  }

  return configured
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
