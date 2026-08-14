import { afterEach, describe, expect, it, vi } from 'vitest'

import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import {
  authClientId,
  authCookieSecure,
  authRedirectUri,
  authResource,
  coralUIAuthConfig,
  resolveAuthConfig,
} from './config.server'
import type { RequiredAuthConfig } from './types'

vi.mock('@/lib/coral-desktop', () => ({ isCoralDesktopBuild: vi.fn(() => false) }))

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

function requiredEnv(overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return {
    NODE_ENV: 'production',
    CORAL_UI_AUTH_ISSUER: 'https://coral.example.test',
    CORAL_UI_AUTH_MODE: 'required',
    CORAL_UI_PUBLIC_URL: 'https://coral-ui.example.test',
    CORAL_UI_SESSION_SECRET: SESSION_SECRET,
    ...overrides,
  }
}

function requiredConfig(overrides: NodeJS.ProcessEnv = {}): RequiredAuthConfig {
  const config = resolveAuthConfig({ env: requiredEnv(overrides), isDesktopBuild: false })
  expect(config.mode).toBe('required')
  if (config.mode !== 'required') throw new Error('expected required auth config')
  return config
}

describe('Coral UI auth config', () => {
  it('defaults local development and tests to disabled auth', () => {
    expect(resolveAuthConfig({ env: { NODE_ENV: 'development' }, isDesktopBuild: false })).toEqual({
      mode: 'disabled',
    })
    expect(resolveAuthConfig({ env: { NODE_ENV: 'test' }, isDesktopBuild: false })).toEqual({
      mode: 'disabled',
    })
  })

  it('keeps desktop builds disabled regardless of ambient hosted config', () => {
    expect(
      resolveAuthConfig({
        env: requiredEnv(),
        isDesktopBuild: true,
      }),
    ).toEqual({ mode: 'disabled' })
  })

  it('fails closed when a non-desktop production server has no explicit mode', () => {
    expect(() =>
      resolveAuthConfig({ env: { NODE_ENV: 'production' }, isDesktopBuild: false }),
    ).toThrow('CORAL_UI_AUTH_MODE must be set to disabled or required in production')
  })

  it('allows production to explicitly disable auth', () => {
    expect(
      resolveAuthConfig({
        env: { NODE_ENV: 'production', CORAL_UI_AUTH_MODE: 'disabled' },
        isDesktopBuild: false,
      }),
    ).toEqual({ mode: 'disabled' })
  })

  it('derives every OAuth identifier and cookie security from the canonical public URL', () => {
    const config = requiredConfig({
      CORAL_UI_AUTH_CLIENT_ID: 'ignored-client-id',
      CORAL_UI_AUTH_ISSUER: 'https://coral.example.test:443/tenant/',
      CORAL_UI_AUTH_REDIRECT_URI: 'https://ignored.example.test/callback',
      CORAL_UI_AUTH_RESOURCE: 'https://ignored.example.test',
      CORAL_UI_AUTH_SCOPE: 'ignored:scope',
      CORAL_UI_COOKIE_SECURE: 'false',
      CORAL_UI_MCP_URL: 'https://ignored.example.test/mcp',
      CORAL_UI_PUBLIC_URL: 'https://CORAL-UI.Example.test:443/',
      CORAL_UI_SESSION_COOKIE_NAME: 'custom_session',
      CORAL_UI_SESSION_MAX_AGE_SECONDS: '1800',
    })

    expect(config).toEqual({
      cookieName: 'custom_session',
      issuer: 'https://coral.example.test:443/tenant/',
      mode: 'required',
      publicUrl: 'https://coral-ui.example.test',
      sessionMaxAgeSeconds: 1800,
      sessionSecret: SESSION_SECRET,
    })
    expect(authResource(config)).toBe('https://coral-ui.example.test')
    expect(authClientId(config)).toBe('https://coral-ui.example.test/.well-known/oauth-client')
    expect(authRedirectUri(config)).toBe('https://coral-ui.example.test/auth/callback')
    expect(authCookieSecure(config)).toBe(true)
  })

  it('allows plain HTTP only when Coral UI and Coral are both explicit loopback URLs', () => {
    const config = requiredConfig({
      CORAL_UI_AUTH_ISSUER: 'http://127.42.0.1:3000',
      CORAL_UI_PUBLIC_URL: 'http://[::1]:5173',
    })

    expect(config.publicUrl).toBe('http://[::1]:5173')
    expect(authResource(config)).toBe('http://[::1]:5173')
    expect(authClientId(config)).toBe('http://[::1]:5173/.well-known/oauth-client')
    expect(authRedirectUri(config)).toBe('http://[::1]:5173/auth/callback')
    expect(authCookieSecure(config)).toBe(false)
  })

  it.each([
    ['http://localhost:5173', 'http://localhost:3000'],
    ['http://localhost.:5173', 'http://localhost.:3000'],
    ['http://127.255.1.2:5173', 'http://127.0.0.1:3000'],
    ['http://[::1]:5173', 'http://[::1]:3000'],
    ['http://[::ffff:127.0.0.1]:5173', 'http://[::ffff:127.0.0.2]:3000'],
  ])('accepts explicit loopback pair %s and %s', (publicUrl, issuer) => {
    expect(() =>
      requiredConfig({ CORAL_UI_AUTH_ISSUER: issuer, CORAL_UI_PUBLIC_URL: publicUrl }),
    ).not.toThrow()
  })

  it('rejects an HTTP Coral UI URL when the Coral issuer is not also loopback HTTP', () => {
    expect(() => requiredConfig({ CORAL_UI_PUBLIC_URL: 'http://127.0.0.1:5173' })).toThrow(
      'CORAL_UI_PUBLIC_URL may use HTTP only when CORAL_UI_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  })

  it.each([
    'http://192.168.1.10:5173',
    'http://10.0.0.10:5173',
    'http://169.254.1.10:5173',
    'http://coral-ui.internal:5173',
    'http://preview.localhost:5173',
  ])('rejects a non-loopback HTTP public URL: %s', (publicUrl) => {
    expect(() => requiredConfig({ CORAL_UI_PUBLIC_URL: publicUrl })).toThrow(
      'CORAL_UI_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'https://user:password@coral-ui.example.test',
    'https://coral-ui.example.test/a/path',
    'https://coral-ui.example.test?tenant=coral',
    'https://coral-ui.example.test#fragment',
    'ftp://coral-ui.example.test',
  ])('rejects a public URL that is not a clean HTTP(S) origin: %s', (publicUrl) => {
    expect(() => requiredConfig({ CORAL_UI_PUBLIC_URL: publicUrl })).toThrow(
      'CORAL_UI_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'http://192.168.1.10:3000',
    'http://10.0.0.10:3000',
    'http://169.254.1.10:3000',
    'http://coral.internal:3000',
    'http://preview.localhost:3000',
  ])('rejects a non-loopback HTTP Coral issuer: %s', (issuer) => {
    expect(() => requiredConfig({ CORAL_UI_AUTH_ISSUER: issuer })).toThrow(
      'CORAL_UI_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP',
    )
  })

  it.each([
    ['sometimes', 'CORAL_UI_AUTH_MODE must be set to disabled or required'],
    ['', 'CORAL_UI_AUTH_MODE must be set to disabled or required'],
  ])('rejects invalid auth mode %s', (mode, message) => {
    expect(() =>
      resolveAuthConfig({
        env: requiredEnv({ CORAL_UI_AUTH_MODE: mode }),
        isDesktopBuild: false,
      }),
    ).toThrow(message)
  })

  it.each([
    [
      { CORAL_UI_SESSION_SECRET: 'too-short' },
      'CORAL_UI_SESSION_SECRET must be at least 32 characters',
    ],
    [{ CORAL_UI_AUTH_ISSUER: '' }, 'CORAL_UI_AUTH_ISSUER must be set'],
    [{ CORAL_UI_PUBLIC_URL: '' }, 'CORAL_UI_PUBLIC_URL must be set'],
    [
      { CORAL_UI_AUTH_ISSUER: 'file:///tmp/issuer' },
      'CORAL_UI_AUTH_ISSUER must be an absolute HTTP(S) URL',
    ],
    [
      { CORAL_UI_AUTH_ISSUER: 'https://user:password@coral.example.test' },
      'CORAL_UI_AUTH_ISSUER must not include credentials',
    ],
    [
      { CORAL_UI_AUTH_ISSUER: 'https://coral.example.test/tenant?region=us' },
      'CORAL_UI_AUTH_ISSUER must not include a query string or fragment',
    ],
    [
      { CORAL_UI_SESSION_MAX_AGE_SECONDS: '0' },
      'CORAL_UI_SESSION_MAX_AGE_SECONDS must be a positive integer',
    ],
    // Each of these serializes into `Set-Cookie` without complaint and is then
    // dropped by the browser, so the only symptom is a session that never
    // persists. A separator, whitespace, and a control character cover the three
    // ways a name leaves the token grammar.
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui session' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui=session' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui;session' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui\tsession' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui,session' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { CORAL_UI_SESSION_COOKIE_NAME: 'coral-ui"session"' },
      'CORAL_UI_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
  ])('rejects invalid required config %#', (overrides, message) => {
    expect(() => requiredConfig(overrides)).toThrow(message)
  })

  it('accepts every character the cookie-name grammar allows', () => {
    const name = "coral_ui_session-0.9!#$%&'*+^`|~"
    expect(requiredConfig({ CORAL_UI_SESSION_COOKIE_NAME: name }).cookieName).toBe(name)
  })

  // A `__Host-`/`__Secure-` name is well-formed but browser-enforced: without
  // `Secure` the cookie is discarded, and Coral UI derives `Secure` from the public
  // URL rather than from a switch. So the same name is correct on HTTPS and
  // unusable on loopback HTTP, and only the config knows which it is.
  it.each(['__Host-coral_ui_session', '__Secure-coral_ui_session'])(
    'accepts %s over HTTPS and rejects it over loopback HTTP',
    (name) => {
      expect(requiredConfig({ CORAL_UI_SESSION_COOKIE_NAME: name }).cookieName).toBe(name)

      expect(() =>
        resolveAuthConfig({
          env: requiredEnv({
            CORAL_UI_AUTH_ISSUER: 'http://127.0.0.1:9080',
            CORAL_UI_PUBLIC_URL: 'http://127.0.0.1:5173',
            CORAL_UI_SESSION_COOKIE_NAME: name,
          }),
          isDesktopBuild: false,
        }),
      ).toThrow('CORAL_UI_SESSION_COOKIE_NAME may use a __Host- or __Secure- prefix only')
    },
  )
})

// Every case above hands `resolveAuthConfig` an explicit `isDesktopBuild`, which
// is exactly the wiring that let a Desktop branch reading a variable nothing
// sets pass for as long as it did. These exercise `coralUIAuthConfig` itself.
//
// Both cases run under the same hosted environment on purpose. Without it the
// pair is vacuous: with no CORAL_UI_AUTH_MODE, `resolveAuthConfig` returns disabled
// down either branch, so a permanently-false marker would still look correct.
describe('coralUIAuthConfig', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(false)
  })

  // Derived from the same fixture the rest of the file uses. Spelling the keys
  // out again would be a second definition of "a hosted environment", free to
  // drift from the first the moment either gains a variable.
  function stubHostedEnv(): void {
    for (const [key, value] of Object.entries(requiredEnv())) {
      if (value !== undefined) vi.stubEnv(key, value)
    }
  }

  it('disables auth for a Desktop build even under a hosted environment', () => {
    stubHostedEnv()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(true)

    expect(coralUIAuthConfig()).toEqual({ mode: 'disabled' })
  })

  it('keeps that same environment authenticated when the build is not Desktop', () => {
    stubHostedEnv()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(false)

    expect(coralUIAuthConfig().mode).toBe('required')
  })
})
