import { readFileSync } from 'node:fs'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import {
  authClientId,
  authCookieSecure,
  authRedirectUri,
  authResource,
  reefAuthConfig,
  resolveAuthConfig,
} from './config.server'
import type { RequiredAuthConfig } from './types'

vi.mock('@/lib/coral-desktop', () => ({ isCoralDesktopBuild: vi.fn(() => false) }))

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

function requiredEnv(overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return {
    NODE_ENV: 'production',
    REEF_AUTH_ISSUER: 'https://coral.example.test',
    REEF_AUTH_MODE: 'required',
    REEF_PUBLIC_URL: 'https://reef.example.test',
    REEF_SESSION_SECRET: SESSION_SECRET,
    ...overrides,
  }
}

function requiredConfig(overrides: NodeJS.ProcessEnv = {}): RequiredAuthConfig {
  const config = resolveAuthConfig({ env: requiredEnv(overrides), isDesktopBuild: false })
  expect(config.mode).toBe('required')
  if (config.mode !== 'required') throw new Error('expected required auth config')
  return config
}

describe('Reef auth config', () => {
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
    ).toThrow('REEF_AUTH_MODE must be set to disabled or required in production')
  })

  it('allows production to explicitly disable auth', () => {
    expect(
      resolveAuthConfig({
        env: { NODE_ENV: 'production', REEF_AUTH_MODE: 'disabled' },
        isDesktopBuild: false,
      }),
    ).toEqual({ mode: 'disabled' })
  })

  it('derives every OAuth identifier and cookie security from the canonical public URL', () => {
    const config = requiredConfig({
      REEF_AUTH_CLIENT_ID: 'ignored-client-id',
      REEF_AUTH_ISSUER: 'https://coral.example.test:443/tenant/',
      REEF_AUTH_REDIRECT_URI: 'https://ignored.example.test/callback',
      REEF_AUTH_RESOURCE: 'https://ignored.example.test',
      REEF_AUTH_SCOPE: 'ignored:scope',
      REEF_COOKIE_SECURE: 'false',
      REEF_MCP_URL: 'https://ignored.example.test/mcp',
      REEF_PUBLIC_URL: 'https://REEF.Example.test:443/',
      REEF_SESSION_COOKIE_NAME: 'custom_session',
      REEF_SESSION_MAX_AGE_SECONDS: '1800',
    })

    expect(config).toEqual({
      cookieName: 'custom_session',
      issuer: 'https://coral.example.test:443/tenant/',
      mode: 'required',
      publicUrl: 'https://reef.example.test',
      sessionMaxAgeSeconds: 1800,
      sessionSecret: SESSION_SECRET,
    })
    expect(authResource(config)).toBe('https://reef.example.test')
    expect(authClientId(config)).toBe('https://reef.example.test/.well-known/oauth-client')
    expect(authRedirectUri(config)).toBe('https://reef.example.test/auth/callback')
    expect(authCookieSecure(config)).toBe(true)
  })

  it('allows plain HTTP only when Reef and Coral are both explicit loopback URLs', () => {
    const config = requiredConfig({
      REEF_AUTH_ISSUER: 'http://127.42.0.1:3000',
      REEF_PUBLIC_URL: 'http://[::1]:5173',
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
      requiredConfig({ REEF_AUTH_ISSUER: issuer, REEF_PUBLIC_URL: publicUrl }),
    ).not.toThrow()
  })

  it('rejects an HTTP Reef URL when the Coral issuer is not also loopback HTTP', () => {
    expect(() => requiredConfig({ REEF_PUBLIC_URL: 'http://127.0.0.1:5173' })).toThrow(
      'REEF_PUBLIC_URL may use HTTP only when REEF_AUTH_ISSUER also uses explicit-loopback HTTP',
    )
  })

  it.each([
    'http://192.168.1.10:5173',
    'http://10.0.0.10:5173',
    'http://169.254.1.10:5173',
    'http://reef.internal:5173',
    'http://preview.localhost:5173',
  ])('rejects a non-loopback HTTP public URL: %s', (publicUrl) => {
    expect(() => requiredConfig({ REEF_PUBLIC_URL: publicUrl })).toThrow(
      'REEF_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'https://user:password@reef.example.test',
    'https://reef.example.test/a/path',
    'https://reef.example.test?tenant=coral',
    'https://reef.example.test#fragment',
    'ftp://reef.example.test',
  ])('rejects a public URL that is not a clean HTTP(S) origin: %s', (publicUrl) => {
    expect(() => requiredConfig({ REEF_PUBLIC_URL: publicUrl })).toThrow(
      'REEF_PUBLIC_URL must be an HTTPS or explicit-loopback HTTP origin',
    )
  })

  it.each([
    'http://192.168.1.10:3000',
    'http://10.0.0.10:3000',
    'http://169.254.1.10:3000',
    'http://coral.internal:3000',
    'http://preview.localhost:3000',
  ])('rejects a non-loopback HTTP Coral issuer: %s', (issuer) => {
    expect(() => requiredConfig({ REEF_AUTH_ISSUER: issuer })).toThrow(
      'REEF_AUTH_ISSUER must use HTTPS or explicit-loopback HTTP',
    )
  })

  it.each([
    ['sometimes', 'REEF_AUTH_MODE must be set to disabled or required'],
    ['', 'REEF_AUTH_MODE must be set to disabled or required'],
  ])('rejects invalid auth mode %s', (mode, message) => {
    expect(() =>
      resolveAuthConfig({
        env: requiredEnv({ REEF_AUTH_MODE: mode }),
        isDesktopBuild: false,
      }),
    ).toThrow(message)
  })

  it.each([
    [{ REEF_SESSION_SECRET: 'too-short' }, 'REEF_SESSION_SECRET must be at least 32 characters'],
    [{ REEF_AUTH_ISSUER: '' }, 'REEF_AUTH_ISSUER must be set'],
    [{ REEF_PUBLIC_URL: '' }, 'REEF_PUBLIC_URL must be set'],
    [
      { REEF_AUTH_ISSUER: 'file:///tmp/issuer' },
      'REEF_AUTH_ISSUER must be an absolute HTTP(S) URL',
    ],
    [
      { REEF_AUTH_ISSUER: 'https://user:password@coral.example.test' },
      'REEF_AUTH_ISSUER must not include credentials',
    ],
    [
      { REEF_AUTH_ISSUER: 'https://coral.example.test/tenant?region=us' },
      'REEF_AUTH_ISSUER must not include a query string or fragment',
    ],
    [
      { REEF_SESSION_MAX_AGE_SECONDS: '0' },
      'REEF_SESSION_MAX_AGE_SECONDS must be a positive integer',
    ],
    // Each of these serializes into `Set-Cookie` without complaint and is then
    // dropped by the browser, so the only symptom is a session that never
    // persists. A separator, whitespace, and a control character cover the three
    // ways a name leaves the token grammar.
    [
      { REEF_SESSION_COOKIE_NAME: 'reef session' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { REEF_SESSION_COOKIE_NAME: 'reef=session' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { REEF_SESSION_COOKIE_NAME: 'reef;session' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { REEF_SESSION_COOKIE_NAME: 'reef\tsession' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { REEF_SESSION_COOKIE_NAME: 'reef,session' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
    [
      { REEF_SESSION_COOKIE_NAME: 'reef"session"' },
      'REEF_SESSION_COOKIE_NAME must be an RFC 6265 cookie name',
    ],
  ])('rejects invalid required config %#', (overrides, message) => {
    expect(() => requiredConfig(overrides)).toThrow(message)
  })

  it('accepts every character the cookie-name grammar allows', () => {
    const name = "reef_session-0.9!#$%&'*+^`|~"
    expect(requiredConfig({ REEF_SESSION_COOKIE_NAME: name }).cookieName).toBe(name)
  })

  // A `__Host-`/`__Secure-` name is well-formed but browser-enforced: without
  // `Secure` the cookie is discarded, and Reef derives `Secure` from the public
  // URL rather than from a switch. So the same name is correct on HTTPS and
  // unusable on loopback HTTP, and only the config knows which it is.
  it.each(['__Host-reef_session', '__Secure-reef_session'])(
    'accepts %s over HTTPS and rejects it over loopback HTTP',
    (name) => {
      expect(requiredConfig({ REEF_SESSION_COOKIE_NAME: name }).cookieName).toBe(name)

      expect(() =>
        resolveAuthConfig({
          env: requiredEnv({
            REEF_AUTH_ISSUER: 'http://127.0.0.1:9080',
            REEF_PUBLIC_URL: 'http://127.0.0.1:5173',
            REEF_SESSION_COOKIE_NAME: name,
          }),
          isDesktopBuild: false,
        }),
      ).toThrow('REEF_SESSION_COOKIE_NAME may use a __Host- or __Secure- prefix only')
    },
  )

  it('keeps redundant auth settings out of the documented environment contract', () => {
    const example = readFileSync(new URL('../../.env.example', import.meta.url), 'utf8')
    for (const removedName of [
      'REEF_AUTH_CLIENT_ID',
      'REEF_AUTH_REDIRECT_URI',
      'REEF_AUTH_RESOURCE',
      'REEF_AUTH_SCOPE',
      'REEF_COOKIE_SECURE',
      'REEF_MCP_URL',
    ]) {
      expect(example).not.toContain(removedName)
    }
  })
})

// Every case above hands `resolveAuthConfig` an explicit `isDesktopBuild`, which
// is exactly the wiring that let a Desktop branch reading a variable nothing
// sets pass for as long as it did. These exercise `reefAuthConfig` itself.
//
// Both cases run under the same hosted environment on purpose. Without it the
// pair is vacuous: with no REEF_AUTH_MODE, `resolveAuthConfig` returns disabled
// down either branch, so a permanently-false marker would still look correct.
describe('reefAuthConfig', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(false)
  })

  function stubHostedEnv(): void {
    vi.stubEnv('REEF_AUTH_ISSUER', 'https://coral.example.test')
    vi.stubEnv('REEF_AUTH_MODE', 'required')
    vi.stubEnv('REEF_PUBLIC_URL', 'https://reef.example.test')
    vi.stubEnv('REEF_SESSION_SECRET', SESSION_SECRET)
  }

  it('disables auth for a Desktop build even under a hosted environment', () => {
    stubHostedEnv()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(true)

    expect(reefAuthConfig()).toEqual({ mode: 'disabled' })
  })

  it('keeps that same environment authenticated when the build is not Desktop', () => {
    stubHostedEnv()
    vi.mocked(isCoralDesktopBuild).mockReturnValue(false)

    expect(reefAuthConfig().mode).toBe('required')
  })
})
