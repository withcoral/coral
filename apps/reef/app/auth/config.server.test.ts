import { describe, expect, it } from 'vitest'

import { resolveAuthConfig } from './config.server'

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

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
        env: {
          NODE_ENV: 'production',
          REEF_AUTH_ISSUER: 'https://login.example.test',
          REEF_AUTH_MODE: 'required',
          REEF_SESSION_SECRET: SESSION_SECRET,
        },
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

  it('validates required auth config without changing exact OAuth identifiers', () => {
    expect(
      resolveAuthConfig({
        env: {
          NODE_ENV: 'production',
          REEF_AUTH_CLIENT_ID: 'coral-cloud-ui',
          REEF_AUTH_ISSUER: 'https://login.example.test:443/tenant/',
          REEF_AUTH_MODE: 'required',
          REEF_AUTH_REDIRECT_URI: 'https://reef.example.test:443/auth/callback?tenant=coral',
          REEF_AUTH_SCOPE: 'coral:mcp',
          REEF_SESSION_COOKIE_NAME: 'custom_session',
          REEF_SESSION_MAX_AGE_SECONDS: '1800',
          REEF_SESSION_SECRET: SESSION_SECRET,
        },
        isDesktopBuild: false,
      }),
    ).toEqual({
      clientId: 'coral-cloud-ui',
      cookieName: 'custom_session',
      cookieSecure: true,
      issuer: 'https://login.example.test:443/tenant/',
      mode: 'required',
      redirectUri: 'https://reef.example.test:443/auth/callback?tenant=coral',
      scope: 'coral:mcp',
      sessionMaxAgeSeconds: 1800,
      sessionSecret: SESSION_SECRET,
    })
  })

  it('rejects insecure hosted production cookies', () => {
    expect(() =>
      resolveAuthConfig({
        env: {
          NODE_ENV: 'production',
          REEF_AUTH_ISSUER: 'https://login.example.test',
          REEF_AUTH_MODE: 'required',
          REEF_AUTH_REDIRECT_URI: 'https://reef.example.test/auth/callback',
          REEF_COOKIE_SECURE: 'false',
          REEF_SESSION_SECRET: SESSION_SECRET,
        },
        isDesktopBuild: false,
      }),
    ).toThrow('REEF_COOKIE_SECURE cannot be false in production')
  })

  it.each([
    [{ REEF_AUTH_MODE: 'sometimes' }, 'REEF_AUTH_MODE must be set to disabled or required'],
    [
      { REEF_AUTH_ISSUER: 'https://login.example.test', REEF_AUTH_MODE: 'required' },
      'REEF_SESSION_SECRET must be at least 32 characters',
    ],
    [
      { REEF_AUTH_MODE: 'required', REEF_SESSION_SECRET: SESSION_SECRET },
      'REEF_AUTH_ISSUER must be set',
    ],
    [
      {
        REEF_AUTH_ISSUER: 'file:///tmp/issuer',
        REEF_AUTH_MODE: 'required',
        REEF_SESSION_SECRET: SESSION_SECRET,
      },
      'REEF_AUTH_ISSUER must be an absolute HTTP(S) URL',
    ],
    [
      {
        REEF_AUTH_ISSUER: 'https://login.example.test/tenant?region=us',
        REEF_AUTH_MODE: 'required',
        REEF_SESSION_SECRET: SESSION_SECRET,
      },
      'REEF_AUTH_ISSUER must not include a query string or fragment',
    ],
    [
      {
        REEF_AUTH_ISSUER: 'https://login.example.test',
        REEF_AUTH_MODE: 'required',
        REEF_AUTH_REDIRECT_URI: 'https://reef.example.test/auth/callback#done',
        REEF_SESSION_SECRET: SESSION_SECRET,
      },
      'REEF_AUTH_REDIRECT_URI must not include a fragment',
    ],
    [
      {
        REEF_AUTH_ISSUER: 'https://login.example.test',
        REEF_AUTH_MODE: 'required',
        REEF_SESSION_MAX_AGE_SECONDS: '0',
        REEF_SESSION_SECRET: SESSION_SECRET,
      },
      'REEF_SESSION_MAX_AGE_SECONDS must be a positive integer',
    ],
  ])('rejects invalid config %#', (env, message) => {
    expect(() => resolveAuthConfig({ env, isDesktopBuild: false })).toThrow(message)
  })
})
