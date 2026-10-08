import { afterEach, describe, expect, it, vi } from 'vitest'

import { assertCoralUIRuntimeConfig } from './runtime-config.server'

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

function stubRequiredEnv(overrides: NodeJS.ProcessEnv = {}): void {
  const env = {
    CORAL_ENDPOINT: 'https://coral.example.test',
    NODE_ENV: 'production',
    CORAL_UI_AUTH_ISSUER: 'https://auth.example.test',
    CORAL_UI_AUTH_MODE: 'required',
    CORAL_UI_PUBLIC_URL: 'https://coral-ui.example.test',
    CORAL_UI_SESSION_SECRET: SESSION_SECRET,
    ...overrides,
  }
  for (const [name, value] of Object.entries(env)) vi.stubEnv(name, value)
}

describe('Coral UI startup configuration', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('accepts complete production auth and Coral configuration', () => {
    stubRequiredEnv()

    expect(() => assertCoralUIRuntimeConfig()).not.toThrow()
  })

  it.each([
    [
      'CORAL_UI_AUTH_MODE',
      '',
      'CORAL_UI_AUTH_MODE must be set to disabled or required in production',
    ],
    ['CORAL_UI_PUBLIC_URL', '', 'CORAL_UI_PUBLIC_URL must be set when auth is required'],
    [
      'CORAL_UI_SESSION_SECRET',
      '',
      'CORAL_UI_SESSION_SECRET must be at least 32 characters when auth is required',
    ],
    ['CORAL_ENDPOINT', '', 'CORAL_ENDPOINT must be set when Coral authentication is enabled'],
  ])('fails deterministically for invalid %s', (name, value, message) => {
    stubRequiredEnv({ [name]: value })

    expect(() => assertCoralUIRuntimeConfig()).toThrow(message)
  })

  it('uses the authenticated endpoint policy for insecure Coral URLs', () => {
    stubRequiredEnv({
      CORAL_ENDPOINT: 'http://coral.internal:14555',
      CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT: 'false',
    })

    expect(() => assertCoralUIRuntimeConfig()).toThrow(
      'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
    )
  })

  it('allows disabled auth without parsing the insecure endpoint flag', () => {
    stubRequiredEnv({
      CORAL_ENDPOINT: 'http://coral.internal:14555',
      CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT: 'not-a-boolean',
      CORAL_UI_AUTH_MODE: 'disabled',
    })

    expect(() => assertCoralUIRuntimeConfig()).not.toThrow()
  })
})
