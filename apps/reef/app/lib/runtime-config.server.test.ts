import { afterEach, describe, expect, it, vi } from 'vitest'

import { assertReefRuntimeConfig } from './runtime-config.server'

const SESSION_SECRET = '0123456789abcdef0123456789abcdef'

function stubRequiredEnv(overrides: NodeJS.ProcessEnv = {}): void {
  const env = {
    CORAL_ENDPOINT: 'https://coral.example.test',
    NODE_ENV: 'production',
    REEF_AUTH_ISSUER: 'https://auth.example.test',
    REEF_AUTH_MODE: 'required',
    REEF_PUBLIC_URL: 'https://reef.example.test',
    REEF_SESSION_SECRET: SESSION_SECRET,
    ...overrides,
  }
  for (const [name, value] of Object.entries(env)) vi.stubEnv(name, value)
}

describe('Reef startup configuration', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('accepts complete production auth and Coral configuration', () => {
    stubRequiredEnv()

    expect(() => assertReefRuntimeConfig()).not.toThrow()
  })

  it.each([
    ['REEF_AUTH_MODE', '', 'REEF_AUTH_MODE must be set to disabled or required in production'],
    ['REEF_PUBLIC_URL', '', 'REEF_PUBLIC_URL must be set when auth is required'],
    [
      'REEF_SESSION_SECRET',
      '',
      'REEF_SESSION_SECRET must be at least 32 characters when auth is required',
    ],
    ['CORAL_ENDPOINT', '', 'CORAL_ENDPOINT must be set when Coral authentication is enabled'],
  ])('fails deterministically for invalid %s', (name, value, message) => {
    stubRequiredEnv({ [name]: value })

    expect(() => assertReefRuntimeConfig()).toThrow(message)
  })

  it('uses the authenticated endpoint policy for insecure Coral URLs', () => {
    stubRequiredEnv({
      CORAL_ENDPOINT: 'http://coral.internal:14555',
      REEF_ALLOW_INSECURE_CORAL_ENDPOINT: 'false',
    })

    expect(() => assertReefRuntimeConfig()).toThrow(
      'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
    )
  })

  it('allows disabled auth without parsing the insecure endpoint flag', () => {
    stubRequiredEnv({
      CORAL_ENDPOINT: 'http://coral.internal:14555',
      REEF_ALLOW_INSECURE_CORAL_ENDPOINT: 'not-a-boolean',
      REEF_AUTH_MODE: 'disabled',
    })

    expect(() => assertReefRuntimeConfig()).not.toThrow()
  })
})
