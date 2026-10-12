import { afterEach, describe, expect, it, vi } from 'vitest'

import { hostedRuntimeConfig, stubHostedEnv } from '@/auth/test-config'

import { bootstrapRuntimeConfig, readCoralWebRuntimeConfig } from './runtime-config.server'

describe('Coral web startup configuration', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('reads complete production auth and Coral configuration', () => {
    stubHostedEnv({ CORAL_UPSTREAM_OAUTH_CALLBACK_URL: undefined })

    expect(readCoralWebRuntimeConfig()).toEqual(hostedRuntimeConfig)
  })

  // Request code receives this value; a later environment change must not reach it.
  it('keeps the first bootstrap read for the life of the process', () => {
    stubHostedEnv()
    const first = bootstrapRuntimeConfig()
    stubHostedEnv({ CORAL_ENDPOINT: 'https://other.example.test' })

    expect(bootstrapRuntimeConfig()).toBe(first)
  })

  it.each([
    [
      'CORAL_WEB_AUTH_MODE',
      '',
      'CORAL_WEB_AUTH_MODE must be set to disabled or required in production',
    ],
    ['CORAL_WEB_PUBLIC_URL', '', 'CORAL_WEB_PUBLIC_URL must be set when auth is required'],
    [
      'CORAL_WEB_SESSION_SECRET',
      '',
      'CORAL_WEB_SESSION_SECRET must be at least 32 characters when auth is required',
    ],
    ['CORAL_ENDPOINT', '', 'CORAL_ENDPOINT must be set when Coral authentication is enabled'],
  ])('fails deterministically for invalid %s', (name, value, message) => {
    stubHostedEnv({ [name]: value })

    expect(() => readCoralWebRuntimeConfig()).toThrow(message)
  })

  it('allows disabled auth without parsing the insecure endpoint flag', () => {
    stubHostedEnv({
      CORAL_ENDPOINT: 'http://coral.internal:14555',
      CORAL_WEB_ALLOW_INSECURE_CORAL_ENDPOINT: 'not-a-boolean',
      CORAL_WEB_AUTH_MODE: 'disabled',
    })

    expect(() => readCoralWebRuntimeConfig()).not.toThrow()
  })
})
