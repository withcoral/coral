import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { coralEndpointForRequest } from './source-service.server'

describe('coralEndpointForRequest', () => {
  const originalEndpoint = process.env.CORAL_ENDPOINT
  const originalNodeEnv = process.env.NODE_ENV

  beforeEach(() => {
    delete process.env.CORAL_ENDPOINT
    process.env.NODE_ENV = 'test'
  })

  afterEach(() => {
    restore('CORAL_ENDPOINT', originalEndpoint)
    restore('NODE_ENV', originalNodeEnv)
  })

  it('uses the configured endpoint and trims a trailing slash', () => {
    process.env.CORAL_ENDPOINT = 'https://api.coral.test/'

    expect(coralEndpointForRequest(new Request('https://app.coral.test/sources'))).toBe(
      'https://api.coral.test',
    )
  })

  it('uses the default dev endpoint for localhost requests outside production', () => {
    expect(coralEndpointForRequest(new Request('http://localhost:3000/sources'))).toBe(
      'http://127.0.0.1:1457',
    )
  })

  it('does not derive the endpoint from the request origin in production', () => {
    process.env.NODE_ENV = 'production'

    expect(() => coralEndpointForRequest(new Request('https://attacker.example/sources'))).toThrow(
      /CORAL_ENDPOINT/,
    )
  })
})

function restore(key: 'CORAL_ENDPOINT' | 'NODE_ENV', value: string | undefined): void {
  if (value === undefined) {
    delete process.env[key]
  } else {
    process.env[key] = value
  }
}
