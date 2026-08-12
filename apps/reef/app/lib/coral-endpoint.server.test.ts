import { describe, expect, it } from 'vitest'

import { DEFAULT_DEV_CORAL_ENDPOINT } from './constants'
import { resolveCoralEndpoint } from './coral-endpoint.server'

const request = new Request('https://attacker.example.test/a/path')

function resolve(
  env: NodeJS.ProcessEnv,
  authenticated = true,
): ReturnType<typeof resolveCoralEndpoint> {
  return resolveCoralEndpoint({ authenticated, env, request })
}

describe('Coral endpoint policy', () => {
  it('requires an explicit endpoint for authenticated calls in every environment', () => {
    for (const NODE_ENV of ['development', 'test', 'production']) {
      expect(() => resolve({ NODE_ENV })).toThrow(
        'CORAL_ENDPOINT must be set when Coral authentication is enabled',
      )
    }
  })

  it('accepts authenticated HTTPS and ignores an irrelevant opt-in value', () => {
    expect(
      resolve({
        CORAL_ENDPOINT: 'https://coral.example.test/',
        REEF_ALLOW_INSECURE_CORAL_ENDPOINT: 'garbage',
      }),
    ).toEqual({ authenticatedCleartextOrigin: null, baseUrl: 'https://coral.example.test' })
  })

  it.each([
    'http://localhost:14555',
    'http://127.42.0.1:14555',
    'http://[::1]:14555',
    'http://[::ffff:127.0.0.1]:14555',
  ])('accepts authenticated explicit-loopback HTTP: %s', (CORAL_ENDPOINT) => {
    expect(resolve({ CORAL_ENDPOINT })).toEqual({
      authenticatedCleartextOrigin: null,
      baseUrl: CORAL_ENDPOINT,
    })
  })

  it.each([' 1 ', ' TrUe '])('accepts normalized cleartext opt-in %j', (optIn) => {
    expect(
      resolve({
        CORAL_ENDPOINT: 'http://coral.internal:14555/rpc/',
        REEF_ALLOW_INSECURE_CORAL_ENDPOINT: optIn,
      }),
    ).toEqual({
      authenticatedCleartextOrigin: 'http://coral.internal:14555',
      baseUrl: 'http://coral.internal:14555/rpc',
    })
  })

  it.each([undefined, '', '0', ' false '])('rejects cleartext without opt-in %j', (optIn) => {
    expect(() =>
      resolve({
        CORAL_ENDPOINT: 'http://coral.internal:14555',
        REEF_ALLOW_INSECURE_CORAL_ENDPOINT: optIn,
      }),
    ).toThrow('CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP')
  })

  it('rejects localhost subdomains and validates garbage only on an insecure path', () => {
    expect(() => resolve({ CORAL_ENDPOINT: 'http://coral.localhost:14555' })).toThrow(
      'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP',
    )
    expect(() =>
      resolve({
        CORAL_ENDPOINT: 'http://coral.internal:14555',
        REEF_ALLOW_INSECURE_CORAL_ENDPOINT: 'yes',
      }),
    ).toThrow('REEF_ALLOW_INSECURE_CORAL_ENDPOINT must be set to 1 or true')
  })

  it.each(['file:///tmp/coral.sock', 'grpc://coral.internal:14555'])(
    'always rejects non-HTTP endpoint %s',
    (CORAL_ENDPOINT) => {
      expect(() => resolve({ CORAL_ENDPOINT, REEF_ALLOW_INSECURE_CORAL_ENDPOINT: 'true' })).toThrow(
        'CORAL_ENDPOINT must be an absolute HTTP(S) URL',
      )
    },
  )

  it.each([
    'https://operator:secret@coral.example.test',
    'https://coral.example.test/rpc?tenant=analytics',
    'https://coral.example.test/rpc?',
    'https://coral.example.test/rpc#method',
    'https://coral.example.test/rpc#',
  ])('rejects endpoint URL components that Connect cannot safely compose: %s', (CORAL_ENDPOINT) => {
    expect(() => resolve({ CORAL_ENDPOINT })).toThrow(
      'CORAL_ENDPOINT must not include credentials, a query string, or a fragment',
    )
  })

  it('preserves request-derived development fallback only for unauthenticated calls', () => {
    expect(resolve({ NODE_ENV: 'development' }, false)).toEqual({
      authenticatedCleartextOrigin: null,
      baseUrl: request.url.replace('/a/path', ''),
    })
    expect(() => resolve({ NODE_ENV: 'production' }, false)).toThrow(
      'CORAL_ENDPOINT must be set in production',
    )
  })

  it.each(['http://[::1]:5173/a/path', 'http://[::ffff:127.0.0.1]:5173/a/path'])(
    'uses the default Coral endpoint for unauthenticated IPv6 development origin %s',
    (url) => {
      expect(
        resolveCoralEndpoint({
          authenticated: false,
          env: { NODE_ENV: 'development' },
          request: new Request(url),
        }),
      ).toEqual({
        authenticatedCleartextOrigin: null,
        baseUrl: DEFAULT_DEV_CORAL_ENDPOINT,
      })
    },
  )
})
