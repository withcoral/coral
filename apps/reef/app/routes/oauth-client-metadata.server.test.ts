import { describe, expect, it } from 'vitest'

import { localRuntimeConfig, publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig } from '@/auth/test-config'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import { loader } from './oauth-client-metadata'

describe('OAuth client metadata route', () => {
  it('serves a cacheable CIMD document derived from the canonical Coral web public URL', async () => {
    const response = await runLoader('https://internal-proxy/.well-known/oauth-client')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(response.headers.get('Content-Type')).toBe('application/json')
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=300')
    expect(response.headers.has('Set-Cookie')).toBe(false)
    expect(Buffer.byteLength(JSON.stringify(body))).toBeLessThanOrEqual(5 * 1024)
    expect(body).toEqual({
      client_id: 'https://coral-web.example.test/.well-known/oauth-client',
      client_name: 'Coral',
      grant_types: ['authorization_code'],
      redirect_uris: [
        'https://coral-web.example.test/auth/callback',
        'https://coral-web.example.test/provider-setup/oauth/callback',
      ],
      response_types: ['code'],
      token_endpoint_auth_method: 'none',
    })
  })

  it('does not publish OAuth client metadata when auth is disabled', async () => {
    const error = await runLoader(
      'http://localhost:5173/.well-known/oauth-client',
      localRuntimeConfig,
    ).catch((caught: unknown) => caught)

    expect(error).toBeInstanceOf(Response)
    expect((error as Response).status).toBe(404)
  })
})

function runLoader(
  url: string,
  runtime: CoralWebRuntimeConfig = hostedRuntimeConfig,
): Promise<Response> {
  return loader(publicRouteTestArgs(new Request(url), runtime))
}
