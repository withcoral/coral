import { beforeEach, describe, expect, it, vi } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

const { importSource, sourceClientForRequest } = vi.hoisted(() => ({
  importSource: vi.fn(),
  sourceClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({ sourceClientForRequest }))

import { action } from './source-oauth-import'

describe('source OAuth import action', () => {
  beforeEach(() => {
    importSource.mockReset()
    sourceClientForRequest.mockReset()
  })

  it('threads the server-held access token into the source client', async () => {
    importSource.mockReturnValue(emptyStream())
    sourceClientForRequest.mockReturnValue({ importSource })
    const request = new Request(
      'https://reef.example.test/workspaces/analytics/sources/oauth-import',
      {
        body: new URLSearchParams({
          manifest_yaml: 'name: github',
          oauth_input_key: 'GITHUB_TOKEN',
          oauth_method_index: '0',
        }),
        method: 'POST',
      },
    )

    const response = await action(authRouteTestArgs(request, { workspaceId: 'analytics' }))
    await response.text()

    expect(sourceClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
    expect(importSource).toHaveBeenCalledWith(
      expect.objectContaining({
        manifestYaml: 'name: github',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
      expect.objectContaining({ signal: request.signal }),
    )
  })
})

async function* emptyStream() {}
