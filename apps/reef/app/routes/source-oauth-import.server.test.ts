import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { importSource } = vi.hoisted(() => ({ importSource: vi.fn() }))

vi.mock('@/lib/coral-request.server', () => ({
  sourceClientForRequest: () => ({ importSource }),
}))

import { ImportSourceResponseSchema, SourceSchema } from '@/generated/coral/v1/sources_pb'

import { action } from './source-oauth-import'

describe('action', () => {
  beforeEach(() => importSource.mockReset())

  it('imports the authored manifest with its selected OAuth credential method', async () => {
    importSource.mockImplementation(async function* () {
      yield create(ImportSourceResponseSchema, {
        event: {
          case: 'source',
          value: create(SourceSchema, { name: 'github_custom' }),
        },
      })
    })
    const request = new Request('http://localhost/workspaces/analytics/sources/oauth-import', {
      body: new URLSearchParams({
        manifest_yaml: 'name: github_custom\ndsl_version: 4\n',
        name: 'github_custom',
        oauth_input_key: 'API_TOKEN',
        oauth_method_index: '0',
      }),
      method: 'POST',
    })

    const response = await action({
      params: { workspaceId: 'analytics' },
      request,
    } as Parameters<typeof action>[0])
    expect(response.headers.get('content-type')).toContain('application/x-ndjson')
    await response.text()

    expect(importSource).toHaveBeenCalledWith(
      expect.objectContaining({
        manifestYaml: 'name: github_custom\ndsl_version: 4\n',
        oauthCredentialRetrievals: [
          expect.objectContaining({ inputKey: 'API_TOKEN', methodIndex: 0 }),
        ],
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
      expect.objectContaining({ signal: request.signal }),
    )
  })
})
