import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  OauthCredentialFlowType,
  SourceInfoSchema,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'

const { describeSourceManifest, importSource, sourceClientForRequest } = vi.hoisted(() => ({
  describeSourceManifest: vi.fn(),
  importSource: vi.fn(),
  sourceClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({ sourceClientForRequest }))

import { action } from './source-oauth-import'

const MANIFEST = 'name: github'

describe('source OAuth import action', () => {
  beforeEach(() => {
    describeSourceManifest.mockReset()
    importSource.mockReset()
    sourceClientForRequest.mockReset()
    importSource.mockReturnValue(emptyStream())
    sourceClientForRequest.mockReturnValue({ describeSourceManifest, importSource })
  })

  it('threads the server-held access token into the source client', async () => {
    describeSourceManifest.mockResolvedValue({ sourceInfo: sourceInfo() })
    const request = importRequest({ 'method:GITHUB_TOKEN': '0' })

    const response = await action(authRouteTestArgs(request, { workspaceId: 'analytics' }))
    await response.text()

    expect(sourceClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
    expect(importSource).toHaveBeenCalledWith(
      expect.objectContaining({
        manifestYaml: MANIFEST,
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
      expect.objectContaining({ signal: request.signal }),
    )
  })

  it('sends the variables and plain secrets alongside the OAuth retrievals', async () => {
    describeSourceManifest.mockResolvedValue({ sourceInfo: sourceInfo() })
    const request = importRequest({
      'method:GITHUB_TOKEN': '0',
      'sec:EXTRA_TOKEN': ' pasted ',
      'var:GITHUB_API_BASE': 'https://ghe.example.test/api/v3',
    })

    await (await action(authRouteTestArgs(request, { workspaceId: 'analytics' }))).text()

    // create() wraps each binding in a protobuf message, so compare the fields the
    // request actually carries rather than the whole object.
    const [sent] = importSource.mock.calls[0] as [
      {
        oauthCredentialRetrievals: { inputKey: string }[]
        secrets: { key: string; value: string }[]
        variables: { key: string; value: string }[]
      },
    ]
    expect(sent.variables.map(({ key, value }) => ({ key, value }))).toEqual([
      { key: 'GITHUB_API_BASE', value: 'https://ghe.example.test/api/v3' },
    ])
    expect(sent.secrets.map(({ key, value }) => ({ key, value }))).toEqual([
      { key: 'EXTRA_TOKEN', value: 'pasted' },
    ])
    expect(sent.oauthCredentialRetrievals.map((r) => r.inputKey)).toEqual(['GITHUB_TOKEN'])
  })

  it('rejects a manifest whose selected methods are all non-OAuth', async () => {
    describeSourceManifest.mockResolvedValue({ sourceInfo: sourceInfo() })
    const request = importRequest({ 'method:GITHUB_TOKEN': '1' })

    const response = await action(authRouteTestArgs(request, { workspaceId: 'analytics' }))

    expect(response.status).toBe(400)
    expect(importSource).not.toHaveBeenCalled()
  })
})

function importRequest(fields: Record<string, string>): Request {
  return new Request('https://coral-ui.example.test/workspaces/analytics/sources/oauth-import', {
    body: new URLSearchParams({ manifest_yaml: MANIFEST, ...fields }),
    method: 'POST',
  })
}

function sourceInfo(): SourceInfo {
  return create(SourceInfoSchema, {
    name: 'github',
    inputs: [
      {
        key: 'GITHUB_API_BASE',
        input: { case: 'variable', value: { defaultValue: 'https://api.github.com' } },
      },
      {
        key: 'GITHUB_TOKEN',
        input: {
          case: 'secret',
          value: {
            credential: {
              methods: [
                {
                  method: {
                    case: 'oauth',
                    value: { flow: OauthCredentialFlowType.DEVICE_CODE },
                  },
                },
                { method: { case: 'sourceConfig', value: {} } },
              ],
            },
          },
        },
      },
      {
        key: 'EXTRA_TOKEN',
        input: { case: 'secret', value: {} },
      },
    ],
  })
}

async function* emptyStream() {}
