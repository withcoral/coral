import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

const { createBundledSourceWithOAuth, getSourceInfo } = vi.hoisted(() => ({
  createBundledSourceWithOAuth: vi.fn(),
  getSourceInfo: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  sourceClientForRequest: () => ({ createBundledSourceWithOAuth, getSourceInfo }),
}))

import {
  CreateBundledSourceWithOAuthResponseSchema,
  OAuthCredentialMethodSchema,
  OAuthCredentialCallbackReceivedSchema,
  SourceCredentialMethodSchema,
  SourceCredentialSchema,
  SourceInfoSchema,
  SourceInputSpecSchema,
  SourceOrigin,
  SourceSchema,
  SourceSecretInputSchema,
  type CreateBundledSourceWithOAuthResponse,
} from '@/generated/coral/v1/sources_pb'
import type { OAuthInstallStreamEvent } from '@/lib/source-oauth-install-stream'
import { relayOAuthSourceStreamEvents } from '@/lib/source-oauth-response.server'

import { action } from './source-oauth-install'

async function* responses(
  events: CreateBundledSourceWithOAuthResponse[],
): AsyncIterable<CreateBundledSourceWithOAuthResponse> {
  yield* events
}

const oauthSourceInfo = create(SourceInfoSchema, {
  inputs: [
    create(SourceInputSpecSchema, {
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {
          credential: create(SourceCredentialSchema, {
            methods: [
              create(SourceCredentialMethodSchema, {
                method: { case: 'oauth', value: create(OAuthCredentialMethodSchema) },
              }),
            ],
          }),
        }),
      },
      key: 'GITHUB_TOKEN',
      required: true,
    }),
  ],
  name: 'github',
  origin: SourceOrigin.BUNDLED,
})

describe('action', () => {
  beforeEach(() => {
    createBundledSourceWithOAuth.mockReset()
    getSourceInfo.mockReset()
  })

  it('installs the source in the route workspace', async () => {
    getSourceInfo.mockResolvedValue({ sourceInfo: oauthSourceInfo })
    createBundledSourceWithOAuth.mockImplementation(async function* () {
      yield create(CreateBundledSourceWithOAuthResponseSchema, {
        event: {
          case: 'source',
          value: create(SourceSchema, { name: 'github', version: '1.0.0' }),
        },
      })
    })
    const request = new Request(
      'http://localhost/workspaces/analytics/sources/github/oauth-install',
      {
        body: new URLSearchParams({
          'method:GITHUB_TOKEN': '0',
          name: 'github',
        }),
        method: 'POST',
      },
    )

    const response = await action(
      authRouteTestArgs(request, { sourceName: 'github', workspaceId: 'analytics' }),
    )
    await response.text()

    expect(getSourceInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'github',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(createBundledSourceWithOAuth).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'github',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
      expect.objectContaining({ signal: request.signal }),
    )
  })
})

describe('relayOAuthSourceStreamEvents', () => {
  it('relays callback receipt before the terminal source event', async () => {
    const send = vi.fn<(event: OAuthInstallStreamEvent) => void>()

    await relayOAuthSourceStreamEvents(
      responses([
        create(CreateBundledSourceWithOAuthResponseSchema, {
          event: {
            case: 'oauthCallbackReceived',
            value: create(OAuthCredentialCallbackReceivedSchema, { inputKey: 'GITHUB_TOKEN' }),
          },
        }),
        create(CreateBundledSourceWithOAuthResponseSchema, {
          event: {
            case: 'source',
            value: create(SourceSchema, { name: 'github', version: '1.0.0' }),
          },
        }),
      ]),
      send,
    )

    expect(send.mock.calls.map(([event]) => event)).toEqual([
      { type: 'oauthCallbackReceived', inputKey: 'GITHUB_TOKEN' },
      { type: 'source', name: 'github', version: '1.0.0' },
    ])
  })
})
