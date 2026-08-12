import { create } from '@bufbuild/protobuf'
import { describe, expect, it, vi } from 'vitest'

import {
  CreateBundledSourceWithOAuthResponseSchema,
  OAuthCredentialCallbackReceivedSchema,
  SourceSchema,
  type CreateBundledSourceWithOAuthResponse,
} from '@/generated/coral/v1/sources_pb'
import type { OAuthInstallStreamEvent } from '@/lib/source-oauth-install-stream'
import { relayOAuthSourceStreamEvents } from '@/lib/source-oauth-response.server'

async function* responses(
  events: CreateBundledSourceWithOAuthResponse[],
): AsyncIterable<CreateBundledSourceWithOAuthResponse> {
  yield* events
}

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
