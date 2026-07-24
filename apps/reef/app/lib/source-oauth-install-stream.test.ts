import { describe, expect, it, vi } from 'vitest'

import { oauthInstallEventToNdjson, readOAuthInstallStream } from './source-oauth-install-stream'

function streamedResponse(chunks: string[], init?: ResponseInit): Response {
  const encoder = new TextEncoder()
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
        controller.close()
      },
    }),
    init,
  )
}

describe('readOAuthInstallStream', () => {
  it('parses incremental NDJSON OAuth progress and final source events', async () => {
    const authorization = oauthInstallEventToNdjson({
      type: 'oauthAuthorization',
      authorizationUrl: 'https://github.com/login/device',
      expiresInSeconds: '900',
      inputKey: 'GITHUB_TOKEN',
      userCode: 'ABCD-1234',
      verificationUri: 'https://github.com/login/device',
      verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-1234',
    })
    const completed = oauthInstallEventToNdjson({
      type: 'oauthCompleted',
      inputKey: 'GITHUB_TOKEN',
      metadata: [{ key: 'account', value: 'coral' }],
    })
    const callbackReceived = oauthInstallEventToNdjson({
      type: 'oauthCallbackReceived',
      inputKey: 'GITHUB_TOKEN',
    })
    const source = oauthInstallEventToNdjson({ type: 'source', name: 'github', version: '1.0.0' })
    const onAuthorization = vi.fn()
    const onCallbackReceived = vi.fn()
    const onCompleted = vi.fn()
    const onSource = vi.fn()

    const result = await readOAuthInstallStream(
      streamedResponse([
        authorization.slice(0, 20),
        authorization.slice(20),
        callbackReceived,
        completed,
        source,
      ]),
      { onAuthorization, onCallbackReceived, onCompleted, onSource },
    )

    expect(result).toEqual({ type: 'source', name: 'github', version: '1.0.0' })
    expect(onAuthorization).toHaveBeenCalledWith(
      expect.objectContaining({
        expiresInSeconds: '900',
        inputKey: 'GITHUB_TOKEN',
        userCode: 'ABCD-1234',
      }),
    )
    expect(onCompleted).toHaveBeenCalledWith(
      expect.objectContaining({
        inputKey: 'GITHUB_TOKEN',
        metadata: [{ key: 'account', value: 'coral' }],
      }),
    )
    expect(onCallbackReceived).toHaveBeenCalledWith({
      type: 'oauthCallbackReceived',
      inputKey: 'GITHUB_TOKEN',
    })
    expect(onSource).toHaveBeenCalledWith({ type: 'source', name: 'github', version: '1.0.0' })
  })

  it('surfaces streamed error events', async () => {
    await expect(
      readOAuthInstallStream(
        streamedResponse([oauthInstallEventToNdjson({ type: 'error', message: 'boom' })], {
          status: 400,
        }),
      ),
    ).rejects.toThrow('boom')
  })

  it.each([
    { type: 'oauthAuthorization' },
    { type: 'oauthCallbackReceived' },
    { type: 'oauthCompleted', inputKey: 'GITHUB_TOKEN', metadata: [{}] },
    { type: 'source', name: 'github' },
    { type: 'error' },
  ])('rejects malformed $type events', async (event) => {
    await expect(
      readOAuthInstallStream(streamedResponse([`${JSON.stringify(event)}\n`])),
    ).rejects.toThrow('OAuth install stream included an invalid event')
  })
})
