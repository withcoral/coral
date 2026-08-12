import type {
  CreateBundledSourceWithOAuthResponse,
  ImportSourceResponse,
} from '@/generated/coral/v1/sources_pb'

import {
  oauthInstallEventToNdjson,
  type OAuthInstallStreamEvent,
} from './source-oauth-install-stream'

const NDJSON_HEADERS = {
  'Cache-Control': 'no-store',
  'Content-Type': 'application/x-ndjson; charset=utf-8',
} as const

type OAuthSourceResponse = CreateBundledSourceWithOAuthResponse | ImportSourceResponse

export async function oauthSourceStreamResponse(
  responses: AsyncIterable<OAuthSourceResponse>,
  signal?: AbortSignal,
): Promise<Response> {
  const iterator = responses[Symbol.asyncIterator]()
  const first = await iterator.next()
  if (first.done) throw new Error('OAuth install stream ended without a source event')

  const encoder = new TextEncoder()
  let closed = false

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (event: OAuthInstallStreamEvent) => {
        if (closed || signal?.aborted) return
        controller.enqueue(encoder.encode(oauthInstallEventToNdjson(event)))
      }

      try {
        await relayOAuthSourceStreamEvents(responsesFromFirst(first.value, iterator), send, signal)
      } catch (error) {
        if (!signal?.aborted) {
          send({ type: 'error', message: error instanceof Error ? error.message : String(error) })
        }
      } finally {
        if (!closed) {
          closed = true
          if (!signal?.aborted) controller.close()
        }
      }
    },
    cancel() {
      closed = true
    },
  })

  return new Response(stream, { headers: NDJSON_HEADERS })
}

async function* responsesFromFirst(
  first: OAuthSourceResponse,
  iterator: AsyncIterator<OAuthSourceResponse>,
): AsyncIterable<OAuthSourceResponse> {
  try {
    yield first
    while (true) {
      const next = await iterator.next()
      if (next.done) return
      yield next.value
    }
  } finally {
    await iterator.return?.()
  }
}

export async function relayOAuthSourceStreamEvents(
  responses: AsyncIterable<OAuthSourceResponse>,
  send: (event: OAuthInstallStreamEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  for await (const response of responses) {
    if (signal?.aborted) return
    const event = response.event
    switch (event.case) {
      case 'oauthAuthorization':
        send({
          type: 'oauthAuthorization',
          authorizationUrl: event.value.authorizationUrl,
          expiresInSeconds: event.value.expiresInSeconds.toString(),
          inputKey: event.value.inputKey,
          userCode: event.value.userCode,
          verificationUri: event.value.verificationUri,
          verificationUriComplete: event.value.verificationUriComplete,
        })
        break
      case 'oauthCallbackReceived':
        send({ type: 'oauthCallbackReceived', inputKey: event.value.inputKey })
        break
      case 'oauthCompleted':
        send({
          type: 'oauthCompleted',
          inputKey: event.value.inputKey,
          metadata: event.value.metadata.map((item) => ({ key: item.key, value: item.value })),
        })
        break
      case 'source':
        send({ type: 'source', name: event.value.name, version: event.value.version })
        return
      case undefined:
        throw new Error('OAuth install stream included an empty event')
      default: {
        const exhaustive: never = event
        return exhaustive
      }
    }
  }
  throw new Error('OAuth install stream ended without a source event')
}

export function oauthStreamErrorResponse(message: string, status: number): Response {
  return new Response(oauthInstallEventToNdjson({ type: 'error', message }), {
    headers: NDJSON_HEADERS,
    status,
  })
}
