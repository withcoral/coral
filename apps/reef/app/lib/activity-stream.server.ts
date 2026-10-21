import { createHash } from 'node:crypto'
import { Code, ConnectError } from '@connectrpc/connect'
import type {
  ActivityCountValue,
  ActivityPageValue,
  ActivityStreamEvent,
} from './activity-live-types'
import { privateHeaders, type ManagementApi } from './management.server'

/** Compare authorized evidence, not clock samples. Otherwise unrelated private
 * mutations could leak through changing duration fields in an unchanged view.
 */
function evidence(value: ActivityPageValue | ActivityCountValue) {
  return createHash('sha256')
    .update(
      JSON.stringify(value, (key, field: unknown) =>
        key === 'durationNanos' || key === 'waitingNanos' ? undefined : field,
      ),
    )
    .digest('hex')
}

/** A proxy that terminates the browser's HTTP/2 connection (an AWS ALB, for one) can keep
 * its own request to Reef open after the browser dropped the stream, and the heartbeats
 * defeat its idle timeout. Reef cannot tell such a stranded stream from a live one, so every
 * stream ends after this long and the browser replaces it with a fresh snapshot. The bound
 * caps how long a stranded stream keeps reading on every change.
 */
export const STREAM_LIFETIME_MS = 2 * 60_000

/** Feature-specific SSE adapter. Coral subscribes before its first `changed` message, so
 * changes during reads stay queued. Coral heartbeats become browser heartbeats without an
 * activity read. No idle reload timer, raw invalidation hints or hidden payloads reach the
 * browser.
 */
export function activityStream<Value extends ActivityPageValue | ActivityCountValue>(
  request: Request,
  api: Pick<ManagementApi, 'activity'>,
  read: (signal: AbortSignal) => Promise<Value>,
  expiresAt = Infinity,
): Response {
  const closed = new AbortController()
  const expired = new AbortController()
  const ended = new AbortController()
  const signal = AbortSignal.any([request.signal, closed.signal, expired.signal, ended.signal])
  async function* events(): AsyncGenerator<ActivityStreamEvent<Value>> {
    const remaining = expiresAt * 1000 - Date.now()
    // An early timer only ends the stream sooner; the browser then reconnects.
    const expiry = Number.isFinite(remaining)
      ? setTimeout(() => expired.abort(), Math.min(Math.max(0, remaining), 2_147_483_647))
      : undefined
    const lifetime = setTimeout(() => ended.abort(), STREAM_LIFETIME_MS)
    let previous: string | undefined
    let rejected = false
    try {
      if (remaining <= 0) throw new Error('Session expired')
      // The ordinary unary deadline must not reconnect a healthy stream.
      for await (const { changed } of api.activity.watchActivity({}, { signal, timeoutMs: 0 })) {
        if (!changed) {
          yield { kind: 'heartbeat' }
          continue
        }
        const value = await read(signal)
        signal.throwIfAborted()
        const key = evidence(value)
        if (key === previous) continue
        previous = key
        yield value
      }
    } catch (error) {
      rejected = error instanceof ConnectError && error.code === Code.Unauthenticated
    } finally {
      clearTimeout(expiry)
      clearTimeout(lifetime)
    }
    if (request.signal.aborted || closed.signal.aborted) return
    // Every failure ends the stream closed. An outage lets the browser reconnect to a new
    // snapshot; an expired or rejected session cannot recover, so the reader must sign in.
    // A stream at its lifetime bound was healthy, so the browser replaces it at once.
    yield expired.signal.aborted || remaining <= 0 || rejected
      ? { kind: 'sessionExpired' }
      : ended.signal.aborted
        ? { kind: 'reconnect' }
        : { kind: 'unavailable' }
  }
  const iterator = events()
  const encoder = new TextEncoder()
  const body = new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        const event = await iterator.next()
        if (event.done) controller.close()
        else
          controller.enqueue(
            encoder.encode(`event: ${event.value.kind}\ndata: ${JSON.stringify(event.value)}\n\n`),
          )
      },
      // ReadableStream.from would only call return(), which waits for the pending upstream
      // message. Abort first so cancellation releases the upstream watch immediately.
      async cancel() {
        closed.abort()
        await iterator.return(undefined)
      },
    },
    { highWaterMark: 0 },
  )
  return new Response(body, {
    headers: {
      ...privateHeaders,
      'Content-Type': 'text/event-stream',
      'X-Accel-Buffering': 'no',
    },
  })
}
