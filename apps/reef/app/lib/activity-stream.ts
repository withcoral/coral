import type {
  ActivityCountValue,
  ActivityObservation,
  ActivityPageValue,
  ActivityStreamEvent,
} from './activity-live-types'

/** Coral heartbeats every 5 seconds. Three missed heartbeats end the authorized view. */
const LEASE_MS = 15_000
const RETRY_MS = 2000

interface Streams {
  '/activity/count': { input: undefined; value: ActivityCountValue }
  '/activity/observe': { input: ActivityObservation; value: ActivityPageValue }
}
const valueKinds = { '/activity/count': 'count', '/activity/observe': 'page' } as const

export interface ActivityStreamSubscription {
  /** Replace the connection after `delay` ms. The lease keeps running, so rescheduling
   * cannot extend authorization without a server event. */
  reconnect(delay: number): void
  close(): void
}

/** The only browser code that holds a connection to Reef. Loaders supply initial data;
 * the stream only pushes later changes. Each reconnect is a new authorized snapshot:
 * no event IDs, replay cursors or cached authority cross attempts.
 *
 * `onUnavailable` runs whenever the view loses authorization: a stall past the lease,
 * an `unavailable` event, a failed or ended connection, or the browser going offline.
 * A value that `onValue` rejects by throwing ends the connection the same way.
 */
export function subscribeActivityStream<Path extends keyof Streams>(
  path: Path,
  stream: {
    /** Read at every connection, so a reconnect carries the latest reading window. */
    input?: () => Streams[Path]['input']
    onValue: (value: Streams[Path]['value']) => void
    onUnavailable: () => void
    /** The session ended. Retrying cannot help, so the stream stops for good. */
    onSessionExpired: () => void
  },
): ActivityStreamSubscription {
  let closed = false
  let expired = false
  let attempt: AbortController | undefined
  let lease: ReturnType<typeof setTimeout> | undefined
  let retry: ReturnType<typeof setTimeout> | undefined
  const renew = () => {
    clearTimeout(lease)
    lease = setTimeout(() => {
      lease = undefined
      attempt?.abort()
      stream.onUnavailable()
    }, LEASE_MS)
  }
  const expire = () => {
    expired = true
    attempt?.abort()
    clearTimeout(lease)
    clearTimeout(retry)
    stream.onSessionExpired()
  }
  const schedule = (delay: number) => {
    if (expired) return
    clearTimeout(retry)
    retry = setTimeout(() => void connect(), delay)
  }
  async function connect() {
    const current = new AbortController()
    attempt = current
    // A connection that never answers still ends within one lease.
    if (lease === undefined) renew()
    // Reef ends every stream at a lifetime bound. The replacement follows at once, and the
    // view stays as it is: the ended stream was healthy.
    let replace = false
    try {
      const input = stream.input?.()
      const response = await fetch(path, {
        method: 'POST',
        ...(input === undefined
          ? {}
          : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input) }),
        // Reef redirects an expired session to sign in; following that into OAuth is never a stream.
        redirect: 'manual',
        signal: current.signal,
      })
      if (response.type === 'opaqueredirect' || (response.status >= 300 && response.status < 400))
        return expire()
      for await (const event of streamEvents<Streams[Path]['value']>(response)) {
        // A superseded or canceled connection must never publish into a newer view.
        current.signal.throwIfAborted()
        if (event.kind === 'sessionExpired') return expire()
        if (event.kind === 'unavailable') break
        renew()
        if (event.kind === 'heartbeat') continue
        if (event.kind === 'reconnect') {
          replace = true
          break
        }
        if (event.kind !== valueKinds[path]) throw new Error('Unexpected activity event')
        stream.onValue(event as Streams[Path]['value'])
      }
    } catch {
      // Every failure below ends this connection closed.
    }
    if (closed || expired || attempt !== current) return
    current.abort()
    if (replace) {
      schedule(0)
      return
    }
    stream.onUnavailable()
    schedule(RETRY_MS)
  }
  const offline = () => {
    if (expired) return
    attempt?.abort()
    stream.onUnavailable()
  }
  globalThis.addEventListener?.('offline', offline)
  void connect()
  return {
    reconnect(delay) {
      attempt?.abort()
      attempt = undefined
      schedule(delay)
    },
    close() {
      closed = true
      attempt?.abort()
      clearTimeout(lease)
      clearTimeout(retry)
      globalThis.removeEventListener?.('offline', offline)
    },
  }
}

/** SSE over fetch keeps the bounded reading window in a POST body, not a URL.
 * Exported for tests that read the server adapter's output.
 */
export async function* streamEvents<Value extends ActivityPageValue | ActivityCountValue>(
  response: Response,
): AsyncGenerator<ActivityStreamEvent<Value>> {
  if (
    !response.ok ||
    !response.headers.get('content-type')?.startsWith('text/event-stream') ||
    !response.body
  )
    throw new Error('Activity unavailable')
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let pending = ''
  let data: string[] = []
  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) return
      pending += decoder.decode(value, { stream: true })
      let end: number
      while ((end = pending.indexOf('\n')) >= 0) {
        const line = pending.slice(0, end).replace(/\r$/, '')
        pending = pending.slice(end + 1)
        if (line === '') {
          if (data.length) yield JSON.parse(data.join('\n')) as ActivityStreamEvent<Value>
          data = []
        } else if (line === 'data' || line.startsWith('data:')) {
          data.push(line.slice(5).replace(/^ /, ''))
        }
      }
    }
  } finally {
    await reader.cancel()
  }
}
