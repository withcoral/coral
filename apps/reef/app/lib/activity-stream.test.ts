import { afterEach, expect, it, vi } from 'vitest'
import { streamEvents, subscribeActivityStream } from './activity-stream'

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

const events = (...values: unknown[]) =>
  new Response(values.map((value) => `data: ${JSON.stringify(value)}\n\n`).join(''), {
    headers: { 'Content-Type': 'text/event-stream' },
  })

it('reads fragmented SSE frames without losing Unicode or splitting multiline data', async () => {
  const bytes = new TextEncoder().encode(
    ': keepalive\r\nevent: count\r\ndata: {"kind":"count",\r\ndata: "count":"é"}\r\n\r\n',
  )
  const response = new Response(
    new ReadableStream({
      start(controller) {
        for (const byte of bytes) controller.enqueue(new Uint8Array([byte]))
        controller.close()
      },
    }),
    { headers: { 'Content-Type': 'text/event-stream' } },
  )
  const parsed = streamEvents(response)
  expect((await parsed.next()).value).toEqual({ kind: 'count', count: 'é' })
  expect((await parsed.next()).done).toBe(true)
})

it('conceals a view that receives another stream’s value instead of publishing it', async () => {
  vi.useFakeTimers()
  const fetch = vi.fn(async () =>
    events({ kind: 'count', counts: { '/workspaces/w': '3' } }, { kind: 'page' }),
  )
  vi.stubGlobal('fetch', fetch)
  const values: unknown[] = []
  const onUnavailable = vi.fn()
  const stream = subscribeActivityStream('/activity/count', {
    onValue: (value) => values.push(value),
    onUnavailable,
    onSessionExpired() {},
  })
  try {
    await vi.waitFor(() => expect(onUnavailable).toHaveBeenCalled())
    expect(values).toEqual([{ kind: 'count', counts: { '/workspaces/w': '3' } }])
    expect(fetch).toHaveBeenCalledOnce()
    await vi.advanceTimersByTimeAsync(2000)
    expect(fetch).toHaveBeenCalledTimes(2)
  } finally {
    stream.close()
  }
})

it('keeps the lease across reconnects, so rescheduling never extends authorization', async () => {
  vi.useFakeTimers()
  const signals: AbortSignal[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn((_path: string, init: RequestInit) => {
      signals.push(init.signal!)
      return new Promise<Response>(() => {})
    }),
  )
  const onUnavailable = vi.fn()
  const stream = subscribeActivityStream('/activity/count', {
    onValue() {},
    onUnavailable,
    onSessionExpired() {},
  })
  try {
    for (let index = 0; index < 4; index++) {
      await vi.advanceTimersByTimeAsync(3000)
      stream.reconnect(0)
      await vi.advanceTimersByTimeAsync(0)
    }
    expect(signals).toHaveLength(5)
    expect(onUnavailable).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(3000)
    expect(onUnavailable).toHaveBeenCalledOnce()
    expect(signals.every((signal) => signal.aborted)).toBe(true)
  } finally {
    stream.close()
  }
})

it('never publishes a response that arrives after its connection was replaced', async () => {
  vi.useFakeTimers()
  const responses: ((response: Response) => void)[] = []
  // Pending requests ignore abort, so a replaced connection can still deliver.
  vi.stubGlobal(
    'fetch',
    vi.fn(() => new Promise<Response>((respond) => responses.push(respond))),
  )
  const onValue = vi.fn()
  const stream = subscribeActivityStream('/activity/count', {
    onValue,
    onUnavailable() {},
    onSessionExpired() {},
  })
  try {
    stream.reconnect(0)
    await vi.advanceTimersByTimeAsync(0)
    expect(responses).toHaveLength(2)
    responses[0](events({ kind: 'count', counts: { '/workspaces/w': '3' } }))
    await vi.advanceTimersByTimeAsync(0)
    expect(onValue).not.toHaveBeenCalled()
  } finally {
    stream.close()
  }
})

it('stops for good when the session ends, instead of retrying it as an outage', async () => {
  // Reef answers an expired session with a sign-in redirect before the stream starts,
  // and ends a started stream with an explicit event. Neither is an outage to retry.
  for (const response of [
    () => Response.redirect('http://reef.test/login?returnTo=%2Factivity', 303),
    () => events({ kind: 'sessionExpired' }),
  ]) {
    vi.useFakeTimers()
    const fetch = vi.fn((_path: string, _init: RequestInit) => Promise.resolve(response()))
    vi.stubGlobal('fetch', fetch)
    const onUnavailable = vi.fn()
    const onSessionExpired = vi.fn()
    const stream = subscribeActivityStream('/activity/count', {
      onValue() {},
      onUnavailable,
      onSessionExpired,
    })
    try {
      await vi.waitFor(() => expect(onSessionExpired).toHaveBeenCalledOnce())
      stream.reconnect(0)
      await vi.advanceTimersByTimeAsync(20_000)
      expect(fetch).toHaveBeenCalledOnce()
      expect(fetch.mock.calls[0][1]).toMatchObject({ redirect: 'manual' })
      expect(onUnavailable).not.toHaveBeenCalled()
      expect(onSessionExpired).toHaveBeenCalledOnce()
    } finally {
      stream.close()
      vi.useRealTimers()
    }
  }
})

/** A stream response the test writes to, one SSE event at a time. */
function liveStream() {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const fetch = vi.fn(
    async (_path: string, init: RequestInit) =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(value) {
            controller = value
            init.signal!.addEventListener('abort', () => value.error(new Error('aborted')), {
              once: true,
            })
          },
        }),
        { headers: { 'Content-Type': 'text/event-stream' } },
      ),
  )
  vi.stubGlobal('fetch', fetch)
  const emit = (event: unknown) =>
    controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`))
  return { fetch, emit }
}

function subscribeCount() {
  const onUnavailable = vi.fn()
  const stream = subscribeActivityStream('/activity/count', {
    onValue() {},
    onUnavailable,
    onSessionExpired() {},
  })
  return { stream, onUnavailable }
}

it('conceals the view when the browser goes offline, and retries only after the delay', async () => {
  vi.useFakeTimers()
  const browser = new EventTarget()
  vi.stubGlobal('addEventListener', browser.addEventListener.bind(browser))
  vi.stubGlobal('removeEventListener', browser.removeEventListener.bind(browser))
  const { fetch, emit } = liveStream()
  const { stream, onUnavailable } = subscribeCount()
  try {
    await vi.advanceTimersByTimeAsync(0)
    emit({ kind: 'count', counts: { '/workspaces/w': '1' } })
    await vi.advanceTimersByTimeAsync(0)
    browser.dispatchEvent(new Event('offline'))
    await vi.advanceTimersByTimeAsync(0)
    expect(onUnavailable).toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1999)
    expect(fetch).toHaveBeenCalledOnce()
    await vi.advanceTimersByTimeAsync(1)
    expect(fetch).toHaveBeenCalledTimes(2)
  } finally {
    stream.close()
  }
})

it('renews the lease on each heartbeat', async () => {
  vi.useFakeTimers()
  const { emit } = liveStream()
  const { stream, onUnavailable } = subscribeCount()
  try {
    await vi.advanceTimersByTimeAsync(10_000)
    emit({ kind: 'heartbeat' })
    await vi.advanceTimersByTimeAsync(14_000)
    expect(onUnavailable).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1000)
    expect(onUnavailable).toHaveBeenCalled()
  } finally {
    stream.close()
  }
})

it('conceals the view on an unavailable event and reconnects after the retry delay', async () => {
  vi.useFakeTimers()
  const { fetch, emit } = liveStream()
  const { stream, onUnavailable } = subscribeCount()
  try {
    await vi.advanceTimersByTimeAsync(0)
    emit({ kind: 'unavailable' })
    await vi.advanceTimersByTimeAsync(0)
    expect(onUnavailable).toHaveBeenCalledOnce()
    await vi.advanceTimersByTimeAsync(1999)
    expect(fetch).toHaveBeenCalledOnce()
    await vi.advanceTimersByTimeAsync(1)
    expect(fetch).toHaveBeenCalledTimes(2)
  } finally {
    stream.close()
  }
})

// Catches a lifetime end that conceals the view or waits out the retry delay.
it('replaces a stream that reached its lifetime at once, with a full lease and no concealment', async () => {
  vi.useFakeTimers()
  const { fetch, emit } = liveStream()
  const { stream, onUnavailable } = subscribeCount()
  try {
    await vi.advanceTimersByTimeAsync(0)
    emit({ kind: 'count', counts: { '/workspaces/w': '1' } })
    await vi.advanceTimersByTimeAsync(10_000)
    emit({ kind: 'reconnect' })
    await vi.advanceTimersByTimeAsync(0)
    expect(fetch).toHaveBeenCalledTimes(2)
    expect(onUnavailable).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(14_000)
    expect(onUnavailable).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1000)
    expect(onUnavailable).toHaveBeenCalled()
  } finally {
    stream.close()
  }
})

const countEvent = 'data: {"kind":"count","count":"1"}\n\n'
it.each([
  ['malformed JSON', 'data: {"kind":\n\n', { headers: { 'Content-Type': 'text/event-stream' } }],
  ['a response that is not SSE', countEvent, { headers: { 'Content-Type': 'text/plain' } }],
  [
    'a failed response',
    countEvent,
    { status: 503, headers: { 'Content-Type': 'text/event-stream' } },
  ],
])('conceals the view on %s without publishing a value', async (_, body, init) => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => new Response(body, init)),
  )
  const onValue = vi.fn()
  const onUnavailable = vi.fn()
  const stream = subscribeActivityStream('/activity/count', {
    onValue,
    onUnavailable,
    onSessionExpired() {},
  })
  try {
    await vi.waitFor(() => expect(onUnavailable).toHaveBeenCalledOnce())
    expect(onValue).not.toHaveBeenCalled()
  } finally {
    stream.close()
  }
})
