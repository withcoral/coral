import { setTimeout as delay } from 'node:timers/promises'
import { Code, ConnectError, createClient, createRouterTransport } from '@connectrpc/connect'
import { afterEach, expect, it, vi } from 'vitest'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import { STREAM_LIFETIME_MS, activityStream } from './activity-stream.server'
import { streamEvents } from './activity-stream'
import { activityChanges } from './activity-stream.test-support'

afterEach(() => {
  vi.useRealTimers()
})

it('subscribes before the initial read and refreshes on changes, not heartbeats', async () => {
  const changes = activityChanges()
  let reads = 0
  let count = '1'
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(({ service }) => {
        service(ActivityService, { watchActivity: changes.watch })
      }),
    ),
  }
  const response = activityStream(new Request('http://reef/activity/count'), api, async () => {
    reads++
    const snapshot = count
    if (reads === 1) {
      count = '2'
      changes.emit()
    } // Commit during the first read.
    return { kind: 'count', counts: { '/workspaces/w': snapshot } }
  })
  expect(response.headers.get('content-type')).toBe('text/event-stream')
  expect(response.headers.get('cache-control')).toContain('no-store')
  const events = streamEvents(response)
  try {
    expect((await events.next()).value).toEqual({
      kind: 'count',
      counts: { '/workspaces/w': '1' },
    })
    expect((await events.next()).value).toEqual({
      kind: 'count',
      counts: { '/workspaces/w': '2' },
    })
    changes.emit(false)
    expect((await events.next()).value).toEqual({ kind: 'heartbeat' })
    expect(reads).toBe(2)
    changes.emit() // A private/unrelated change must not become a browser event.
    changes.emit(false)
    expect((await events.next()).value).toEqual({ kind: 'heartbeat' })
    expect(reads).toBe(3)
  } finally {
    await events.return(undefined)
  }
})

it('fails closed when a change cannot be reauthorized instead of retaining a count', async () => {
  const changes = activityChanges()
  let available = true
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(({ service }) => {
        service(ActivityService, { watchActivity: changes.watch })
      }),
    ),
  }
  const events = streamEvents(
    activityStream(new Request('http://reef/activity/count'), api, async () => {
      if (!available) throw new Error('authority lost')
      return { kind: 'count', counts: { '/workspaces/w': '7' } }
    }),
  )
  expect((await events.next()).value).toEqual({
    kind: 'count',
    counts: { '/workspaces/w': '7' },
  })
  available = false
  changes.emit()
  expect((await events.next()).value).toEqual({ kind: 'unavailable' })
  expect((await events.next()).done).toBe(true)
})

it('keeps healthy observation open beyond the ordinary unary RPC deadline', async () => {
  const changes = activityChanges()
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(
        ({ service }) => {
          service(ActivityService, { watchActivity: changes.watch })
        },
        { transport: { defaultTimeoutMs: 50 } },
      ),
    ),
  }
  const events = streamEvents(
    activityStream(new Request('http://reef/activity/count'), api, async () => ({
      kind: 'count',
      counts: { '/workspaces/w': '1' },
    })),
  )
  try {
    expect((await events.next()).value).toEqual({
      kind: 'count',
      counts: { '/workspaces/w': '1' },
    })
    const heartbeat = events.next()
    await delay(100)
    changes.emit(false)
    expect((await heartbeat).value).toEqual({ kind: 'heartbeat' })
  } finally {
    await events.return(undefined)
  }
})

it('expires an idle session without reloading its count or waiting for a mutation', async () => {
  const changes = activityChanges()
  let reads = 0
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(({ service }) => {
        service(ActivityService, { watchActivity: changes.watch })
      }),
    ),
  }
  const events = streamEvents(
    activityStream(
      new Request('http://reef/activity/count'),
      api,
      async () => {
        reads++
        return { kind: 'count', counts: { '/workspaces/w': '1' } }
      },
      Date.now() / 1000 + 0.05,
    ),
  )
  expect((await events.next()).value).toEqual({
    kind: 'count',
    counts: { '/workspaces/w': '1' },
  })
  expect((await events.next()).value).toEqual({ kind: 'sessionExpired' })
  expect(reads).toBe(1)
  expect((await events.next()).done).toBe(true)
})

// Catches a stream that a proxy strands after the browser dropped it: it would keep its
// observer and read on every change until the session ended.
it('ends a healthy stream at its lifetime bound with a reconnect event', async () => {
  vi.useFakeTimers()
  const changes = activityChanges()
  let reads = 0
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(({ service }) => {
        service(ActivityService, { watchActivity: changes.watch })
      }),
    ),
  }
  const events = streamEvents(
    activityStream(
      new Request('http://reef/activity/count'),
      api,
      async () => {
        reads++
        return { kind: 'count', counts: { '/workspaces/w': '1' } }
      },
      Date.now() / 1000 + 3600,
    ),
  )
  expect((await events.next()).value).toEqual({
    kind: 'count',
    counts: { '/workspaces/w': '1' },
  })
  const next = events.next()
  await vi.advanceTimersByTimeAsync(STREAM_LIFETIME_MS - 1)
  changes.emit(false)
  expect((await next).value).toEqual({ kind: 'heartbeat' })
  const end = events.next()
  await vi.advanceTimersByTimeAsync(1)
  expect((await end).value).toEqual({ kind: 'reconnect' })
  expect((await events.next()).done).toBe(true)
  expect(reads).toBe(1)
})

it('ends an already expired session before subscribing or reading', async () => {
  let reads = 0
  let watched = false
  const api = {
    activity: createClient(
      ActivityService,
      createRouterTransport(({ service }) => {
        service(ActivityService, {
          // eslint-disable-next-line require-yield
          watchActivity: async function* () {
            watched = true
          },
        })
      }),
    ),
  }
  const response = activityStream(
    new Request('http://reef/activity/count'),
    api,
    async () => {
      reads++
      return { kind: 'count', counts: { '/workspaces/w': '1' } }
    },
    0,
  )
  expect(await response.text()).toBe('event: sessionExpired\ndata: {"kind":"sessionExpired"}\n\n')
  expect(reads).toBe(0)
  expect(watched).toBe(false)
})

it('tells a session Coral rejects apart from an outage', async () => {
  for (const [code, kind] of [
    [Code.Unauthenticated, 'sessionExpired'],
    [Code.Unavailable, 'unavailable'],
  ] as const) {
    const api = {
      activity: createClient(
        ActivityService,
        createRouterTransport(({ service }) => {
          service(ActivityService, {
            // eslint-disable-next-line require-yield
            watchActivity: async function* () {
              throw new ConnectError('rejected', code)
            },
          })
        }),
      ),
    }
    const events = streamEvents(
      activityStream(new Request('http://reef/activity/count'), api, async () => ({
        kind: 'count',
        counts: { '/workspaces/w': '1' },
      })),
    )
    expect((await events.next()).value).toEqual({ kind })
    expect((await events.next()).done).toBe(true)
  }
})
