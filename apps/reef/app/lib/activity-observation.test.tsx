/** @vitest-environment happy-dom */
import { act, createRef, useLayoutEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { createMemoryRouter, Outlet, RouterProvider, useLoaderData } from 'react-router'
import { createStore, Provider } from 'jotai'
import { renderToString } from 'react-dom/server'
import { afterEach, expect, it, vi } from 'vitest'
import type { ActivityPage, ExecutionRow } from '@/views/activity/types'
import type { ActivityObservation } from './activity-live-types'
import { activityObservation, activityView, useActivityReader } from './activity-observation'
import { loadActivityPage } from './activity-page-fetch'
import { taskRow } from '@/views/activity/fixtures'

const page: ActivityPage = {
  rows: [],
  trace: null,
  hasBefore: false,
  hasAfter: false,
  anchorUnavailable: false,
  listUnavailable: false,
  agent: null,
  detail: null,
  unavailable: false,
  selected: null,
  selectedRow: null,
  listPath: '/activity',
}
/** One stream event that carries `value` as the observed page. */
const snapshot = (value: ActivityPage) =>
  new TextEncoder().encode(`data: ${JSON.stringify({ kind: 'page', page: value })}\n\n`)
afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})
it('aborts an unused subscription and ignores even a transport that delivers after cancellation', async () => {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  let signal!: AbortSignal
  vi.stubGlobal(
    'fetch',
    vi.fn((_url, init) => {
      signal = init.signal
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(c) {
              controller = c
            },
          }),
          { headers: { 'Content-Type': 'text/event-stream' } },
        ),
      )
    }),
  )
  const store = createStore()
  const state = activityObservation(page, '/activity', '')
  const unmount = store.sub(state, () => {})
  await vi.waitFor(() => expect(controller).toBeDefined())
  await Promise.resolve()
  unmount()
  expect(signal.aborted).toBe(true)
  controller.enqueue(
    new TextEncoder().encode(
      `data: ${JSON.stringify({ kind: 'page', page: { ...page, hasAfter: true } })}\n\n`,
    ),
  )
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(store.get(state).page.hasAfter).toBe(false)
})
it('keeps history reading identities when observation reconnects', async () => {
  vi.useFakeTimers()
  const root = 'workspaces/w/runs/r'
  const initial: ActivityPage = {
    ...page,
    listPath: '/activity',
    rows: [
      {
        kind: 'run',
        name: root,
        workspace: 'workspaces/w',
        agent: '',
        actor: 'you',
        task: '',
        state: 'running',
        startedAt: '2026-01-01T00:00:00.000Z',
        endedAt: null,
        durationNanos: '1',
        requiresAction: true,
      },
    ],
  }
  const requests: ActivityObservation[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn((_url, init) => {
      requests.push(JSON.parse(init.body))
      return Promise.resolve(
        new Response(`data: ${JSON.stringify({ kind: 'page', page: initial })}\n\n`, {
          headers: { 'Content-Type': 'text/event-stream' },
        }),
      )
    }),
  )
  const store = createStore()
  const state = activityObservation(initial, '/activity', '')
  const unmount = store.sub(state, () => {})
  try {
    await vi.waitFor(() => expect(store.get(state).connected).toBe(false))
    expect(store.get(state).page.rows).toEqual([])
    expect(store.get(state).roots).toEqual([root])
    await vi.advanceTimersByTimeAsync(2000)
    expect(requests[1].window.roots).toEqual([root])
  } finally {
    unmount()
  }
})

it('does not interpret a later root page at local scroll top as the global live edge', () => {
  const store = createStore()
  const state = activityObservation({ ...page, hasBefore: true }, '/activity', '?after=older')
  store.set(state, { kind: 'root', atStart: true })
  expect(store.get(state).followRoot).toBe(false)
})
it('starts navigation observation with only fresh payload and the list window', async () => {
  const requests: ActivityObservation[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn((_url, init) => {
      requests.push(JSON.parse(init.body))
      return new Promise(() => {})
    }),
  )
  const initial: ActivityPage = {
    ...page,
    hasBefore: true,
  }
  const store = createStore()
  const state = activityObservation(initial, '/activity', '')
  const unmount = store.sub(state, () => {})
  try {
    expect(store.get(state).page).toBe(initial)
    expect(requests[0].window).toEqual({ roots: [], followRoot: false })
  } finally {
    unmount()
  }
})
it('conceals rows, selectors and selected exact evidence on disconnect before reconnecting', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.reject(new Error('offline'))),
  )
  const run = {
    kind: 'run' as const,
    name: 'run',
    workspace: 'private-workspace',
    task: '',
    agent: '',
    actor: 'user',
    state: 'running' as const,
    startedAt: '1970-01-01T00:00:00.000Z',
    endedAt: null,
    durationNanos: '0',
    requiresAction: false,
  }
  const output = {
    kind: 'output' as const,
    name: 'run/events/2',
    workspace: run.workspace,
    run: run.name,
    agent: '',
    label: 'emission #2',
    eventKind: 'program.emitted',
    startedAt: run.startedAt,
  }
  const initial: ActivityPage = {
    ...page,
    rows: [run],
    trace: { root: run, children: { [run.name]: [output] }, complete: true, unavailable: [] },
    hasBefore: true,
    hasAfter: true,
    selected: output.name,
    selectedRow: output,
    detail: {
      kind: 'output',
      run: run.name,
      event: {
        kind: 'program.emitted',
        sequence: '2',
        occurredAt: output.startedAt,
        value: 'secret',
      },
    },
  }
  const store = createStore()
  const state = activityObservation(initial, '/activity', '')
  const unmount = store.sub(state, () => {})
  try {
    await vi.waitFor(() => expect(store.get(state).connected).toBe(false))
    // Only navigation state survives; nothing authorized by the lost connection does.
    expect(store.get(state).page).toEqual({
      ...initial,
      rows: [],
      trace: null,
      hasBefore: false,
      hasAfter: false,
      detail: null,
      selectedRow: null,
      unavailable: true,
      listUnavailable: true,
    })
  } finally {
    unmount()
  }
})

it('conceals the view and asks for sign in when the session ends', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      Promise.resolve(
        new Response('event: sessionExpired\ndata: {"kind":"sessionExpired"}\n\n', {
          headers: { 'Content-Type': 'text/event-stream' },
        }),
      ),
    ),
  )
  const store = createStore()
  const state = activityObservation(
    { ...page, detail: { kind: 'run' } as ActivityPage['detail'] },
    '/activity',
    '',
  )
  const unmount = store.sub(state, () => {})
  try {
    await vi.waitFor(() => expect(store.get(state).sessionExpired).toBe(true))
    expect(store.get(state)).toMatchObject({ connected: false, page: { detail: null } })
  } finally {
    unmount()
  }
})

const parentRun: ExecutionRow = {
  name: 'workspaces/w/runs/r',
  workspace: 'workspaces/w',
  agent: '',
  actor: 'you',
  task: '',
  state: 'running',
  startedAt: '2026-01-01T00:00:00Z',
  endedAt: null,
  durationNanos: '1',
  requiresAction: true,
}
const invocation = {
  ...parentRun,
  name: 'workspaces/w/invocations/i',
  run: parentRun.name,
  provider: 'p',
  tool: 'call',
  callPath: 'p.call',
  state: 'pending' as const,
  outcome: 'unknown' as const,
  dispatchedAt: null,
  waitingNanos: '1',
}
const first = 'workspaces/w/approvals/first'
const second = 'workspaces/w/approvals/second'
function review(name: string, resolved: boolean, initialRequest?: string): ActivityPage {
  return {
    rows: [],
    trace: null,
    hasBefore: false,
    hasAfter: false,
    anchorUnavailable: false,
    listUnavailable: false,
    agent: null,
    selected: invocation.name,
    selectedRow: { ...invocation, kind: 'invocation' },
    initialRequest,
    unavailable: false,
    listPath: '/activity',
    detail: {
      kind: 'invocation',
      invocation,
      arguments: '{}',
      result: { kind: 'unknown' },
      requests: [],
      requestUnavailable: false,
      request: {
        kind: 'approval',
        name,
        run: parentRun.name,
        invocation: invocation.name,
        provider: 'p',
        callPath: 'p.call',
        arguments: '{}',
        state: resolved && name === first ? 'approved' : 'pending',
        actionable: !resolved || name !== first,
        deadline: parentRun.startedAt,
        approver: '',
        decidedAt: null,
        comment: null,
      },
    },
  }
}
function Reader() {
  const reader = useActivityReader(useLoaderData<ActivityPage>())
  return (
    <div role="status">
      {reader.page.detail?.kind === 'invocation' ? reader.page.detail.request?.name : 'unavailable'}
    </div>
  )
}
it.each([false, true])(
  'pins the exact tool review without repeating loaders (from Run: %s)',
  async (fromRun) => {
    const observations: URLSearchParams[] = []
    const streams: ReadableStreamDefaultController<Uint8Array>[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url, init: RequestInit) => {
        observations.push(new URLSearchParams(JSON.parse(init.body as string).search))
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              streams.push(controller)
              init.signal!.addEventListener('abort', () => controller.close(), { once: true })
            },
          }),
          { headers: { 'Content-Type': 'text/event-stream' } },
        )
      }),
    )
    let parentReads = 0
    const reads: URL[] = []
    let resolved = false
    const router = createMemoryRouter(
      [
        {
          path: '/',
          element: <Outlet />,
          loader: () => {
            parentReads++
            return null
          },
          children: [
            {
              path: 'activity',
              element: <Reader />,
              loader: ({ request }) => {
                const url = new URL(request.url)
                reads.push(url)
                const explicit = url.searchParams.get('request')
                const name = explicit ?? (resolved ? second : first)
                return {
                  ...review(name, resolved, explicit ? undefined : name),
                  initialRecord:
                    url.searchParams.get('record') === parentRun.name ? invocation.name : undefined,
                }
              },
            },
          ],
        },
      ],
      {
        initialEntries: [
          `/activity?record=${fromRun ? parentRun.name : invocation.name}${fromRun ? `&request=${first}` : ''}`,
        ],
      },
    )
    const container = document.body.appendChild(document.createElement('div'))
    const root = createRoot(container)
    try {
      await act(async () => root.render(<RouterProvider router={router} />))
      await vi.waitFor(() =>
        expect(new URLSearchParams(router.state.location.search).get('request')).toBe(first),
      )
      expect(container.textContent).toBe(first)
      expect(reads).toHaveLength(1)
      expect(parentReads).toBe(1)
      expect(observations[0].get('request')).toBe(first)
      expect(observations[0].get('record')).toBe(invocation.name)
      expect(new URLSearchParams(router.state.location.search).get('record')).toBe(invocation.name)

      resolved = true
      await act(async () =>
        streams[0].enqueue(
          new TextEncoder().encode(
            `data: ${JSON.stringify({ kind: 'page', page: review(first, true) })}\n\n`,
          ),
        ),
      )
      expect(container.textContent).toBe(first)
      await act(async () => {
        await router.revalidate()
      })
      expect(reads).toHaveLength(2)
      expect(reads[1].searchParams.get('request')).toBe(first)
      expect(container.textContent).toBe(first)

      await act(async () => {
        await router.navigate(`/activity?record=${invocation.name}&request=${second}`)
      })
      expect(reads).toHaveLength(3)
      expect(parentReads).toBe(3)
      expect(container.textContent).toBe(second)
    } finally {
      await act(async () => root.unmount())
      router.dispose()
      container.remove()
      vi.unstubAllGlobals()
    }
  },
)

const listed = (n: number, state: ExecutionRow['state'] = 'succeeded') => ({
  ...parentRun,
  kind: 'run' as const,
  name: `workspaces/w/runs/r${n}`,
  state,
  requiresAction: false,
})
const shortNames = (value: ActivityPage) =>
  value.rows.map((row) => `${row.name.split('/').at(-1)}:${row.kind === 'run' ? row.state : ''}`)

it('drops an unavailable anchor when the first page joins the list', () => {
  const store = createStore()
  const missing = activityObservation(
    { ...page, anchorUnavailable: true },
    '/activity',
    '?anchor=gone',
  )
  const opened = store.get(
    activityObservation({ ...page, rows: [listed(1)] }, '/activity', '', store.get(missing)),
  )
  expect(opened.joined).toBe(true)
  expect(activityView(opened).anchorUnavailable).toBe(false)
  expect(opened.canFollowRoot).toBe(true)
})
it('away from the newest records, watches the running records and lets a snapshot add no arrival', async () => {
  vi.useFakeTimers()
  const streams: ReadableStreamDefaultController<Uint8Array>[] = []
  const requests: ActivityObservation[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn((_url, init) => {
      requests.push(JSON.parse(init.body))
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(c) {
              streams.push(c)
              c.enqueue(snapshot(initial))
              init.signal.addEventListener('abort', () => c.close(), { once: true })
            },
          }),
          { headers: { 'Content-Type': 'text/event-stream' } },
        ),
      )
    }),
  )
  const initial = { ...page, rows: [listed(1, 'running'), listed(2), listed(3)], hasAfter: true }
  const store = createStore()
  const state = activityObservation(initial, '/activity', '')
  const unmount = store.sub(state, () => {})
  try {
    await vi.waitFor(() => expect(requests).toHaveLength(1))
    expect(requests[0].window).toMatchObject({ followRoot: true })
    // Reading away from the newest records watches only the running one.
    store.set(state, { kind: 'root', atStart: false })
    await vi.advanceTimersByTimeAsync(120)
    expect(requests[1].window).toMatchObject({ followRoot: false, roots: [listed(1).name] })
    await act(async () =>
      streams[1].enqueue(
        new TextEncoder().encode(
          `data: ${JSON.stringify({ kind: 'page', page: { ...page, rows: [listed(0), listed(1, 'failed')] } })}\n\n`,
        ),
      ),
    )
    // The reader joins this snapshot as watched records: r1 updates, r0 does not arrive.
    const shown = shortNames(activityView(store.get(state)))
    expect(shown).toContain('r1:failed')
    expect(shown).not.toContain('r0:succeeded')
    // The ended Run leaves the window, so the stream reconnects without it.
    await vi.advanceTimersByTimeAsync(120)
    expect(requests.at(-1)!.window.roots).toEqual([])
  } finally {
    unmount()
    await vi.advanceTimersByTimeAsync(0)
  }
})

// Catches: page live mode resting on an old record while a new Task or Search arrives
// that a snapshot of only the records you watch never adds to the list.
it('loads older records through the Activity loader, keeps them on a failure, and keeps them across a navigation inside the list', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => new Promise(() => {})),
  )
  const reads: URLSearchParams[] = []
  let failing = true
  const latest = createRef<ReturnType<typeof useActivityReader>>()
  function ListReader() {
    const value = useActivityReader(useLoaderData<ActivityPage>())
    useLayoutEffect(() => {
      latest.current = value
    })
    return <output>{shortNames(value.page).join(' ')}</output>
  }
  const router = createMemoryRouter(
    [
      {
        path: '/activity',
        element: <ListReader />,
        loader: ({ request }) =>
          loadActivityPage(request, async () => {
            const query = new URL(request.url).searchParams
            reads.push(query)
            if (query.get('load') !== 'page')
              return {
                ...page,
                rows: [listed(1), listed(2)],
                hasAfter: true,
                selected: query.get('record'),
              }
            if (failing) throw new Error('offline')
            return { ...page, rows: [listed(3), listed(4)], hasBefore: true }
          }),
      },
    ],
    { initialEntries: ['/activity?expand=workspaces/w/runs/r9&tree=1&record=x'] },
  )
  const container = document.body.appendChild(document.createElement('div'))
  const root = createRoot(container)
  try {
    await act(async () => root.render(<RouterProvider router={router} />))
    expect(container.textContent).toBe('r1:succeeded r2:succeeded')
    await act(async () => latest.current!.list.older!.load())
    await vi.waitFor(() => expect(latest.current!.list.older?.failed).toBe(true))
    expect(container.textContent).toBe('r1:succeeded r2:succeeded')
    expect(Object.fromEntries(reads.at(-1)!)).toEqual({ after: listed(2).name, load: 'page' })

    failing = false
    const loaded = latest.current!.list.count
    await act(async () => latest.current!.list.older!.load())
    await vi.waitFor(() => expect(latest.current!.list.older).toBeNull())
    expect(container.textContent).toBe('r1:succeeded r2:succeeded r3:succeeded r4:succeeded')
    expect(latest.current!.list.count - loaded).toBe(2)

    const before = latest.current!.windowKey
    await act(async () => {
      await router.navigate(latest.current!.navigation.recordHref(listed(3).name))
    })
    expect(latest.current!.page.selected).toBe(listed(3).name)
    expect(container.textContent).toBe('r1:succeeded r2:succeeded r3:succeeded r4:succeeded')
    expect(latest.current!.windowKey).toBe(before)
  } finally {
    await act(async () => root.unmount())
    router.dispose()
    container.remove()
  }
})

// Catches: a page the previous list still loads blocking the next list's first load,
// as when you open another workspace's Activity while a page loads.
it('loads a page for a new list while the previous list still loads one', async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => new Promise(() => {})),
  )
  const latest = createRef<ReturnType<typeof useActivityReader>>()
  function ListReader() {
    const value = useActivityReader(useLoaderData<ActivityPage>())
    useLayoutEffect(() => {
      latest.current = value
    })
    return <output>{shortNames(value.page).join(' ')}</output>
  }
  const router = createMemoryRouter(
    [
      {
        path: '/:workspace/activity',
        element: <ListReader />,
        loader: ({ request, params }) =>
          loadActivityPage(request, async () => {
            const other = params.workspace === 'b'
            const listPath = `/${params.workspace}/activity`
            if (new URL(request.url).searchParams.get('load') !== 'page')
              return {
                ...page,
                listPath,
                rows: other ? [listed(5), listed(6)] : [listed(1), listed(2)],
                hasAfter: true,
              }
            // The first list's page never answers.
            return other
              ? { ...page, listPath, rows: [listed(7)], hasBefore: true }
              : new Promise<ActivityPage>(() => {})
          }),
      },
    ],
    { initialEntries: ['/a/activity'] },
  )
  const container = document.body.appendChild(document.createElement('div'))
  const root = createRoot(container)
  try {
    await act(async () => root.render(<RouterProvider router={router} />))
    await act(async () => latest.current!.list.older!.load())
    expect(latest.current!.list.older?.loading).toBe(true)

    await act(async () => {
      await router.navigate('/b/activity')
    })
    expect(container.textContent).toBe('r5:succeeded r6:succeeded')
    await act(async () => latest.current!.list.older!.load())
    await vi.waitFor(() =>
      expect(container.textContent).toBe('r5:succeeded r6:succeeded r7:succeeded'),
    )
  } finally {
    await act(async () => root.unmount())
    router.dispose()
    container.remove()
  }
})

it("loads more of an Agent's history through the Agent route", async () => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => new Promise(() => {})),
  )
  const agent = 'workspaces/w/agents/a'
  const paths: string[] = []
  const latest = createRef<ReturnType<typeof useActivityReader>>()
  function AgentReader() {
    const value = useActivityReader(useLoaderData<{ activity: ActivityPage }>().activity)
    useLayoutEffect(() => {
      latest.current = value
    })
    return <output>{shortNames(value.page).join(' ')}</output>
  }
  const history = { ...page, agent, listPath: `/${agent}` }
  const router = createMemoryRouter(
    [
      {
        path: '/workspaces/:workspace/agents/:agent',
        element: <AgentReader />,
        loader: ({ request }) => {
          const url = new URL(request.url)
          paths.push(url.pathname)
          // The Agent route returns its history under `activity`, beside the Agent.
          return url.searchParams.get('load') === 'page'
            ? { mode: 'current', activity: { ...history, rows: [listed(3)] } }
            : { mode: 'current', activity: { ...history, rows: [listed(1)], hasAfter: true } }
        },
      },
    ],
    { initialEntries: [`/${agent}`] },
  )
  const container = document.body.appendChild(document.createElement('div'))
  const root = createRoot(container)
  try {
    await act(async () => root.render(<RouterProvider router={router} />))
    await act(async () => latest.current!.list.older!.load())
    await vi.waitFor(() => expect(container.textContent).toBe('r1:succeeded r3:succeeded'))
    expect(paths).toEqual([`/${agent}`, `/${agent}`])
  } finally {
    await act(async () => root.unmount())
    router.dispose()
    container.remove()
  }
})

it('stays connected while scrolling past running records keeps changing the watched set', async () => {
  vi.useFakeTimers()
  let connections = 0
  vi.stubGlobal(
    'fetch',
    vi.fn((_url, init) => {
      connections++
      // Coral answers each connection with its first snapshot after 500 ms.
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(c) {
              const answer = setTimeout(
                () =>
                  c.enqueue(
                    new TextEncoder().encode(
                      `data: ${JSON.stringify({ kind: 'page', page: { ...page, rows: [] } })}\n\n`,
                    ),
                  ),
                500,
              )
              init.signal.addEventListener(
                'abort',
                () => {
                  clearTimeout(answer)
                  c.close()
                },
                { once: true },
              )
            },
          }),
          { headers: { 'Content-Type': 'text/event-stream' } },
        ),
      )
    }),
  )
  const rows = Array.from({ length: 200 }, (_, n) => listed(n, 'running'))
  const store = createStore()
  const state = activityObservation({ ...page, rows, hasAfter: true }, '/activity', '')
  const unmount = store.sub(state, () => {})
  try {
    store.set(state, { kind: 'root', atStart: false })
    // Each step brings other running records near the viewport.
    for (let step = 0; step < 100; step++) {
      store.set(state, { kind: 'visible', records: [rows[step]!.name] })
      await vi.advanceTimersByTimeAsync(200)
      expect({ step, connected: store.get(state).connected }).toEqual({ step, connected: true })
    }
    expect(connections).toBeGreaterThan(1)
  } finally {
    unmount()
    await vi.advanceTimersByTimeAsync(0)
  }
})

// Catches: a new list scrolling to the top instead of the root it opened, or a list that
// jumps to the open root again on each navigation inside it.
it('reveals the root a new list opened, and nothing on a navigation inside the list', () => {
  const root = taskRow('open')
  const initial: ActivityPage = {
    ...page,
    rows: [root],
    trace: { root, children: {}, complete: true, unavailable: [] },
  }
  function Reveal() {
    return <output>{useActivityReader(initial).revealRecord ?? 'none'}</output>
  }
  const router = createMemoryRouter([{ path: '/activity', element: <Reveal /> }], {
    initialEntries: [`/activity?root=${root.name}`],
  })
  try {
    // Server rendering runs no effects, so no observation connects.
    expect(
      renderToString(
        <Provider>
          <RouterProvider router={router} />
        </Provider>,
      ),
    ).toBe(`<output>${root.name}</output>`)
  } finally {
    router.dispose()
  }
})

// Catches: a passing fault in one snapshot's root read closing the open timeline, and with it
// the reader's zoom and collapsed rows, or a refusal leaving a timeline you can no longer read.
it.each([
  { reason: 'failed' as const, kept: true },
  { reason: 'refused' as const, kept: false },
])(
  'a snapshot whose timeline read $reason keeps the open timeline: $kept',
  async ({ reason, kept }) => {
    const root = taskRow('open')
    const trace = { root, children: {}, complete: true, unavailable: [] }
    const initial: ActivityPage = { ...page, rows: [root], trace }
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(snapshot({ ...initial, trace: null, traceUnavailable: reason }))
              },
            }),
            { headers: { 'Content-Type': 'text/event-stream' } },
          ),
        ),
      ),
    )
    const store = createStore()
    const state = activityObservation(initial, '/activity', `root=${root.name}`)
    const unmount = store.sub(state, () => {})
    try {
      await vi.waitFor(() => expect(store.get(state).page).not.toBe(initial))
      expect(store.get(state).page.trace).toEqual(kept ? trace : null)
    } finally {
      unmount()
    }
  },
)
