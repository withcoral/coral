/** @vitest-environment happy-dom */
import { act, render, screen, waitFor } from '@testing-library/react'
import { createMemoryRouter, RouterProvider, useLoaderData } from 'react-router'
import { useEffect, useMemo, useState } from 'react'
import { expect, it, onTestFinished, vi } from 'vitest'
import type { ActivityPage } from '@/views/activity/types'
import { loadActivityPage } from './activity-page-fetch'
import { traceChildren, traceProgress, type ActivityTraceStore } from './activity-trace'
import {
  TRACE_REFRESH_INTERVAL_MS,
  TRACE_REVALIDATE_INTERVAL_MS,
  useActivityTrace,
} from './activity-trace-reader'
import { call, coral, runRow, task, type Row } from './activity-trace.test-support'

/**
 * Mounts a trace reader on a route whose loader is `source`, as the Activity route is. The
 * first page lists the root alone, as a list that has not expanded it. `evidence` stands
 * for a live snapshot.
 */
function mount(
  source: ReturnType<typeof coral>,
  first: string | null = task.name,
  {
    listed,
    answerMs,
  }: {
    /** What the list itself expands, so its page holds those windows. */
    listed?: string[]
    /** How long each answer takes. Coral gives it when the read arrives. */
    answerMs?: number
  } = {},
) {
  let report!: () => void
  let open!: (root: string) => void
  let conceal!: (hidden: boolean) => void
  let snapshot!: () => void
  let repage!: () => void
  const list = () =>
    source.load(new URLSearchParams((listed ?? []).map((name) => ['expand', name])))
  // The reads the test holds in flight until it lets them answer.
  let gate: {
    when: (query: URLSearchParams) => boolean
    open: Promise<void>
    answered: boolean
  } | null = null
  const shown: { reader: ReturnType<typeof useActivityTrace> } = { reader: null }
  const requests: URLSearchParams[] = []
  function Reader() {
    const loaded = useLoaderData<ActivityPage>()
    const [evidence, setEvidence] = useState(0)
    const [root, setRoot] = useState(first)
    const [hidden, setHidden] = useState(false)
    const [pushed, setPushed] = useState<ActivityPage | null>(null)
    useEffect(() => {
      report = () => setEvidence((value) => value + 1)
      open = setRoot
      conceal = setHidden
      snapshot = () => {
        setPushed(list())
        setEvidence((value) => value + 1)
      }
      repage = () => setPushed(list())
    }, [])
    const shownPage = pushed ?? loaded
    // Observation lost its authority: the page hides its list.
    const page = useMemo(
      () => (hidden ? { ...shownPage, rows: [], branches: {}, listUnavailable: true } : shownPage),
      [shownPage, hidden],
    )
    const reader = useActivityTrace(page, root, evidence)
    useEffect(() => {
      shown.reader = reader
    })
    const progress = reader && traceProgress(reader.store)
    return (
      <output>
        {reader?.store.failed
          ? 'failed'
          : progress
            ? `${progress.loaded}/${progress.total}${progress.complete ? ' complete' : ''}`
            : 'none'}
      </output>
    )
  }
  const router = createMemoryRouter(
    [
      {
        path: '/workspaces/finance/activity',
        Component: Reader,
        loader: ({ request }) => {
          const query = new URL(request.url).searchParams
          if (query.get('load') !== 'page') return list()
          requests.push(query)
          return loadActivityPage(request, async () => {
            if (answerMs) {
              const slow = source.load(query)
              await new Promise((resolve) => setTimeout(resolve, answerMs))
              return slow
            }
            const held = gate?.when(query) ? gate : null
            // An answer that Coral gave before the wait holds what Coral held then.
            const answer = held?.answered ? source.load(query) : null
            if (held) await held.open
            return answer ?? source.load(query)
          })
        },
      },
    ],
    { initialEntries: ['/workspaces/finance/activity'] },
  )
  onTestFinished(() => router.dispose())
  render(<RouterProvider router={router} />)
  return {
    requests,
    router,
    reader: () => shown.reader!,
    /** Later evidence arrives, as a live snapshot does. */
    report: () => act(async () => report()),
    /** A live snapshot arrives: the list's page as Coral holds it now, which is the evidence. */
    snapshot: () => act(async () => snapshot()),
    /** The list's page changes for another record, with no evidence for this root. */
    repage: () => act(async () => repage()),
    /** You open another top-level record. */
    open: (root: string) => act(async () => open(root)),
    /** The page is concealed, as after observation lost its authority, or shows again. */
    conceal: (hidden: boolean) => act(async () => conceal(hidden)),
    /**
     * Holds the reads `when` picks in flight until the returned function lets them answer.
     * Arm it before the read it holds can start. With `answered`, Coral gives its answer
     * when the read arrives, and the answer is what waits.
     */
    hold: (when: (query: URLSearchParams) => boolean, answered = false) => {
      let release!: () => void
      gate = { when, answered, open: new Promise((resolve) => (release = resolve)) }
      return () => {
        gate = null
        release()
      }
    },
    shows: (text: string) =>
      waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(text)),
  }
}

it('reads a whole root through the route loader, window by window', async () => {
  const [a, b] = [runRow('a'), runRow('b')]
  const source = coral({
    [task.name]: [a, b],
    [a.name]: Array.from({ length: 250 }, (_, index) => call(a, index)),
    [b.name]: [call(b, 0)],
  })
  const view = mount(source)
  await view.shows('253/253 complete')
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toHaveLength(250))
  // The root, then both runs, then the long run's two further windows.
  expect(view.requests.map((query) => query.getAll('expand').length)).toEqual([1, 3, 2, 2])
  expect(view.requests.every((query) => query.get('anchor') === task.name)).toBe(true)
})

it('reads the root you open while a read of the root you left is in flight', async () => {
  const other: Row<'task'> = { ...task, name: `${task.name}-other`, intent: 'Other goal' }
  const [a, b] = [runRow('a'), { ...runRow('b'), task: other.name }]
  const source = coral(
    {
      [task.name]: [a],
      [a.name]: Array.from({ length: 450 }, (_, index) => call(a, index)),
      [other.name]: [b],
      [b.name]: [{ ...call(b, 0), task: other.name }],
    },
    [task, other],
  )
  const view = mount(source)
  // Armed before any read can ask for the Run's second window, which it holds.
  const release = view.hold((query) => query.get(`offset.${a.name}`) === '100')
  await view.shows('101/451')
  await waitFor(() => expect(view.requests.at(-1)!.get(`offset.${a.name}`)).toBe('100'))
  // The answer for the first root lands only after you opened the second.
  await view.open(other.name)
  await act(async () => release())
  await view.shows('2/2 complete')
  expect(view.reader().store.root).toBe(other.name)
  // The answer you no longer need joined nothing.
  expect(traceChildren(view.reader().store, a.name)).toEqual([])
})

it('reads nothing without a root', async () => {
  const view = mount(coral({ [task.name]: [] }), null)
  await view.shows('none')
  expect(view.requests).toEqual([])
})

it('says a read failed, stops, and reads again on a retry', async () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const load = source.load
  let fail = true
  source.load = (query) => {
    if (fail && query.has('load')) throw new Error('Coral is unreachable')
    return load(query)
  }
  const view = mount(source)
  await view.shows('failed')
  const failed = view.requests.length
  await act(async () => {})
  expect(view.requests).toHaveLength(failed)
  fail = false
  await act(async () => view.reader().retry())
  await view.shows('2/2 complete')
})

it('reads one tail for later evidence, and at most once per interval', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const live = runRow('live', 'running')
  const entries = [call(live, 0), call(live, 1, false)]
  const source = coral({ [open.name]: [live], [live.name]: entries }, open)
  const view = mount(source, open.name)
  await view.shows('3/3 complete')
  const before = view.requests.length

  // The first evidence reads at once: one request for the running Run's tail.
  entries[1] = call(live, 1)
  entries.push(call(live, 2, false))
  await view.report()
  await view.shows('4/4 complete')
  expect(view.requests).toHaveLength(before + 1)
  expect(view.requests.at(-1)!.get(`offset.${live.name}`)).toBe('1')

  // A burst of evidence inside the interval costs one more read, after the interval.
  entries.push(call(live, 3, false))
  await view.report()
  await view.report()
  await view.report()
  await act(async () => {})
  expect(view.requests).toHaveLength(before + 1)
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REFRESH_INTERVAL_MS)
  })
  await view.shows('5/5 complete')
  expect(view.requests).toHaveLength(before + 2)
})

// Catches: an answer that Coral gave before later evidence, and that joins after it, taken
// as a read of that evidence. The trace then held the older records, said that it was
// complete, and asked for nothing until more evidence came.
it('reads again what evidence changed while an older answer was on its way', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const live = runRow('live', 'running')
  const entries = [call(live, 0), call(live, 1, false)]
  const view = mount(coral({ [open.name]: [live], [live.name]: entries }, open), open.name)
  await view.shows('3/3 complete')
  const before = view.requests.length

  // Evidence starts a read of the Run's tail. Coral answers it now, and the answer is slow.
  entries[1] = call(live, 1)
  entries.push(call(live, 2, false))
  const release = view.hold((query) => query.has(`offset.${live.name}`), true)
  await view.report()
  await waitFor(() => expect(view.requests).toHaveLength(before + 1))
  // Coral records one more call, and its evidence arrives before that answer does.
  entries.push(call(live, 3, false))
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REFRESH_INTERVAL_MS)
  })
  await view.report()
  await act(async () => release())
  // The answer holds three calls. With the next mark, the trace reads the tail once more
  // for the fourth.
  await advance(TRACE_REFRESH_INTERVAL_MS)
  await view.shows('5/5 complete')
  expect(view.requests).toHaveLength(before + 2)
})

it('reads nothing for later evidence once the root and its runs have ended', async () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const view = mount(source)
  await view.shows('2/2 complete')
  const before = view.requests.length
  await view.report()
  await act(async () => {})
  expect(view.requests).toHaveLength(before)
})

it('shows and reads nothing while the page is concealed', async () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const load = source.load
  // Observation lost its authority: the page hides its list.
  source.load = (query) => ({ ...load(query), rows: [], branches: {}, listUnavailable: true })
  const view = mount(source)
  await view.shows('none')
  await act(async () => {})
  expect(view.requests).toEqual([])
})

// Catches: records from before the page was hidden shown again when it returns, with no
// read. Whatever hid the page can have ended your access to them.
it('forgets what it read while the page is concealed, and reads it again when the page returns', async () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const load = source.load
  const view = mount(source)
  await view.shows('2/2 complete')
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toHaveLength(1))

  let hidden = true
  source.load = (query) =>
    hidden ? { ...load(query), rows: [], branches: {}, listUnavailable: true } : load(query)
  await act(async () => view.router.revalidate())
  await view.shows('none')
  const before = view.requests.length
  await act(async () => {})
  expect(view.requests).toHaveLength(before)

  hidden = false
  await act(async () => view.router.revalidate())
  await view.shows('2/2 complete')
  // The trace started over: it asked for the root alone first, as for a root it never read,
  // and then for the Run.
  const again = view.requests.slice(before).map((query) => query.getAll('expand'))
  expect(again).toContainEqual([task.name])
  expect(again.at(-1)).toContain(a.name)
  expect(view.reader().restoring).toBe(false)
})

// Catches: an ended root read once and then never again, so a Run that Coral stops
// returning stays on screen for the whole visit; a first check that waits for a second
// event after a quiet time; or a check with every snapshot, which costs a full read each.
it('reads an ended root once more on the first evidence a minute after it read it, and drops what Coral withholds', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const [a, b] = [runRow('a'), runRow('b')]
  const children: Parameters<typeof coral>[0] = {
    [task.name]: [a, b],
    [a.name]: [call(a, 0)],
    [b.name]: [call(b, 0)],
  }
  const view = mount(coral(children))
  await view.shows('4/4 complete')
  await act(async () => {})
  const before = view.requests.length

  // Evidence within the minute reads nothing: nothing under an ended root changes.
  await view.report()
  await act(async () => {})
  expect(view.requests).toHaveLength(before)

  // Ten quiet minutes, in which Coral stops returning one Run. The first evidence after
  // them reads everything once, in one request, and that Run's records go.
  children[a.name] = 'unavailable'
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10 * TRACE_REVALIDATE_INTERVAL_MS)
  })
  expect(view.requests).toHaveLength(before)
  await view.report()
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toEqual([]))
  expect(traceChildren(view.reader().store, b.name)).toHaveLength(1)
  expect(view.requests).toHaveLength(before + 1)
  expect(view.requests.at(-1)!.getAll('expand')).toEqual([task.name, a.name, b.name])
  // Evidence right after reads nothing again.
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REFRESH_INTERVAL_MS)
  })
  await view.report()
  await act(async () => {})
  expect(view.requests).toHaveLength(before + 1)

  // Coral returns the Run again. The next check, a minute later, asks for it once more.
  children[a.name] = [call(a, 0)]
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REVALIDATE_INTERVAL_MS)
  })
  await view.report()
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toHaveLength(1))
  await view.shows('4/4 complete')
})

// Catches: the minute counted from the first evidence, not from the read. The first event
// after any quiet time then checked nothing, and only a second one a minute later did.
it('revalidates on the very first evidence when it comes a minute after the read', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const a = runRow('a')
  const children: Parameters<typeof coral>[0] = { [task.name]: [a], [a.name]: [call(a, 0)] }
  const view = mount(coral(children))
  await view.shows('2/2 complete')
  await act(async () => {})
  const before = view.requests.length
  // No evidence at all for ten minutes, in which Coral stops returning the Run.
  children[a.name] = 'unavailable'
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10 * TRACE_REVALIDATE_INTERVAL_MS)
  })
  expect(view.requests).toHaveLength(before)
  await view.report()
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toEqual([]))
  expect(view.requests).toHaveLength(before + 1)
})

// Catches: an answer that Coral gave before the page was concealed joining the trace that
// reads after it shows again. Whatever concealed the page can have ended your access.
it('drops the answer to a read from before the page was concealed', async () => {
  const a = runRow('a')
  const source = coral({
    [task.name]: [a],
    [a.name]: Array.from({ length: 250 }, (_, index) => call(a, index)),
  })
  const view = mount(source)
  const release = view.hold((query) => query.get(`offset.${a.name}`) === '100')
  await waitFor(() => expect(view.requests.at(-1)?.get(`offset.${a.name}`)).toBe('100'))
  const held = view.requests.length

  await view.conceal(true)
  await view.shows('none')
  await view.conceal(false)
  // The read from before is still in flight, so the trace that shows now waits its turn.
  expect(view.reader().restoring).toBe(true)
  expect(traceChildren(view.reader().store, a.name)).toEqual([])
  await act(async () => release())
  await view.shows('251/251 complete')
  // Its answer joined nothing: the trace asked for the root first, then for every window
  // of the Run, also the one that answer held.
  const after = view.requests.slice(held)
  expect(after[0]!.getAll('expand')).toEqual([task.name])
  expect(after.map((query) => query.get(`offset.${a.name}`)).filter(Boolean)).toEqual([
    '0',
    '100',
    '200',
  ])
  expect(view.reader().restoring).toBe(false)
})

// Catches: the minute counted from the first trace the reader held, not from the one it
// holds now. A trace that just read everything then read it all again with the first
// evidence, or a later trace never checked anything.
it.each([
  ['a root you open', 'open'],
  ['a page that shows again after it was concealed', 'conceal'],
] as const)('counts the minute to the next check from the first read of %s', async (_case, how) => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const other: Row<'task'> = { ...task, name: `${task.name}-other`, intent: 'Other goal' }
  const [a, b] = [runRow('a'), { ...runRow('b'), task: other.name }]
  const source = coral(
    {
      [task.name]: [a],
      [a.name]: [call(a, 0)],
      [other.name]: [b],
      [b.name]: [{ ...call(b, 0), task: other.name }],
    },
    [task, other],
  )
  const view = mount(source)
  await view.shows('2/2 complete')
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REVALIDATE_INTERVAL_MS - 10_000)
  })
  if (how === 'open') await view.open(other.name)
  else {
    await view.conceal(true)
    await view.shows('none')
    await view.conceal(false)
  }
  // The trace that reads now holds its Run's records: its first read is over.
  const run = how === 'open' ? b : a
  await waitFor(() => expect(traceChildren(view.reader().store, run.name)).toHaveLength(1))
  await act(async () => {})
  const before = view.requests.length

  // More than a minute after the first trace started, and well within the minute of this one.
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20_000)
  })
  await view.report()
  await act(async () => {})
  expect(view.requests).toHaveLength(before)

  // A minute after this trace started, the first evidence reads everything once.
  await act(async () => {
    await vi.advanceTimersByTimeAsync(TRACE_REVALIDATE_INTERVAL_MS - 15_000)
  })
  await view.report()
  await waitFor(() => expect(view.requests).toHaveLength(before + 1))
})

const usesFakeClock = () => {
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
}
/** Time passes in short steps, so each answer joins before the next read starts. */
async function advance(milliseconds: number, step = 250) {
  for (let left = milliseconds; left > 0; left -= step)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(Math.min(left, step))
    })
}
/**
 * Evidence every two seconds for `seconds`, as from a root whose work goes on. `each` runs
 * before each one, with the time and what the trace holds then.
 */
async function evidenceFor(
  view: ReturnType<typeof mount>,
  seconds: number,
  each: (second: number, store: ActivityTraceStore) => void,
) {
  for (let second = 0; second < seconds; second += 2) {
    each(second, view.reader().store)
    await view.report()
    await advance(TRACE_REFRESH_INTERVAL_MS)
  }
}
const openTask: Row<'task'> = { ...task, outcome: 'open', endedAt: null }

// Catches: the same first ten changing Runs read with every request, because evidence
// marks them all again before each slow answer. The Runs after them then showed calls
// minutes late, or never.
it.each([
  ['2.1 s', 2_100, 6],
  ['0.35 s', 350, 1],
])(
  'reads each of twelve runs that run in its turn when an answer takes %s',
  async (_time, answerMs, bound) => {
    usesFakeClock()
    const runs = Array.from({ length: 40 }, (_, index) =>
      runRow(`r${index}`, index % 3 === 0 && index < 36 ? 'running' : 'succeeded'),
    )
    const running = runs.filter((run) => run.state === 'running')
    const tails = new Map(running.map((run) => [run.name, [call(run, 0)]]))
    const source = coral(
      {
        [openTask.name]: runs,
        ...Object.fromEntries(runs.map((run) => [run.name, tails.get(run.name) ?? [call(run, 0)]])),
      },
      openTask,
    )
    const view = mount(source, openTask.name, { answerMs })
    await advance(30_000)
    await view.shows('80/80 complete')
    const behind = new Map<string, number>()
    await evidenceFor(view, 240, (_second, store) => {
      for (const [name, tail] of tails) {
        const late = tail.length - traceChildren(store, name).length
        behind.set(name, Math.max(behind.get(name) ?? 0, late))
        // Each Run that runs makes a call every two seconds.
        tail.push(call(runRow(name.split('/').at(-1)!, 'running'), tail.length))
      }
    })
    // No Run waits longer than the others. A request reads ten of the twelve, so at 2.1 s an
    // answer a Run is a few calls behind; these are this fake's values, not a promise.
    expect(Math.max(...behind.values())).toBeLessThanOrEqual(bound)
  },
  60_000,
)

// Catches: a check that never ends, and so never starts again, because evidence marks two
// windows of the Task again before each slow answer; and a Run or a root that Coral stops
// returning, or returns again, going unnoticed meanwhile.
it('ends each check while slow answers and evidence never stop, and follows what Coral withholds and returns', async () => {
  usesFakeClock()
  const runs = Array.from({ length: 60 }, (_, index) =>
    runRow(`r${index}`, index === 3 ? 'running' : 'succeeded'),
  )
  const live = runs[3]!
  const tail = [call(live, 0)]
  const lost = runs[50]!
  const children: Parameters<typeof coral>[0] = {
    [openTask.name]: runs,
    ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
    [live.name]: tail,
  }
  const other: Row<'task'> = { ...task, name: `${task.name}-other`, intent: 'Other goal' }
  const b = { ...runRow('b'), task: other.name }
  children[other.name] = [b]
  children[b.name] = [{ ...call(b, 0), task: other.name }]
  const source = coral(children, [openTask, other])
  const load = source.load
  let rootWithheld = false
  source.load = (query) =>
    rootWithheld && query.get('anchor') === openTask.name
      ? { ...load(query), rows: [], branches: {}, anchorUnavailable: true }
      : load(query)
  const view = mount(source, openTask.name, { answerMs: 2_100 })
  await advance(40_000)
  await view.shows('120/120 complete')

  let behind = 0
  const seen: Record<string, number> = {}
  await evidenceFor(view, 660, (at, store) => {
    if (!store.unavailable && at < 420)
      behind = Math.max(behind, tail.length - traceChildren(store, live.name).length)
    tail.push(call(live, tail.length))
    // Coral stops returning a Run, then returns it. Later it does the same with the root.
    if (at === 120) children[lost.name] = 'unavailable'
    if (at === 260) children[lost.name] = [call(lost, 0)]
    if (at === 420) rootWithheld = true
    if (at === 540) rootWithheld = false
    const held = traceChildren(store, lost.name).length
    if (at > 120 && at <= 260 && !held) seen.withheld ??= at
    if (at > 260 && at <= 420 && held) seen.returned ??= at
    if (at > 420 && at <= 540 && store.unavailable) seen.rootWithheld ??= at
    if (at > 540 && !store.unavailable && traceChildren(store, live.name).length)
      seen.rootReturned ??= at
  })
  // Each change shows with the check after it: a minute, and the time the check takes.
  expect(seen.withheld).toBeLessThanOrEqual(120 + 120)
  expect(seen.returned).toBeLessThanOrEqual(260 + 120)
  expect(seen.rootWithheld).toBeLessThanOrEqual(420 + 120)
  expect(seen.rootReturned).toBeLessThanOrEqual(540 + 120)
  // The live tail is never further behind than a mark and a few requests take.
  expect(behind).toBeLessThanOrEqual(4)
  // Another root you open reads at once, whatever the first was doing.
  await view.open(other.name)
  await advance(10_000)
  await view.shows('2/2 complete')
}, 120_000)

// Catches: an answer that Coral gave before a Run ended overwriting the list's newer
// snapshot of that Run, which the list page itself holds. The trace then showed the Run and
// its calls in progress for good, and asked for nothing.
it.each([
  ['joins before the next mark of evidence', 'answer first'],
  ['joins after the next mark of evidence', 'mark first'],
] as const)(
  'keeps what a newer list snapshot says when an older answer %s',
  async (_case, order) => {
    usesFakeClock()
    const live = runRow('live', 'running')
    const entries = [call(live, 0), call(live, 1, false), call(live, 2, false)]
    const children: Parameters<typeof coral>[0] = { [openTask.name]: [live], [live.name]: entries }
    const view = mount(coral(children, openTask), openTask.name, {
      listed: [openTask.name, live.name],
    })
    await view.shows('4/4 complete')
    await act(async () => {})
    const state = () => {
      const { store } = view.reader()
      return {
        run: traceChildren(store, openTask.name).map((row) => row.kind === 'run' && row.state),
        pending: traceChildren(store, live.name).filter(
          (row) => row.kind === 'invocation' && row.state !== 'ended',
        ).length,
      }
    }
    expect(state()).toEqual({ run: ['running'], pending: 2 })

    // A check is due. Coral answers its read now, and the answer is slow.
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    const release = view.hold((query) => query.has(`offset.${live.name}`), true)
    const before = view.requests.length
    await view.report()
    await waitFor(() => expect(view.requests).toHaveLength(before + 1))
    // The Run ends. The list's snapshot holds it and its calls, and is the evidence.
    children[openTask.name] = [runRow('live')]
    children[live.name] = entries.map((_entry, index) => call(live, index))
    await view.snapshot()
    await waitFor(() => expect(state()).toEqual({ run: ['succeeded'], pending: 0 }))
    if (order === 'mark first') await advance(TRACE_REFRESH_INTERVAL_MS)
    await act(async () => release())
    await advance(3 * TRACE_REFRESH_INTERVAL_MS)
    await waitFor(() => expect(state()).toEqual({ run: ['succeeded'], pending: 0 }))
    expect(traceProgress(view.reader().store)).toMatchObject({ complete: true, stale: false })
  },
  30_000,
)

// Catches: every answer taken as late while the list's page, or evidence, keeps changing,
// so each parent read only its first changeable window again and again, at the speed of
// the answers. A Run that ended in a later window of the Task then showed as running until
// the next check, and the reads never paused.
it.each([
  ['the list changes each 250 ms and an answer takes 0.35 s', 350, 250, 6, 75],
  ['the list changes each second and an answer takes 0.35 s', 350, 1_000, 6, 75],
  ['the list changes each 250 ms and an answer takes 2.1 s', 2_100, 250, 12, 32],
  ['evidence alone comes and an answer takes 2.1 s', 2_100, null, 12, 32],
] as const)(
  'reads every changeable window of a task in turn, at a bounded rate, while %s',
  async (_case, answerMs, pageMs, withinSeconds, mostRequests) => {
    usesFakeClock()
    const runs = Array.from({ length: 150 }, (_, index) =>
      runRow(`r${index}`, index === 3 || index === 130 ? 'running' : 'succeeded'),
    )
    const children: Parameters<typeof coral>[0] = {
      [openTask.name]: runs,
      ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
    }
    const tail = children[runs[3]!.name] as Row<'invocation'>[]
    const view = mount(coral(children, openTask), openTask.name, { answerMs })
    await advance(60_000)
    await view.shows('300/300 complete')
    const state = () =>
      traceChildren(view.reader().store, openTask.name).flatMap((row) =>
        row.kind === 'run' && row.name === runs[130]!.name ? [row.state] : [],
      )[0]
    expect(state()).toBe('running')
    const before = view.requests.length
    let ended: number | undefined
    for (let time = 0; time < 60_000; time += 250) {
      // One Run makes a call each two seconds. The other, in the Task's second window, ends.
      if (time % 2_000 === 0) tail.push(call(runs[3]!, tail.length))
      if (time === 10_000)
        children[openTask.name] = runs.map((run, index) => (index === 130 ? runRow('r130') : run))
      if (pageMs ? time % pageMs === 0 : time % 2_000 === 0)
        await (pageMs ? view.snapshot() : view.report())
      await advance(250)
      if (time >= 10_000 && state() === 'succeeded') ended ??= time
    }
    // The end shows after a mark and a few answers, not with the next check.
    expect(ended).toBeLessThanOrEqual(10_000 + withinSeconds * 1_000)
    // The reads keep to the rate of the marks, however often the list changes.
    expect(view.requests.length - before).toBeLessThanOrEqual(mostRequests)
    // The Run that runs stays current meanwhile.
    expect(
      tail.length - traceChildren(view.reader().store, runs[3]!.name).length,
    ).toBeLessThanOrEqual(4)
  },
  120_000,
)

// Catches: a Run that the list's snapshot says Coral withholds coming back from an answer
// that Coral gave before, with its records shown as final, for as long as nothing else
// changes.
it.each([
  ['an ended task, with no more evidence', false],
  ['an open task, with evidence each two seconds', true],
] as const)(
  'never shows again what a snapshot withholds when an older answer joins after it: %s',
  async (_case, evidence) => {
    usesFakeClock()
    const root = evidence ? openTask : task
    const [a, b] = [runRow('a'), runRow('b')]
    const records = [call(a, 0), call(a, 1), call(a, 2)]
    const children: Parameters<typeof coral>[0] = {
      [root.name]: [a, b],
      [a.name]: records,
      [b.name]: [call(b, 0)],
    }
    const source = coral(children, root)
    const view = mount(source, root.name, { listed: [root.name, a.name] })
    await view.shows('6/6 complete')
    await act(async () => {})
    const held = () => traceChildren(view.reader().store, a.name).length
    expect(held()).toBe(3)

    // A check is due. Coral answers its read now, and the answer is slow.
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    const release = view.hold((query) => query.has(`offset.${a.name}`), true)
    const before = view.requests.length
    await view.report()
    await waitFor(() => expect(view.requests).toHaveLength(before + 1))
    // Coral withholds the Run. The list's snapshot says so.
    children[a.name] = 'unavailable'
    await view.snapshot()
    await waitFor(() => expect(held()).toBe(0))
    // The older answer joins. Nothing of the Run shows, then or later.
    await act(async () => release())
    expect(held()).toBe(0)
    for (let second = 0; second < 50; second += 2) {
      if (evidence) await view.report()
      await advance(TRACE_REFRESH_INTERVAL_MS)
      expect(held()).toBe(0)
    }
    expect(traceProgress(view.reader().store)).toMatchObject({ unavailable: true })
    // A read that fails changes nothing of that, and a retry asks Coral, which still withholds.
    const load = source.load
    source.load = (query) => {
      if (query.has('load')) throw new Error('Coral is unreachable')
      return load(query)
    }
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    await view.report()
    await view.shows('failed')
    expect(held()).toBe(0)
    source.load = load
    await act(async () => view.reader().retry())
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    expect(held()).toBe(0)
    // Coral returns the Run. The next check asks, after the snapshot, and its answer shows.
    children[a.name] = records
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    await view.report()
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    await waitFor(() => expect(held()).toBe(3))
  },
  60_000,
)

// Catches: reads that go on for good after the evidence stops. A mark that only repeated a
// late answer flagged the next read as late too, so each mark made the next one for as long
// as one pass over the changeable windows took longer than the time between two marks.
it.each([
  ['70 of 80 runs run', 80, (index: number) => index < 70, 350],
  ['45 of 80 runs run and answers take 0.5 s', 80, (index: number) => index < 45, 500],
  [
    '6 of 600 runs run, one in each window of the task',
    600,
    (index: number) => index % 100 === 50,
    350,
  ],
  ['12 of 301 runs run', 301, (index: number) => index % 25 === 3, 600],
] as const)(
  'stops reading soon after the evidence stops: %s',
  async (_case, count, runs, answerMs) => {
    usesFakeClock()
    const rows = Array.from({ length: count }, (_, index) =>
      runRow(`r${index}`, runs(index) ? 'running' : 'succeeded'),
    )
    const source = coral(
      {
        [openTask.name]: rows,
        ...Object.fromEntries(rows.map((run) => [run.name, [call(run, 0)]])),
      },
      openTask,
    )
    const view = mount(source, openTask.name, { answerMs })
    await advance(90_000)
    await view.shows(`${2 * count}/${2 * count} complete`)
    const before = view.requests.length
    // Two reports of evidence a second apart, then nothing: the Runs wait, as for an approval.
    await view.report()
    await advance(1_000)
    await view.report()
    await advance(59_000)
    const settled = view.requests.length
    await advance(120_000)
    // One pass over what can change, and a second for the late answers of the first.
    expect(settled - before).toBeLessThanOrEqual(2 * Math.ceil(count / 10) + 10)
    expect(view.requests.length - settled).toBe(0)
    expect(traceProgress(view.reader().store)).toMatchObject({ complete: true, stale: false })
  },
  180_000,
)

// Catches: a read for every answer while the list's page keeps changing, with no evidence
// for this root: each answer is late, and a late answer marked what can change at once.
it('reads at the rate of the marks while the list alone keeps changing', async () => {
  usesFakeClock()
  const runs = Array.from({ length: 40 }, (_, index) =>
    runRow(`r${index}`, index === 3 ? 'running' : 'succeeded'),
  )
  const source = coral(
    {
      [openTask.name]: runs,
      ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
    },
    openTask,
  )
  const view = mount(source, openTask.name, { answerMs: 350 })
  await advance(30_000)
  await view.shows('80/80 complete')
  const before = view.requests.length
  await view.report()
  for (let time = 0; time < 60_000; time += 250) {
    await view.repage()
    await advance(250)
  }
  // One read with each mark, a mark each two seconds: not one read after another.
  expect(view.requests.length - before).toBeLessThanOrEqual(35)
  // The list comes to rest: so do the reads.
  await advance(10_000)
  const rested = view.requests.length
  await advance(60_000)
  expect(view.requests.length).toBe(rested)
})

// Catches: a Run that the trace already holds as withheld coming back for good when Coral
// returns it for one answer and withholds it again. The list's snapshot then repeats what
// the trace holds, and it still is later news than the read that left before it.
it.each([
  ['you stay on the root', 'stay'],
  ['you open another root and come back', 'switch'],
  ['the page is concealed and shows again', 'conceal'],
] as const)(
  'never shows a run that was withheld, returned for one answer and withheld again: %s',
  async (_case, how) => {
    usesFakeClock()
    const other: Row<'task'> = { ...task, name: `${task.name}-other`, intent: 'Other goal' }
    const [a, b, c] = [runRow('a'), runRow('b'), { ...runRow('c'), task: other.name }]
    const records = [call(a, 0), call(a, 1), call(a, 2)]
    const children: Parameters<typeof coral>[0] = {
      [task.name]: [a, b],
      [a.name]: 'unavailable',
      [b.name]: [call(b, 0)],
      [other.name]: [c],
      [c.name]: [{ ...call(c, 0), task: other.name }],
    }
    const source = coral(children, [task, other])
    const view = mount(source, task.name, { listed: [task.name, a.name] })
    await view.shows('3/3')
    await act(async () => {})
    const held = () =>
      view.reader()?.store.root === task.name
        ? traceChildren(view.reader().store, a.name).length
        : 0
    expect(held()).toBe(0)

    // A check is due, and Coral returns the Run when its read arrives. The answer is slow.
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    children[a.name] = records
    const release = view.hold((query) => query.has(`offset.${a.name}`), true)
    const before = view.requests.length
    await view.report()
    await waitFor(() => expect(view.requests).toHaveLength(before + 1))
    // Coral withholds the Run again. The list's snapshot says what the trace holds already.
    children[a.name] = 'unavailable'
    await view.snapshot()
    expect(held()).toBe(0)
    if (how === 'switch') await view.open(other.name)
    if (how === 'conceal') await view.conceal(true)
    await act(async () => release())
    if (how === 'switch') await view.open(task.name)
    if (how === 'conceal') await view.conceal(false)
    expect(held()).toBe(0)
    // Nothing more changes: nothing of the Run shows, however long you stay.
    for (let second = 0; second < 40; second += 2) {
      await advance(TRACE_REFRESH_INTERVAL_MS)
      expect(held()).toBe(0)
    }
    // A read that fails changes nothing of that, nor does its retry while Coral withholds.
    const load = source.load
    source.load = (query) => {
      if (query.has('load')) throw new Error('Coral is unreachable')
      return load(query)
    }
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    await view.report()
    await view.shows('failed')
    expect(held()).toBe(0)
    source.load = load
    await act(async () => view.reader().retry())
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    expect(held()).toBe(0)
    // Coral returns the Run for good. A check that leaves now is later than every snapshot.
    children[a.name] = records
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    await view.report()
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    await waitFor(() => expect(held()).toBe(3))
  },
  60_000,
)

// Catches: a Run hidden for good, or until the next check, because an answer that withheld
// it joined after a newer snapshot of the list showed it readable. The trace hides it at
// once, as a withholding answer asks, then asks Coral once more, and that answer decides.
it.each([
  ['an ended task, with no more evidence', false, true],
  ['an open task, with evidence each two seconds', true, true],
  ['an ended task, while Coral goes on withholding it', false, false],
] as const)(
  'asks once more for a run that an older answer withholds and a newer snapshot shows: %s',
  async (_case, evidence, granted) => {
    usesFakeClock()
    const root = evidence ? openTask : task
    const [a, b] = [runRow('a'), runRow('b')]
    const records = [call(a, 0), call(a, 1), call(a, 2)]
    const children: Parameters<typeof coral>[0] = {
      [root.name]: [a, b],
      [a.name]: records,
      [b.name]: [call(b, 0)],
    }
    const source = coral(children, root)
    const view = mount(source, root.name, { listed: [root.name, a.name] })
    await view.shows('6/6 complete')
    await act(async () => {})
    const held = () => traceChildren(view.reader().store, a.name).length
    expect(held()).toBe(3)

    // A check is due. Coral withholds the Run when its read arrives; the answer is slow.
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    const release = view.hold((query) => query.has(`offset.${a.name}`), true)
    children[a.name] = 'unavailable'
    const before = view.requests.length
    await view.report()
    await waitFor(() => expect(view.requests).toHaveLength(before + 1))
    // Coral returns the Run, and the list's later snapshot holds its records.
    children[a.name] = records
    await view.snapshot()
    expect(held()).toBe(3)
    if (!granted) children[a.name] = 'unavailable'
    // The older answer joins: the Run hides, and one more read leaves at once. It is held
    // here, so the Run is seen hidden while that read is on its way.
    let decide!: () => void
    await act(async () => {
      release()
      decide = view.hold((query) => query.has(`offset.${a.name}`))
    })
    await waitFor(() => expect(view.requests).toHaveLength(before + 2))
    expect(view.requests.at(-1)!.has(`offset.${a.name}`)).toBe(true)
    expect(held()).toBe(0)
    await act(async () => decide())
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    // Its answer decides: the Run shows when Coral grants it, and stays hidden when not.
    expect(held()).toBe(granted ? 3 : 0)
    // Then nothing more, whether evidence comes or not, and no read loop.
    const settled = view.requests.length
    for (let second = 0; second < 40; second += 2) {
      if (evidence) await view.report()
      await advance(TRACE_REFRESH_INTERVAL_MS)
      expect(held()).toBe(granted ? 3 : 0)
    }
    expect(view.requests.length - settled).toBeLessThanOrEqual(evidence ? 20 : 0)
  },
  60_000,
)

// Catches: a list page from before the read taken as later than its answer, so a Run that
// Coral withholds is asked for twice. Only a page that changed while the read was on its
// way is later than the answer.
it('takes an answer that withholds a run as final when the list did not change meanwhile', async () => {
  usesFakeClock()
  const [a, b] = [runRow('a'), runRow('b')]
  const children: Parameters<typeof coral>[0] = {
    [task.name]: [a, b],
    [a.name]: [call(a, 0), call(a, 1)],
    [b.name]: [call(b, 0)],
  }
  const view = mount(coral(children, task), task.name, { listed: [task.name, a.name] })
  await view.shows('5/5 complete')
  await act(async () => {})
  // A check is due, and Coral now withholds the Run. The list's page stays as it was.
  children[a.name] = 'unavailable'
  await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
  const before = view.requests.length
  await view.report()
  await waitFor(() => expect(traceChildren(view.reader().store, a.name)).toEqual([]))
  await advance(3 * TRACE_REFRESH_INTERVAL_MS)
  // One read asked for the Run: the check's. Nothing asks again.
  const asked = view.requests.slice(before).filter((query) => query.has(`offset.${a.name}`))
  expect(asked).toHaveLength(1)
  expect(traceChildren(view.reader().store, a.name)).toEqual([])
})

// Catches: a whole trace left unavailable, with no read, because an answer that withheld
// its root joined after a newer snapshot of the list that shows the root.
it.each([
  ['Coral does not find the anchor, then returns it', { anchorUnavailable: true }, 'granted'],
  ['Coral denies the list, then grants it', { listUnavailable: true }, 'granted'],
  ['the answer lists no root, then does', {}, 'granted'],
  ['Coral goes on withholding the root', { anchorUnavailable: true }, 'denied'],
  ['the later snapshot does not list the root either', { anchorUnavailable: true }, 'unlisted'],
  ['you open another root and come back meanwhile', { anchorUnavailable: true }, 'switch'],
  ['the page is concealed and shows again meanwhile', { anchorUnavailable: true }, 'conceal'],
] as const)(
  'asks once more for a root that an older answer withholds and a newer snapshot lists: %s',
  async (_case, how, then) => {
    usesFakeClock()
    const other: Row<'task'> = { ...task, name: `${task.name}-other`, intent: 'Other goal' }
    const [a, c] = [runRow('a'), { ...runRow('c'), task: other.name }]
    const source = coral(
      {
        [task.name]: [a],
        [a.name]: [call(a, 0)],
        [other.name]: [c],
        [c.name]: [{ ...call(c, 0), task: other.name }],
      },
      [task, other],
    )
    const base = source.load
    let reads = 'grant'
    let listsRoot = true
    source.load = (query) => {
      const page = base(query)
      // The trace's own reads name the root as their anchor; the list's pages do not.
      if (query.has('anchor'))
        return reads === 'grant' ? page : { ...page, rows: [], branches: {}, ...how }
      return listsRoot ? page : { ...page, rows: page.rows.filter((row) => row.name !== task.name) }
    }
    const view = mount(source, task.name, { listed: [task.name] })
    await view.shows('2/2 complete')
    await act(async () => {})
    const unavailable = () => view.reader()?.store.unavailable
    const own = () => view.requests.filter((query) => query.get('anchor') === task.name).length

    // A check is due. Coral withholds the root when the read arrives; the answer is slow.
    await advance(TRACE_REVALIDATE_INTERVAL_MS, TRACE_REVALIDATE_INTERVAL_MS)
    const release = view.hold((query) => query.has('anchor'), true)
    reads = 'withhold'
    const before = own()
    await view.report()
    await waitFor(() => expect(own()).toBe(before + 1))
    // Coral lists the root again, and the list's later snapshot shows it.
    reads = then === 'denied' ? 'withhold' : 'grant'
    listsRoot = then !== 'unlisted'
    await view.snapshot()
    if (then === 'switch') await view.open(other.name)
    if (then === 'conceal') await view.conceal(true)
    await act(async () => release())
    if (then === 'switch') await view.open(task.name)
    if (then === 'conceal') await view.conceal(false)
    await advance(2 * TRACE_REFRESH_INTERVAL_MS)
    const joined = own()
    // Nothing more comes: no evidence, and the list does not change again.
    for (let second = 0; second < 60; second += 2) await advance(TRACE_REFRESH_INTERVAL_MS)
    if (then === 'granted' || then === 'switch' || then === 'conceal') {
      // One more read of the root, or a fresh trace after a switch or a concealment.
      expect(unavailable()).toBe(false)
      await view.shows('2/2 complete')
    } else {
      // The answer stands: the root stays unavailable.
      expect(unavailable()).toBe(true)
    }
    // Reads the root once more at most when the answer withheld it and a newer snapshot
    // listed it, and nothing after that.
    expect(joined - before - 1).toBeLessThanOrEqual(then === 'unlisted' ? 0 : 2)
    expect(own()).toBe(joined)
  },
  60_000,
)
