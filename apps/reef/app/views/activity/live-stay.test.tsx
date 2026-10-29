/** @vitest-environment happy-dom */
import { act, fireEvent, screen } from '@testing-library/react'
import { useEffect, useState } from 'react'
import { useLoaderData, useLocation, useRevalidator } from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { activityPage, approvalRequest, invocationRow, runRow } from './fixtures'
import { MIN_LIVE_RECORD_STAY_MS } from './navigation'
import type { ActivityPage, ActivityRow, LiveLatest } from './types'

// Page live mode shows each record at least MIN_LIVE_RECORD_STAY_MS before it selects
// another, through the view's own navigation, on a fake clock. navigation.test.ts owns
// which record it selects.
// Catches: live mode replacing a record you had no time to read, a snapshot restarting
// the wait, a step that lands after you closed the rail or turned live mode off, and a
// wait that replays each record that arrived meanwhile instead of the newest.

const list = '/workspaces/finance/activity'
const at = Date.parse('2026-10-27T10:00:00.000Z')
/** Standalone Runs, newest first, with the operations each called, in order. */
let runs: { id: string; running: boolean; started: number; calls?: string[] }[] = []
/** An action you can take in a Run, as the loader names it. */
let action: NonNullable<LiveLatest['action']> | undefined
let refresh = () => {}
/** Delivers the page at the current address as the observation stream would. */
let stream = () => {}
/** How long each read of the page takes, on the fake clock. */
let delay = 0
/** Whether each Run you open asks you to review an approval, where you can write a comment. */
let review = false

const rowOf = ({ id, running, started }: (typeof runs)[number]): ActivityRow =>
  runRow(id, '', {
    state: running ? 'running' : 'succeeded',
    startedAt: new Date(started).toISOString(),
    endedAt: running ? null : new Date(started).toISOString(),
  })

/** The operation calls of the Run `id`, in order. */
const callsOf = ({ id, calls = [] }: (typeof runs)[number]): ActivityRow[] =>
  calls.map((operation, position) => invocationRow(runName(id), operation, { position }))

/** The page the loader reads at `url`. */
function pageFor(url: URL): ActivityPage {
  const query = url.searchParams
  const record = query.get('record')
  const rows = runs.map(rowOf)
  const selectedRow = [...rows, ...runs.flatMap(callsOf)].find((row) => row.name === record) ?? null
  const expanded = query.getAll('expand')
  const held = runs.find(
    (run) =>
      run.running &&
      (runName(run.id) === record || callsOf(run).some((row) => row.name === record)),
  )
  const holds = Boolean(held)
  return activityPage({
    rows,
    expanded,
    branches: Object.fromEntries(
      expanded.map((name) => {
        const calls = callsOf(
          runs.find((run) => runName(run.id) === name) ?? { id: '', running: false, started: 0 },
        )
        return [
          name,
          { rows: calls, total: String(calls.length), offset: 0, next: '', unavailable: false },
        ]
      }),
    ),
    selected: selectedRow ? record : null,
    selectedRow,
    detail:
      selectedRow?.kind === 'run'
        ? {
            kind: 'run',
            run: selectedRow,
            request: review ? approvalRequest : null,
            source: '',
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          }
        : null,
    liveLatest:
      query.get('live') === 'latest' && !holds
        ? {
            running: Object.fromEntries(
              rows.flatMap((row) =>
                row.kind === 'run' && row.state === 'running' ? [[row.name, row]] : [],
              ),
            ),
            complete: true,
            action,
            // The newest record, when nothing runs: the newest Run, first in the list.
            latest:
              rows[0] &&
              !action &&
              !rows.some((row) => row.kind === 'run' && row.state === 'running')
                ? { root: rows[0].name, record: rows[0].name, run: true }
                : undefined,
          }
        : undefined,
  })
}

function Route() {
  const loaded = useLoaderData<ActivityPage>()
  const revalidator = useRevalidator()
  const location = useLocation()
  // A page the observation stream delivered, as a state update outside the router, until
  // the next read of the loader.
  const [streamed, setStreamed] = useState<{ page: ActivityPage; over: ActivityPage }>()
  const page = streamed?.over === loaded ? streamed.page : loaded
  useEffect(() => {
    refresh = () => void revalidator.revalidate()
    stream = () =>
      setStreamed({ page: pageFor(new URL(location.search, `http://reef${list}`)), over: loaded })
  })
  return (
    <ActionReviewSession request={page.detail?.kind === 'run' ? page.detail.request : null}>
      <Activity
        page={page}
        windowKey="live-stay"
        live={{ connected: true, followRoot: true, atStart() {}, viewport() {} }}
      />
    </ActionReviewSession>
  )
}

const runName = (id: string) => runRow(id, '').name
/** Starts a Run, newest of all, `running` or not. */
function start(id: string, running = true) {
  runs.unshift({ id, running, started: Date.now() })
}
const end = (id: string) => void (runs.find((run) => run.id === id)!.running = false)
/** The Run `id` calls the operation `call`. Returns the call's record. */
function call(id: string, operation: string) {
  const run = runs.find((item) => item.id === id)!
  run.calls = [...(run.calls ?? []), operation]
  return invocationRow(runName(id), operation).name
}

/**
 * Opens Activity on the Run `record`, or on any record a name with a slash gives, with
 * `expand` open and page live mode on, or off with `live` false.
 */
async function open(record: string, live = true, expand: string[] = []) {
  const query = new URLSearchParams({
    tree: '1',
    record: record.includes('/') ? record : runName(record),
  })
  for (const name of expand) query.append('expand', name)
  if (live) query.set('live', 'latest')
  const { router, unmount } = renderRoute(<Route />, {
    entry: `${list}?${query}`,
    routes: [
      {
        path: list,
        loader: async ({ request }) => {
          if (delay) await new Promise((resolve) => setTimeout(resolve, delay))
          return pageFor(new URL(request.url))
        },
        Component: Route,
      },
    ],
  })
  await settle()
  const address = () => new URLSearchParams(router.state.location.search)
  // Each record the address selected, in order.
  const records = [address().get('record')]
  router.subscribe(() => {
    const selected = address().get('record')
    if (records.at(-1) !== selected) records.push(selected)
  })
  return { router, unmount, address, records }
}
/** Lets reads and steps that are due land, without moving the clock. */
const settle = () => act(() => vi.advanceTimersByTimeAsync(0))
/** Moves the clock by `ms`, and lets what is due land. */
const pass = (ms: number) => act(() => vi.advanceTimersByTimeAsync(ms))
/** Changes Coral, and lets live mode read it. */
async function change(apply: () => void) {
  apply()
  await act(async () => refresh())
  await settle()
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'], now: at })
  runs = []
  action = undefined
  delay = 0
  review = false
  refresh = () => {}
  stream = () => {}
})
afterEach(() => vi.useRealTimers())

it('shows the record it opened on for MIN_LIVE_RECORD_STAY_MS before it selects a newer one', async () => {
  start('old', false)
  const view = await open('old')
  await pass(1000)
  await change(() => start('new'))
  await pass(MIN_LIVE_RECORD_STAY_MS - 1000 - 1)
  expect(view.address().get('record')).toBe(runName('old'))
  await pass(1)
  expect(view.address().get('record')).toBe(runName('new'))
  expect(view.address().get('live')).toBe('latest')
})

it('selects only the newest record that arrived while it waited', async () => {
  start('old', false)
  const view = await open('old')
  for (const id of ['a', 'b', 'c']) {
    await change(() => start(id, false))
    await pass(1000)
  }
  await pass(MIN_LIVE_RECORD_STAY_MS)
  expect(view.records).toEqual([runName('old'), runName('c')])
})

it('never restarts the wait for a snapshot of the same records', async () => {
  start('old', false)
  const view = await open('old')
  await change(() => start('new'))
  for (let elapsed = 0; elapsed < MIN_LIVE_RECORD_STAY_MS - 1000; elapsed += 1000) {
    await pass(1000)
    await change(() => {})
  }
  expect(view.address().get('record')).toBe(runName('old'))
  await pass(1000)
  expect(view.address().get('record')).toBe(runName('new'))
})

it('shows the next record at once when the one before showed past the minimum, then for the full time', async () => {
  start('first')
  const view = await open('first')
  await pass(MIN_LIVE_RECORD_STAY_MS * 2)
  await change(() => start('second'))
  await pass(MIN_LIVE_RECORD_STAY_MS)
  expect(view.address().get('record')).toBe(runName('first'))
  // It shows for longer than the minimum already, so the next Run opens at once.
  await change(() => end('first'))
  expect(view.address().get('record')).toBe(runName('second'))
  // Which then shows for the full minimum before the Run that ended last.
  await change(() => {
    end('second')
    start('third', false)
  })
  await pass(MIN_LIVE_RECORD_STAY_MS - 1)
  expect(view.address().get('record')).toBe(runName('second'))
  await pass(1)
  expect(view.address().get('record')).toBe(runName('third'))
})

it('opens the action you can take once the record showed long enough', async () => {
  start('asks')
  start('old', false)
  const view = await open('old')
  await change(() => {
    start('newer')
    const run = runName('asks')
    action = { task: '', action: { run, record: run, request: 'workspaces/finance/connections/c' } }
  })
  await pass(MIN_LIVE_RECORD_STAY_MS - 1)
  expect(view.address().get('record')).toBe(runName('old'))
  await pass(1)
  expect(view.address().get('record')).toBe(runName('asks'))
  expect(view.address().get('request')).toBe('workspaces/finance/connections/c')
})

it('turns on at once, then waits from the record it moved to', async () => {
  start('old', false)
  start('new')
  const view = await open('old', false)
  await act(async () => {
    fireEvent.click(screen.getAllByRole('button', { name: 'Follow latest activity' })[0])
  })
  await settle()
  expect(view.address().get('record')).toBe(runName('new'))
  await change(() => {
    end('new')
    start('newer', false)
  })
  await pass(MIN_LIVE_RECORD_STAY_MS - 1)
  expect(view.address().get('record')).toBe(runName('new'))
  await pass(1)
  expect(view.address().get('record')).toBe(runName('newer'))
})

// Catches: a step that waited in page live mode leaving a count of your navigations that
// makes Run live mode, turned on from the same record, skip its first step and stay put.
it('follows the Run at once when you switch to it from page live mode while a step waited', async () => {
  start('busy')
  const first = call('busy', 'first')
  const view = await open(first, true, [runName('busy')])
  // A newer call of the Run you have open waits for the one you see.
  const second = call('busy', 'second')
  await change(() => {})
  await pass(1000)
  expect(view.address().get('record')).toBe(first)
  // You turn page live mode off, then follow this Run, through its address: this page
  // stub has no Run toggle. The clock stands still, so the step goes in the render that
  // follows, not after any timer.
  await act(async () => {
    fireEvent.click(screen.getAllByRole('button', { name: 'Follow latest activity' })[0])
  })
  const run = new URLSearchParams(view.address())
  run.set('live', '1')
  await act(() => view.router.navigate(`${list}?${run}`))
  await act(async () => {})
  expect(view.address().get('live')).toBe('1')
  expect(view.address().get('record')).toBe(second)
})

it.each<[string, (view: Awaited<ReturnType<typeof open>>) => Promise<void>]>([
  [
    'you pick a record',
    async ({ router }) => {
      await act(() =>
        router.navigate(`${list}?tree=1&record=${encodeURIComponent(runName('old'))}`),
      )
    },
  ],
  [
    'you turn live mode off',
    async () => {
      await act(async () => {
        fireEvent.click(screen.getAllByRole('button', { name: 'Follow latest activity' })[0])
      })
    },
  ],
  [
    'you close the rail',
    async () => {
      await act(async () => {
        fireEvent.click(screen.getByRole('button', { name: 'Close details' }))
      })
    },
  ],
  ['the view goes away', async ({ unmount }) => unmount()],
])('takes no step after %s while it waits', async (_, stop) => {
  start('old', false)
  const view = await open('old')
  await change(() => start('new'))
  await pass(1000)
  await stop(view)
  await settle()
  const stopped = view.address().toString()
  await pass(MIN_LIVE_RECORD_STAY_MS * 2)
  expect(view.address().toString()).toBe(stopped)
  expect(view.records).not.toContain(runName('new'))
})

const pick = () => {
  const row = screen.getByRole('row', { name: /\bolder\b/ })
  row.dispatchEvent(new MouseEvent('click', { bubbles: true }))
}
// Catches: a step that comes due while your own navigation still loads replacing it,
// also when a snapshot that arrives meanwhile makes the step wait again.
it.each<[string, () => void, boolean]>([
  ['your pick', pick, false],
  [
    'turning live mode off',
    () =>
      screen
        .getAllByRole('button', { name: 'Follow latest activity' })[0]
        .dispatchEvent(new MouseEvent('click', { bubbles: true })),
    false,
  ],
  [
    'closing the rail',
    () =>
      screen
        .getByRole('button', { name: 'Close details' })
        .dispatchEvent(new MouseEvent('click', { bubbles: true })),
    false,
  ],
  ['your pick, while a snapshot names a newer record,', pick, true],
])('lets %s land when the wait ends while it loads', async (_, interrupt, snapshot) => {
  start('older', false)
  start('old', false)
  const view = await open('old')
  await change(() => start('new'))
  await pass(MIN_LIVE_RECORD_STAY_MS - 200)
  delay = 300
  // A click outside `act`, as in a browser: the router applies the navigation in a
  // transition, which the wait's own render can overtake.
  interrupt()
  if (snapshot) {
    // The stream's snapshot is no navigation, so it renders before yours lands.
    start('newer')
    stream()
  }
  // The wait ends before React applies your navigation.
  vi.advanceTimersByTime(200)
  await pass(300)
  expect(view.address().has('live')).toBe(false)
  expect(view.records).not.toContain(runName('new'))
  expect(view.records).not.toContain(runName('newer'))
})

/** Writes `text` as the comment of the review the rail shows. */
const write = (text: string) =>
  act(async () => {
    fireEvent.change(screen.getByRole('textbox', { name: 'Comment (optional)' }), {
      target: { value: text },
    })
  })
/**
 * Writes a comment, before a newer record arrives or while it waits, then picks another
 * record once the wait ended.
 */
async function pickFromComment(waiting: boolean) {
  review = true
  start('older', false)
  start('old', false)
  const view = await open('old')
  if (!waiting) await write('Check it')
  await change(() => start('new'))
  await pass(1000)
  if (waiting) await write('Check it')
  await pass(MIN_LIVE_RECORD_STAY_MS)
  expect(view.address().get('record')).toBe(runName('old'))
  await act(async () => pick())
  await settle()
  screen.getByRole('dialog', { name: 'Discard your comment?' })
  return view
}
const comments = [
  ['before the newer record arrives', false],
  ['while the newer record waits', true],
] as const

// Catches: the review letting go of your comment before the pick it discarded it for
// loads, so live mode's step, already due, replaces your pick.
it.each(comments)(
  'lets the pick you discard a comment for land, written %s',
  async (_, waiting) => {
    const view = await pickFromComment(waiting)
    delay = 300
    // A click outside `act`, as in a browser: the router applies your pick in a transition.
    screen.getByRole('button', { name: 'Discard comment' }).click()
    await pass(300)
    await pass(MIN_LIVE_RECORD_STAY_MS)
    expect(view.records).toEqual([runName('old'), runName('older')])
    expect(view.address().has('live')).toBe(false)
  },
)

// Catches: a navigation that replaces the pick you discarded a comment for asking about
// the comment again, landing somewhere else, or leaving the comment behind.
it('clears the comment you discarded when another navigation replaces the pick', async () => {
  const view = await pickFromComment(true)
  delay = 300
  screen.getByRole('button', { name: 'Discard comment' }).click()
  await pass(100)
  const other = runName('new')
  void view.router.navigate(`${list}?tree=1&record=${encodeURIComponent(other)}`)
  await pass(300)
  await pass(MIN_LIVE_RECORD_STAY_MS)
  expect(view.address().get('record')).toBe(other)
  expect(view.records).not.toContain(runName('older'))
  expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).toBeNull()
  expect(screen.getByRole('textbox', { name: 'Comment (optional)' })).toHaveValue('')
})

// Catches: a pick you gave up to keep your comment holding live mode after you clear it,
// or the wait starting again from then.
it.each(comments)(
  'follows again once you clear the comment you kept, written %s',
  async (_, waiting) => {
    const view = await pickFromComment(waiting)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Keep editing' }))
    })
    await pass(MIN_LIVE_RECORD_STAY_MS)
    expect(view.address().get('record')).toBe(runName('old'))
    await write('')
    await pass(50)
    expect(view.records).toEqual([runName('old'), runName('new')])
    expect(view.address().get('live')).toBe('latest')
  },
)
