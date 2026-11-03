/** @vitest-environment happy-dom */
import { act } from '@testing-library/react'
import { useLayoutEffect } from 'react'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, expect, it, vi } from 'vitest'
import { useLoaderData } from 'react-router'
import { ActivityLogLevel, ActivityRunState } from '@/generated/coral/management/v1/activity_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { loadActivity } from '@/lib/activity.server'
import { observeActivity } from '@/lib/activity-observation.server'
import { activityChanges } from '@/lib/activity-stream.test-support'
import { fakeManagement } from '@/lib/management.test-support'
import { renderRoute } from '@/test-support/route'
import { MIN_LIVE_RECORD_STAY_MS } from '@/views/activity/navigation'
import type { ActivityPage } from '@/views/activity/types'
import { LiveActivity } from './activity-live'

// Page live mode through the whole reader: the Activity loader and the observation route
// read an in-memory Coral, and the view takes live mode's steps itself. The table reads
// deep in a long Run, so each snapshot holds only the records near it.
// Catches: live mode finding new work only near the records the table shows, a new Task
// taking live mode from a Run that runs, and a snapshot of a Run's logs reading every
// older page of the list. navigation.test.ts and activity.server.test.ts own the rules.

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const taskName = (id: string) => `${workspace}/tasks/${id}`
const runName = (id: string) => `${workspace}/runs/${id}`
const now = BigInt(Date.now()) * 1_000_000n
const second = 1_000_000_000n
interface Run {
  id: string
  task: string
  /** The Agent that submitted it, if any. */
  agent?: string
  ended: boolean
  /** How many logs it wrote. */
  logs: number
  order: number
}
/** A Search in a Task, which starts no Run. */
interface Search {
  id: string
  task: string
  order: number
}
interface Task {
  id: string
  agent?: string
  order: number
  /** Its Runs and Searches, in the order they started. */
  children: (Run | Search)[]
}
const isRun = (child: Run | Search): child is Run => 'ended' in child

/** A Run as Coral reports it. */
const runValue = (run: Run) => ({
  name: runName(run.id),
  workspace,
  task: taskName(run.task),
  agent: run.agent,
  state: run.ended ? ActivityRunState.SUCCEEDED : ActivityRunState.RUNNING,
  startedAtUnixNanos: now + BigInt(run.order) * second,
  endedAtUnixNanos: run.ended ? now + BigInt(run.order + 1) * second : undefined,
  intent: `Run ${run.id}`,
  actor: run.agent ?? 'user',
})

/** The submission that accepted `run`, at `position` in its Task. */
const submission = (run: Run, position = 0) => ({
  name: `${taskName(run.task)}/interactions/${run.id}`,
  position: BigInt(position),
  kind: 'submit',
  intent: run.id,
  outcome: 'succeeded',
  startedAtUnixNanos: now + BigInt(run.order) * second,
  acceptedRun: runName(run.id),
  run: runValue(run),
})
/** A Search as Coral reports it, at `position` in its Task. */
const searchValue = (search: Search, position: number) => ({
  name: `${taskName(search.task)}/interactions/${search.id}`,
  position: BigInt(position),
  kind: 'search',
  intent: search.id,
  outcome: 'succeeded',
  startedAtUnixNanos: now + BigInt(search.order) * second,
})

/** The window a read of `total` records takes. An offset at or past the end reads the
 * last window, as Coral does. */
const windowOf = (input: { pageSize: number; viewport?: { offset: bigint } }, total: number) => {
  const size = input.pageSize || 50
  const offset = Number(input.viewport?.offset ?? 0n)
  return { size, offset, from: offset >= total ? Math.max(0, total - size) : offset }
}

/** Coral's history, newest Task first. It counts the list reads it serves. */
function coral() {
  const tasks: Task[] = []
  const changes = activityChanges()
  /**
   * Each read of top-level records: of the records you watch, a page of the list, or a
   * page past it, which page live mode reads 100 records at a time to find new work.
   */
  const roots: ('watched' | 'list' | 'scan')[] = []
  /** Each read of a Task's children: its name, and whether it read only its newest child. */
  const taskReads: { name: string; newest: boolean }[] = []
  let order = 0
  let scanUnavailable = false
  const runs = () => tasks.flatMap((task) => task.children.filter(isRun))
  const find = (name: string) => runs().find((run) => runName(run.id) === name)!
  const root = ({ id, agent, order: started, children }: Task) => ({
    record: {
      case: 'task' as const,
      value: {
        name: taskName(id),
        workspace,
        agent,
        intent: `Task ${id}`,
        outcome: 'open',
        actor: agent ?? 'user',
        startedAtUnixNanos: now + BigInt(started) * second,
        contextImpactBytes: 1n,
        // As Coral does: a Task's row says whether a Run of it runs.
        contextImpactPartial: children.some((child) => isRun(child) && !child.ended),
      },
    },
  })
  const api = fakeManagement({
    workspaces: { getWorkspace: ({ name }) => ({ name, state: WorkspaceState.ACTIVE }) },
    savedFunctions: { listSavedFunctions: () => ({ savedFunctions: [] }) },
    activity: {
      watchActivity: changes.watch,
      browseRequiredActions: () => ({ actions: [], actionCount: 0n }),
      browseActivityRoots: (input) => {
        const names = tasks.map(({ id }) => taskName(id))
        if (input.retained) {
          roots.push('watched')
          const watched = input.retained.names
          return {
            roots: tasks.filter(({ id }) => watched.includes(taskName(id))).map(root),
            hasBefore: names.findIndex((name) => watched.includes(name)) > 0,
          }
        }
        const scan = input.pageSize === 100
        roots.push(scan ? 'scan' : 'list')
        if (scan && scanUnavailable) throw new ConnectError('history unavailable', Code.NotFound)
        const from = input.anchor
          ? Math.max(0, names.indexOf(input.anchor))
          : input.after
            ? names.indexOf(input.after) + 1
            : 0
        const size = input.pageSize || 50
        return {
          roots: tasks.slice(from, from + size).map(root),
          hasBefore: from > 0,
          hasAfter: from + size < tasks.length,
        }
      },
      inspectTask: (input) => {
        const task = tasks.find(({ id }) => taskName(id) === input.name)!
        const total = task.children.length
        const { size, offset, from } = windowOf(input, total)
        taskReads.push({ name: input.name, newest: size === 1 && offset >= total && total > 0 })
        return {
          task: root(task).record.value,
          total: BigInt(total),
          offset: BigInt(from),
          interactions: task.children
            .slice(from, from + size)
            .map((child, index) =>
              isRun(child) ? submission(child, from + index) : searchValue(child, from + index),
            ),
        }
      },
      browseRunEntries: (input) => {
        const run = find(input.name)
        const { size, from } = windowOf(input, run.logs)
        const to = Math.min(run.logs, from + size)
        return {
          run: runValue(run),
          submission: submission(run, position(run)),
          total: BigInt(run.logs),
          offset: BigInt(from),
          hasMore: to < run.logs,
          nextAfter: BigInt(to),
          entries: Array.from({ length: to - from }, (_, index) => ({
            record: {
              case: 'event' as const,
              value: { sequence: BigInt(from + index + 1), kind: 'program.logged' },
            },
          })),
        }
      },
      inspectRun: ({ name }) => {
        const run = find(name)
        return {
          run: runValue(run),
          submission: submission(run, position(run)),
          submittedSource: 'console.info(1)',
          terminal: run.ended
            ? { case: 'succeeded' as const, value: { finalValueJson: 'null' } }
            : undefined,
        }
      },
      inspectRunEvent: ({ run, sequence }) => ({
        run: runValue(find(run)),
        event: {
          sequence,
          payload: {
            case: 'programLogged' as const,
            value: { level: ActivityLogLevel.INFO, message: `log ${sequence}` },
          },
        },
      }),
    },
  })
  /** The position of `run` in its Task. */
  function position(run: Run) {
    return tasks.find(({ id }) => id === run.task)!.children.indexOf(run)
  }
  /** A new Run of the Task `task` that has written `logs`, started after everything else. */
  function submit(task: string, id: string, { logs = 0, ended = false } = {}) {
    const owner = tasks.find((item) => item.id === task)!
    const run = { id, task, agent: owner.agent, ended, logs, order: ++order }
    owner.children.push(run)
    return runName(id)
  }
  return {
    api,
    roots,
    taskReads,
    /** A new Task, newest of all, whose one Run has written `logs`. */
    start(id: string, { logs = 0, ended = false, agent = '' } = {}) {
      const owner = agent ? `${workspace}/agents/${agent}` : undefined
      tasks.unshift({ id, agent: owner, order: order + 1, children: [] })
      return submit(id, `${id}-run`, { logs, ended })
    },
    /** A new Task, newest of all, with no record yet. */
    create(id: string) {
      tasks.unshift({ id, order: ++order, children: [] })
    },
    submit,
    /** A new Search in the Task `task`, started after everything else. Returns its name. */
    search(task: string, id: string) {
      tasks.find((item) => item.id === task)!.children.push({ id, task, order: ++order })
      return `${taskName(task)}/interactions/${id}`
    },
    log: (name: string) => void find(name).logs++,
    end: (name: string) => void (find(name).ended = true),
    /** Whether Reef can read the pages that page live mode reads past the list. */
    scanUnavailable(value: boolean) {
      scanUnavailable = value
    },
    /** Whether Coral ended `run`. */
    ended: (name: string) => find(name).ended,
    notify: () => changes.emit(),
  }
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

/** The loader's page the view last committed. */
let committed: ActivityPage | undefined
/** The Activity route's view of its loader's page. */
function Route() {
  const page = useLoaderData<ActivityPage>()
  useLayoutEffect(() => {
    committed = page
  })
  return <LiveActivity initial={page} />
}

/** Activity at the Workspace route with page live mode on, opened on the Run of `task`. */
function open(world: ReturnType<typeof coral>, task: string) {
  // Live mode shows each record a minimum time: `selects` moves the clock past it.
  vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'Date'] })
  const run = runName(`${task}-run`)
  const path = `/${workspace}/activity`
  // The windows each observation read, in order.
  const windows: { followRoot: boolean; roots: string[] }[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn((_url: string, init: RequestInit) => {
      windows.push(JSON.parse(init.body as string).window)
      return observeActivity(
        new Request('http://reef/activity/observe', {
          method: 'POST',
          body: init.body,
          signal: init.signal,
        }),
        world.api,
      )
    }),
  )
  const query = new URLSearchParams({ tree: '1', live: 'latest', record: run })
  query.append('expand', taskName(task))
  query.append('expand', run)
  const { router, container } = renderRoute(<Route />, {
    entry: `${path}?${query}`,
    routes: [
      {
        path,
        loader: ({ request }) => loadActivity(request, world.api, { scope: { workspace } }),
        Component: Route,
      },
    ],
  })
  const address = () => new URLSearchParams(router.state.location.search)
  // Each record the address selected, in order, and each move from a Run that Coral
  // had not ended.
  const records: string[] = []
  const steals: string[] = []
  router.subscribe(() => {
    const record = address().get('record') ?? ''
    if (records.at(-1) === record) return
    const from = records.at(-1)?.match(/^.*\/runs\/[^/]+/)?.[0]
    const to = record.match(/^.*\/runs\/[^/]+/)?.[0]
    if (from && from !== to && !world.ended(from)) steals.push(`${from} -> ${record}`)
    records.push(record)
  })
  const rowShown = (record: string) =>
    Boolean(container.querySelector(`[role=table] [data-record-key="${CSS.escape(record)}"]`))
  const watched = () => world.roots.filter((read) => read === 'watched').length
  return {
    /** Each record live mode selected after it opened, in order. */
    records,
    /** Each time live mode left a Run that still ran. */
    steals,
    /** Observation reads only the records near the viewport, away from the newest. */
    readingDeep: () => windows.at(-1)!.followRoot === false,
    /** The table shows the row of `record`. */
    rowShown,
    /** Once a record showed for the minimum time, live mode selects `record`, and stays on. */
    async selects(record: string) {
      await act(() => vi.advanceTimersByTimeAsync(MIN_LIVE_RECORD_STAY_MS))
      await vi.waitFor(() => expect(address().get('record')).toBe(record), { timeout: 5000 })
      expect(address().get('live')).toBe('latest')
    },
    /** Scrolls down, away from the newest records, until observation reads only those near. */
    async readDeep() {
      const table = await vi.waitFor(
        () => {
          const found = container.querySelector('[role=table]')
          expect(found).not.toBeNull()
          return found!
        },
        { timeout: 5000 },
      )
      // The router holds a page before React commits it, and a page React has not
      // committed starts a new observation from the one before. A scroll before then
      // reaches only the observation that page replaces.
      await vi.waitFor(
        () => {
          expect(router.state.navigation.state).toBe('idle')
          expect(router.state.revalidation).toBe('idle')
          expect(committed).toBe(Object.values(router.state.loaderData).at(-1))
        },
        { timeout: 5000 },
      )
      await act(async () => {
        table.dispatchEvent(new WheelEvent('wheel', { deltaY: 400, bubbles: true }))
      })
      await vi.waitFor(() => expect(windows.at(-1)!.followRoot).toBe(false), { timeout: 5000 })
    },
    /**
     * Changes Coral, then waits for its snapshot and live mode's steps. Returns how many
     * reads of the page Reef made, as snapshots or as loads, how many pages past the list
     * page live mode read, each Task whose Runs Reef read, and how many reads of only a
     * Task's newest child it made.
     */
    async change(apply: () => void) {
      const before = {
        watched: watched(),
        roots: world.roots.length,
        tasks: world.taskReads.length,
      }
      await act(async () => {
        apply()
        world.notify()
      })
      // Each snapshot reads the records you watch once.
      await vi.waitFor(() => expect(watched()).toBeGreaterThan(before.watched), { timeout: 5000 })
      // Live mode takes its steps from the snapshot.
      await act(() => new Promise((resolve) => setTimeout(resolve, 200)))
      const reads = world.roots.slice(before.roots)
      const children = world.taskReads.slice(before.tasks)
      return {
        snapshots: reads.filter((read) => read === 'watched').length,
        loads: reads.filter((read) => read === 'list').length,
        scans: reads.filter((read) => read === 'scan').length,
        tasks: [...new Set(children.filter((read) => !read.newest).map((read) => read.name))],
        newest: children.filter((read) => read.newest).length,
      }
    },
  }
}

it('keeps a Run that runs while new Tasks arrive deep in its logs, then moves to the newest Run that runs', async () => {
  const world = coral()
  const long = world.start('long', { logs: 250 })
  const view = open(world, 'long')
  // Live mode reads the end of the long Run, past its first windows.
  await view.selects(`${long}/events/250`)
  await view.readDeep()

  // Each log reads the records you watch, never the list past them.
  for (const sequence of [251, 252]) {
    const reads = await view.change(() => world.log(long))
    expect(reads).toMatchObject({ scans: 0, tasks: [taskName('long')], newest: 0 })
    await view.selects(`${long}/events/${sequence}`)
  }

  // Tasks that ended, then one that runs, arrive past the records you watch.
  let next = ''
  const arrived = await view.change(() => {
    for (const id of ['q1', 'q2', 'q3', 'q4', 'q5', 'q6']) world.start(id, { ended: true })
    next = world.start('next')
  })
  expect(arrived).toMatchObject({ scans: 0, tasks: [taskName('long')], newest: 0 })
  await view.selects(`${long}/events/252`)
  expect(view.readingDeep()).toBe(true)

  // The long Run ends. Live mode moves to the Run that runs, without a scroll or a click.
  await view.change(() => world.end(long))
  await view.selects(next)
}, 20_000)

it('rests on the Run that ended, then follows a new Task that arrives while you read deep', async () => {
  const world = coral()
  // More history than one page past the list holds.
  for (let index = 0; index < 150; index++) world.start(`old${index}`, { ended: true })
  const long = world.start('long', { logs: 250 })
  const view = open(world, 'long')
  await view.selects(`${long}/events/250`)
  await view.readDeep()

  // The Run ends with nothing else to follow: live mode selects the Run itself, not its
  // last log, and stays on. With nothing that runs, each read of the page reads the
  // history past it once, here two pages, and only the newest child of each of the 150
  // other Tasks, never their Runs. The long Task's window holds its own.
  const ended = await view.change(() => world.end(long))
  await view.selects(long)
  expect(ended.snapshots).toBeGreaterThan(0)
  expect(ended.scans).toBe(2 * (ended.snapshots + ended.loads))
  expect(ended.tasks).toEqual([taskName('long')])
  expect(ended.newest).toBe(150 * (ended.snapshots + ended.loads))
  await view.readDeep()

  // Reef cannot read the history past the list: live mode stays where it is.
  world.scanUnavailable(true)
  let next = ''
  await view.change(() => {
    next = world.start('next')
  })
  await view.selects(long)
  expect(view.rowShown(long)).toBe(true)

  // Once it can, live mode follows the new Task's Run, with no scroll or click.
  world.scanUnavailable(false)
  await view.change(() => {})
  await view.selects(next)
}, 20_000)

it("keeps one Agent's Run that runs while other Agents' Runs run, log and end", async () => {
  const world = coral()
  const alpha = world.start('alpha', { agent: 'alpha' })
  const beta = world.start('beta', { agent: 'beta' })
  const gamma = world.start('gamma', { agent: 'gamma' })
  // Live mode follows beta's Run, though gamma's started later. The table reads away
  // from the newest records.
  const view = open(world, 'beta')
  await view.readDeep()

  // The other Agents' Runs log, one ends, and a fourth Agent starts a Task. Each
  // snapshot leaves live mode on beta's Run.
  await view.change(() => world.log(gamma))
  await view.change(() => world.log(alpha))
  await view.change(() => world.end(alpha))
  let delta = ''
  await view.change(() => {
    delta = world.start('delta', { agent: 'delta' })
  })
  await view.selects(beta)

  // Beta's Run ends: live mode moves to the Run that runs and started last.
  await view.change(() => world.end(beta))
  await view.selects(delta)
  expect(view.steals).toEqual([])
  expect(view.records.filter((record) => record !== beta && record !== delta)).toEqual([])
}, 20_000)

// The shape of a real capture with three Agents, one Task each. The list orders Tasks by
// when they started, so `single`, the last to start, comes first.
// Catches: live mode leaving a Run that just ended for an older record of the newest
// Task: its Search, or its Run that ended earlier.
it('stays on the Run that ended last until a newer record starts, whichever Task holds it', async () => {
  const world = coral()
  world.create('major')
  world.create('red')
  world.create('single')
  const lookup = world.search('single', 'lookup')
  const own = world.submit('red', 'red-run')
  const view = open(world, 'red')
  await view.selects(own)
  await view.readDeep()

  // The Run ends with nothing else to follow: live mode shows it, not the older Search
  // of the newer Task.
  await view.change(() => world.end(own))
  await view.selects(own)

  // A Run of the newest Task starts, then one of the oldest, which ends last: through
  // the loader's reads, live mode stays on it.
  let house = ''
  await view.change(() => {
    house = world.submit('single', 'house')
  })
  await view.selects(house)
  let late = ''
  await view.change(() => {
    late = world.submit('major', 'late')
  })
  await view.selects(house)
  await view.change(() => world.end(house))
  await view.selects(late)
  await view.change(() => world.end(late))
  await view.selects(late)

  // A newer record replaces it, and live mode stays on.
  let later = ''
  await view.change(() => {
    later = world.search('single', 'later')
  })
  await view.selects(later)
  expect(view.steals).toEqual([])
  expect(view.records).not.toContain(lookup)
  expect(view.records.filter((record) => ![own, house, late, later].includes(record))).toEqual([])
}, 20_000)

// The shape of a real capture: you read deep, a Run you follow ends before its record
// showed for MIN_LIVE_RECORD_STAY_MS, and a newer Task the list does not hold runs.
// Catches: live mode expanding that Task's Run, which a snapshot of the records you
// watch never loads, and then waiting on it.
it('follows a newer Task the list does not hold once a Run ends during the wait', async () => {
  const world = coral()
  for (let index = 0; index < 40; index++) world.start(`old${index}`, { ended: true })
  const long = world.start('long', { logs: 250 })
  const view = open(world, 'long')
  await view.selects(`${long}/events/250`)
  await view.readDeep()
  let next = ''
  await view.change(() => {
    next = world.start('next', { logs: 1 })
  })
  await view.change(() => world.end(long))
  await view.selects(`${next}/events/1`)
  // Within the wait: a newer Task starts, and the Run you follow ends.
  let later = ''
  await view.change(() => {
    later = world.start('later', { logs: 1 })
  })
  await view.change(() => world.end(next))
  await view.selects(`${later}/events/1`)
  expect(view.steals).toEqual([])
}, 30_000)
