import type { Meta, StoryObj } from '@storybook/react-vite'
import {
  createRoutesStub,
  useLoaderData,
  useLocation,
  useNavigate,
  useNavigation,
  useRevalidator,
} from 'react-router'
import { useEffect } from 'react'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { MIN_LIVE_RECORD_STAY_MS, TAKS_MAX_AGE } from './navigation'
import { ActionReviewSession } from '../actions/review'
import {
  activityPage,
  approvalRequest,
  invocationRow,
  runRow,
  taskRow,
  toolRow,
  workspace,
} from './fixtures'
import type { ActivityPage, ActivityRow, RequestDetail, RunDetail } from './types'

// Live mode's scopes in Chromium: the newest activity from the list, and one Task's
// Runs in order, while Tasks, Runs and records arrive. The target rule is
// navigation.test.ts's; live.stories.tsx owns how a followed Run moves.

const at = '2026-09-18T14:02:00.000Z'
type Run = Extract<ActivityRow, { kind: 'run' }>
type Call = Extract<ActivityRow, { kind: 'invocation' }>
type Tool = Extract<ActivityRow, { kind: 'tool' }>
interface RunModel {
  /** Orders it among its Task's records: each new record's is higher. */
  order: number
  row: Run
  calls: Call[]
  ended: boolean
  /** It ended in failure, with no final value. */
  failed?: boolean
  /** The call that waits for your approval, if any. */
  waiting: string | null
}
type Root =
  | {
      kind: 'task'
      /** Orders it with the records of every Task: a Task with no record is this new. */
      order: number
      row: Extract<ActivityRow, { kind: 'task' }>
      runs: RunModel[]
      /** Searches, Describes and exec submissions, ordered with the Runs by `order`. */
      tools: { order: number; row: Tool }[]
      /** Exec submissions that Coral accepted as a Run, which then lists in their place. */
      accepted: Tool[]
    }
  | { kind: 'run'; run: RunModel }

/** When a call started, by the number in its name. */
const startOf = (call: string) => Number(call.split('-op-').at(-1))
const approvalOf = (call: string) => `${workspace}/approvals/${call.split('/').at(-1)}`
const finalOf = (run: string) => `${run}/final`

function newRun(id: string, task = '', order = 0): RunModel {
  return {
    order,
    row: runRow(id, task, {
      actor: `run ${id}`,
      intent: `Run ${id}`,
      state: 'running',
      // Coral accepts each new Run later than the last.
      startedAt: new Date(Date.parse(at) + order * 1000).toISOString(),
    }),
    calls: [],
    ended: false,
    waiting: null,
  }
}
/** A Task that started `age` milliseconds ago: just now, unless a story says otherwise. */
function newTask(id: string, runs: RunModel[] = [], age = 0, order = 0): Root {
  const startedAt = new Date(Date.now() - age).toISOString()
  return {
    kind: 'task',
    order,
    row: { ...taskRow(id, `Task ${id}`), startedAt },
    runs,
    tools: [],
    accepted: [],
  }
}

/** What Coral holds, newest top-level record first. A play changes it, then revalidates. */
const world = {
  roots: [] as Root[],
  count: 0,
  /** By Task, a window of its records that the list keeps although no read refreshes it. */
  stale: {} as Record<string, ActivityRow[]>,
  /** Each loader read, in order: the record it opened and the Tasks whose kept window it served. */
  reads: [] as { record: string | null; kept: string[] }[],
  /** The route's own reads and navigation, for a play while a modal covers its buttons. */
  revalidate: () => {},
  navigate: (_to: string) => {},
  reset(roots: Root[]) {
    this.roots = roots
    this.count = 0
    this.stale = {}
    this.reads = []
  },
  runs(): RunModel[] {
    return this.roots.flatMap((root) => (root.kind === 'task' ? root.runs : [root.run]))
  },
  /** What a play's button changes: a new record, a decision elsewhere, or a Run's end. */
  change(
    kind:
      | 'task'
      | 'standalone'
      | 'run'
      | 'run oldest'
      | 'ask'
      | 'search'
      | 'describe'
      | 'submit'
      | 'accept'
      | 'call'
      | 'approve'
      | 'end'
      | 'end oldest'
      | 'complete'
      | 'end oldest task'
      | 'fail oldest',
  ) {
    const runs = this.runs()
    if (kind === 'task') this.roots.unshift(newTask(`t${++this.count}`, [], 0, this.count))
    if (kind === 'standalone')
      this.roots.unshift({ kind: 'run', run: newRun(`s${++this.count}`, '', this.count) })
    const task = this.roots.find((root) => root.kind === 'task')
    if (kind === 'run' && task?.kind === 'task')
      task.runs.push(newRun(`r${++this.count}`, task.row.name, this.count))
    const oldest = this.roots.findLast((root) => root.kind === 'task')
    if (kind === 'run oldest' && oldest?.kind === 'task')
      oldest.runs.push(newRun(`r${++this.count}`, oldest.row.name, this.count))
    if ((kind === 'search' || kind === 'describe') && task?.kind === 'task') {
      const id = `${kind}-${++this.count}`
      const row = toolRow(task.row.name, id, { tool: kind, intent: `${kind} ${this.count}` })
      task.tools.push({ order: this.count, row })
    }
    // Coral lists an exec submission while it compiles, without a Run.
    if (kind === 'submit' && task?.kind === 'task') {
      const id = `exec-${++this.count}`
      const row = toolRow(task.row.name, id, {
        tool: 'exec',
        intent: `exec ${this.count}`,
        outcome: 'unknown',
        completedAt: null,
      })
      task.tools.push({ order: this.count, row })
    }
    // Then accepts it as a Run that has no record yet, which lists in its place.
    const submitted = task?.kind === 'task' && task.tools.find(({ row }) => row.tool === 'exec')
    if (kind === 'accept' && task?.kind === 'task' && submitted) {
      const model = newRun(`e${++this.count}`, task.row.name, submitted.order)
      const submission = {
        ...submitted.row,
        outcome: 'succeeded',
        completedAt: at,
        acceptedRun: model.row.name,
      }
      model.row = { ...model.row, submission }
      task.tools = task.tools.filter((item) => item !== submitted)
      task.accepted.push(submission)
      task.runs.push(model)
    }
    // The latest Run: each new one's order and number are higher.
    const latest = runs
      .toSorted((a, b) => a.order - b.order || a.row.name.localeCompare(b.row.name))
      .at(-1)
    if (kind === 'call' && latest) this.operate(latest)
    const asking = runs
      .filter((model) => !model.ended)
      .toSorted((a, b) => a.order - b.order)
      .at(-1)
    if (kind === 'ask' && asking) this.operate(asking, true)
    if (kind === 'approve') for (const model of runs) model.waiting = null
    const running = runs.find((model) => !model.ended)
    if (kind === 'end' && running) running.ended = true
    // The Run that started first: the lowest order, then the lowest name.
    const first = runs
      .filter((model) => !model.ended)
      .toSorted((a, b) => a.order - b.order || a.row.name.localeCompare(b.row.name))[0]
    if (kind === 'end oldest' && first) first.ended = true
    if (kind === 'fail oldest' && first) Object.assign(first, { ended: true, failed: true })
    if (kind === 'complete' && task?.kind === 'task')
      task.row = { ...task.row, outcome: 'succeeded', endedAt: at }
    // The oldest Task and its Runs end, while the list keeps its window as it was: a
    // snapshot of only the records you watch never refreshes it.
    if (kind === 'end oldest task' && oldest?.kind === 'task') {
      this.stale[oldest.row.name] = taskRecords(oldest)
      for (const model of oldest.runs) model.ended = true
      oldest.row = { ...oldest.row, outcome: 'succeeded', endedAt: at }
    }
  },
  /** Adds a call to `run`, which waits for your approval if `waits`. */
  operate(run: RunModel, waits = false) {
    const id = `${run.row.name.split('/').at(-1)}-op-${++this.count}`
    const call = invocationRow(run.row.name, id, {
      task: run.row.task,
      callPath: `call.${id}`,
      tool: `call.${id}`,
    })
    run.calls.push(call)
    if (waits) run.waiting = call.name
  },
}

const runView = (model: RunModel): Run => ({
  ...model.row,
  state: model.failed ? 'failed' : model.ended ? 'succeeded' : 'running',
  endedAt: model.ended ? at : null,
  requiresAction: Boolean(model.waiting),
  hasFinalValue: model.ended && !model.failed,
})
function children(model: RunModel): ActivityRow[] {
  const calls = model.calls.map((row): ActivityRow =>
    row.name === model.waiting ? { ...row, state: 'pending', requiresAction: true } : row,
  )
  if (!model.ended || model.failed) return calls
  return [
    ...calls,
    {
      kind: 'output',
      name: finalOf(model.row.name),
      workspace,
      run: model.row.name,
      task: model.row.task,
      label: 'final value',
      startedAt: at,
      eventKind: 'run.ended',
      agent: '',
    },
  ]
}
/** The records of a Task, its Runs and tools, each with its order, oldest first. */
const orderedRecords = (root: Extract<Root, { kind: 'task' }>) =>
  [
    ...root.runs.map((model) => ({ order: model.order, row: runView(model) })),
    ...root.tools,
  ].toSorted((a, b) => a.order - b.order)
/** The records of a Task: its Runs and tools, in order. */
const taskRecords = (root: Extract<Root, { kind: 'task' }>) =>
  orderedRecords(root).map(({ row }) => row)
const branch = (rows: ActivityRow[]) => ({
  rows: rows.map((row, position) => ({ ...row, position })),
  total: String(rows.length),
  next: '',
  unavailable: false,
})

/** What the loader names for page live mode past the list, across the Runs it may follow. */
function latestOf(): ActivityPage['liveLatest'] {
  const recent = (root: Root) =>
    root.kind === 'run' || Date.now() - Date.parse(root.row.startedAt) <= TAKS_MAX_AGE
  const roots = world.roots.filter(recent)
  const asking = roots
    .flatMap((root) =>
      (root.kind === 'task' ? root.runs : [root.run]).map((model) => ({ root, model })),
    )
    .filter(({ model }) => !model.ended && model.waiting)
    .toSorted((a, b) => startOf(a.model.waiting!) - startOf(b.model.waiting!))[0]
  const action = asking && {
    task: asking.root.kind === 'task' ? asking.root.row.name : '',
    action: {
      run: asking.model.row.name,
      record: asking.model.waiting!,
      request: approvalOf(asking.model.waiting!),
    },
  }
  // By top-level record, its newest Run that runs. The loader reads every record.
  const running = Object.fromEntries(
    roots.flatMap((root) => {
      const newest = (root.kind === 'task' ? root.runs : [root.run])
        .filter((model) => !model.ended)
        .toSorted((a, b) => a.order - b.order)
        .at(-1)
      return newest
        ? [[root.kind === 'task' ? root.row.name : newest.row.name, runView(newest)]]
        : []
    }),
  )
  if (!world.roots.length) return undefined
  // With nothing else to follow, the record that started last: a Task's newest record,
  // the Task while it has none, or a standalone Run. At an equal order, the newer root.
  const latest =
    action || Object.keys(running).length
      ? undefined
      : roots
          .map((root) => {
            if (root.kind === 'run')
              return {
                root: root.run.row.name,
                record: root.run.row.name,
                run: true,
                order: root.run.order,
              }
            const newest = orderedRecords(root).at(-1)
            return newest
              ? {
                  root: root.row.name,
                  record: newest.row.name,
                  run: newest.row.kind === 'run',
                  order: newest.order,
                }
              : { root: root.row.name, record: root.row.name, run: false, order: root.order }
          })
          .reduce<{ root: string; record: string; run: boolean; order: number } | undefined>(
            (found, item) => (!found || item.order > found.order ? item : found),
            undefined,
          )
  return {
    ...(action && { action }),
    ...(Object.keys(running).length && { running }),
    ...(latest && { latest: { root: latest.root, record: latest.record, run: latest.run } }),
    complete: true,
  }
}

function pageFor(url: URL): ActivityPage {
  const query = url.searchParams
  const record = query.get('record')
  const expanded = query.has('tree') ? query.getAll('expand') : []
  const branches: ActivityPage['branches'] = {}
  const kept: string[] = []
  world.reads.push({ record, kept })
  for (const root of world.roots)
    if (root.kind === 'task' && expanded.includes(root.row.name)) {
      const stale = world.stale[root.row.name]
      if (stale) kept.push(root.row.name)
      branches[root.row.name] = branch(stale ?? taskRecords(root))
    }
  for (const model of world.runs()) {
    const task = model.row.task ?? ''
    if (expanded.includes(model.row.name) && (!task || expanded.includes(task)))
      branches[model.row.name] = branch(children(model))
  }
  const task = world.roots.find((root) => root.kind === 'task' && root.row.name === record)
  const tool = world.roots
    .flatMap((root) =>
      root.kind === 'task' ? [...root.tools.map(({ row }) => row), ...root.accepted] : [],
    )
    .find((row) => row.name === record)
  const owner = world
    .runs()
    .find(
      (model) =>
        record === model.row.name ||
        record === finalOf(model.row.name) ||
        model.calls.some((row) => row.name === record),
    )
  const call = owner?.calls.find((row) => row.name === record)
  const selectedRow: ActivityRow | null =
    task?.kind === 'task'
      ? task.row
      : tool
        ? tool
        : call
          ? (children(owner!).find((row) => row.name === record) ?? null)
          : owner
            ? runView(owner)
            : null
  const runDetail = (model: RunModel): RunDetail => ({
    kind: 'run',
    run: runView(model),
    request: null,
    source: 'export default async () => {}',
    linkedFunctions: [],
    terminal: model.failed
      ? { kind: 'failed', code: 'program_execution_failed', message: 'Failed', error: null }
      : model.ended
        ? { kind: 'succeeded', finalValue: '"done"' }
        : { kind: 'unavailable' },
  })
  // A waiting call selected with its approval opens that approval's review.
  const review: RequestDetail | null =
    call && owner?.waiting === call.name && query.get('request') === approvalOf(call.name)
      ? {
          ...approvalRequest,
          name: approvalOf(call.name),
          run: owner.row.name,
          invocation: call.name,
          callPath: call.callPath,
          state: 'pending',
          actionable: true,
        }
      : null
  return activityPage({
    rows: world.roots.map((root) => (root.kind === 'task' ? root.row : runView(root.run))),
    expanded,
    branches,
    selected: selectedRow ? record : null,
    selectedRow,
    // The loader names the followed Run's waiting call in every live scope.
    liveAction:
      query.has('live') && owner?.waiting
        ? { run: owner.row.name, record: owner.waiting, request: approvalOf(owner.waiting) }
        : undefined,
    // With no Run that runs open, the loader names the Run whose call started
    // waiting first, then each Task's newest Run that runs, among the Runs page live mode
    // may follow.
    liveLatest: query.get('live') === 'latest' && (!owner || owner.ended) ? latestOf() : undefined,
    detail:
      task?.kind === 'task'
        ? { kind: 'task', task: task.row }
        : tool
          ? { kind: 'tool', task: tool.task, interaction: tool, evidence: { kind: 'unknown' } }
          : call && selectedRow?.kind === 'invocation'
            ? {
                kind: 'invocation',
                invocation: selectedRow,
                arguments: '{}',
                result: { kind: 'upstream_outcome', value: '{}' },
                requests: review ? [{ kind: 'approval', name: review.name }] : [],
                request: review,
                requestUnavailable: false,
                context: runDetail(owner!),
              }
            : owner
              ? runDetail(owner)
              : null,
  })
}

function ScopesRoute() {
  const page = useLoaderData<ActivityPage>()
  const navigate = useNavigate()
  const { search } = useLocation()
  const { state } = useNavigation()
  const revalidator = useRevalidator()
  const change = (apply: () => void) => {
    apply()
    void revalidator.revalidate()
  }
  useEffect(() => {
    world.revalidate = () => void revalidator.revalidate()
    world.navigate = (to) => void navigate(to)
  })
  const button = (label: string, apply: () => void) => (
    <button type="button" onClick={() => change(apply)}>
      {label}
    </button>
  )
  return (
    <ActionReviewSession
      request={
        page.detail?.kind === 'run' || page.detail?.kind === 'invocation'
          ? (page.detail.request ?? null)
          : null
      }
    >
      <code aria-label="Address">{search}</code>
      <output aria-label="Navigation">{state === 'idle' ? revalidator.state : state}</output>
      <button type="button" onClick={() => navigate(-1)}>
        History back
      </button>
      {button('Add task', () => world.change('task'))}
      {button('Add standalone run', () => world.change('standalone'))}
      {button('Add run to newest task', () => world.change('run'))}
      {button('Add run to oldest task', () => world.change('run oldest'))}
      {button('Ask in newest running run', () => world.change('ask'))}
      {button('Add search to newest task', () => world.change('search'))}
      {button('Add describe to newest task', () => world.change('describe'))}
      {button('Submit exec to newest task', () => world.change('submit'))}
      {button('Accept submission', () => world.change('accept'))}
      {button('Add call', () => world.change('call'))}
      {button('Approve elsewhere', () => world.change('approve'))}
      {button('End first running run', () => world.change('end'))}
      {button('End oldest running run', () => world.change('end oldest'))}
      {button('Fail oldest running run', () => world.change('fail oldest'))}
      {button('Complete newest task', () => world.change('complete'))}
      {button('End oldest task', () => world.change('end oldest task'))}
      <Activity
        page={page}
        windowKey="live-scopes"
        live={{ connected: true, followRoot: true, atStart() {}, viewport() {} }}
      />
    </ActionReviewSession>
  )
}

const list = '/workspaces/finance/activity'
const meta = {
  title: 'Activity/Live scopes',
  parameters: { layout: 'fullscreen', initialEntry: list },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: ScopesRoute,
          HydrateFallback: () => null,
          loader: ({ request }) => pageFor(new URL(request.url)),
        },
      ])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub initialEntries={[context.parameters.initialEntry]} />
          </PageLayout>
        </div>
      )
    },
  ],
  render: () => <></>,
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>
type Play = Parameters<NonNullable<Story['play']>>[0]

/** A wait long enough for page live mode to show the previous record for its minimum time. */
const stays = { timeout: MIN_LIVE_RECORD_STAY_MS + 2000 }

async function surface({ canvasElement, userEvent }: Play) {
  const canvas = within(canvasElement)
  const screen = within(canvasElement.ownerDocument.body)
  const table = await canvas.findByRole('table', { name: 'Activity records', hidden: true })
  const rail = () => screen.getByRole('dialog', { name: /^(Activity details|Review action)$/ })
  const address = () => new URLSearchParams(canvas.getByLabelText('Address').textContent ?? '')
  return {
    canvas,
    rail,
    address,
    /** The toggle above the list. A docked rail sits in the page too, so leave out its own. */
    latest: () =>
      canvas
        .getAllByRole('button', { name: 'Live' })
        .find((button) => !button.closest('[role="dialog"]'))!,
    railToggle: (name: string) => within(rail()).getByRole('button', { name }),
    press: (name: string) => userEvent.click(canvas.getByRole('button', { name })),
    idle: async () => {
      await waitFor(() => expect(canvas.getByLabelText('Navigation')).toHaveTextContent('idle'))
      await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
    },
    /**
     * Waits until the rail heads with `heading` and the table selects `row`, and only it,
     * which live mode does once the record before showed long enough.
     */
    selects: (heading: string, row: string) =>
      waitFor(() => {
        expect(within(rail()).getByRole('heading', { name: heading, level: 2 })).toBeVisible()
        const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
        expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([row])
      }, stays),
    /** The rail reviews the approval of the call `call`, and the table selects it. */
    reviews: (call: string) =>
      waitFor(() => {
        expect(within(rail()).getByRole('region', { name: `Review ${call}` })).toBeVisible()
        const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
        expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([call])
      }, stays),
    /** The rail is closed and nothing is selected. */
    closed: () =>
      waitFor(() => {
        expect(screen.queryByRole('dialog', { name: /^Activity details$/ })).toBeNull()
        expect(address().has('record')).toBe(false)
      }),
  }
}

export const OpensActivityThenStaysOnIt: Story = {
  parameters: { initialEntry: `${list}?filter=kept` },
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, closed, idle, canvas } = await surface(context)
    const before = address().toString()

    // With no record yet, live mode waits, and the rail stays closed.
    await userEvent.click(latest())
    await waitFor(() => expect(latest()).toHaveAttribute('aria-pressed', 'true'))
    // Pressed, it keeps its label; its description says pressing it again stops.
    await expect(latest()).toHaveTextContent(/^Live$/)
    await expect(latest()).toHaveAccessibleDescription('Stop following the latest activity.')
    // On, the target icon is filled in; off again, it opens.
    await expect(latest().querySelector('svg')).toHaveClass('lucide-locate-fixed')
    await expect(address().get('live')).toBe('latest')
    await idle()
    await closed()

    // A new Task without a Run opens in the rail. Focus stays on the control you used.
    await press('Add task')
    await selects('Task t1', 'Task t1')
    await expect(canvas.getByRole('button', { name: 'Add task' })).toHaveFocus()
    latest().focus()
    await press('Add run to newest task')
    await selects('Run r2', 'run r2')
    // Then it follows the Run's newest record, as live mode on a Run does.
    await press('Add call')
    await selects('call.r2-op-3', 'call.r2-op-3')

    // A newer Run or Task never takes over the Run you have open.
    await press('Add standalone run')
    await press('Add task')
    await idle()
    await selects('call.r2-op-3', 'call.r2-op-3')
    await expect(address().get('live')).toBe('latest')
    await expect(address().get('filter')).toBe('kept')

    // Following added no history entries: Back returns to before you turned it on.
    await press('History back')
    await closed()
    await expect(address().toString()).toBe(before)
    await waitFor(() => expect(latest()).toHaveAttribute('aria-pressed', 'false'))
    await expect(latest().querySelector('svg')).toHaveClass('lucide-locate')
  },
}

const kept = taskRow('kept').name
export const KeepsWhatYouExpanded: Story = {
  // You expanded the Task yourself.
  parameters: { initialEntry: `${list}?tree=1&expand=${kept}` },
  beforeEach: () => {
    const run = newRun('k1', kept)
    run.ended = true
    world.reset([newTask('kept', [run])])
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle, canvas } = await surface(context)
    const task = kept

    // The Task's newest record is its Run that ended: live mode opens the Run itself.
    // Live mode owns only the Run it expanded.
    await userEvent.click(latest())
    await selects('Run k1', 'run k1')
    await expect(address().getAll('expand')).toEqual([task, runRow('k1', task).name])
    await expect(address().getAll('liveExpanded')).toEqual([runRow('k1', task).name])

    // The open Task opens its own new Run, and collapses only the Run it expanded.
    await press('Add run to newest task')
    await selects('Run r1', 'run r1')
    await expect(address().getAll('liveExpanded')).toEqual([runRow('r1', task).name])

    // The Run's end shows the Run itself. A Run that runs elsewhere then takes over,
    // although the Task has not ended, and live mode collapses only the Run it expanded.
    await press('End first running run')
    await selects('Run r1', 'run r1')
    await press('Add standalone run')
    await selects('Run s2', 'run s2')
    await expect(address().get('record')).toBe(runRow('s2', '').name)
    await expect(address().getAll('expand')).toEqual([task, runRow('s2', '').name])

    // A Run you pick opens. As before, picking a record turns live mode off, and keeps
    // what live mode expanded open. Later activity never takes the selection back.
    await userEvent.click(canvas.getByRole('link', { name: 'run r1' }))
    await selects('Run r1', 'run r1')
    await waitFor(() => expect(address().has('live')).toBe(false))
    await expect(address().has('liveExpanded')).toBe(false)
    await expect(address().getAll('expand')).toContain(runRow('s2', '').name)
    await press('Add standalone run')
    await idle()
    await selects('Run r1', 'run r1')
    await expect(address().get('record')).toBe(runRow('r1', task).name)
  },
}

const alpha = taskRow('alpha').name
export const MovesOnWithinItsTaskOnceItsRunEnds: Story = {
  beforeEach: () => {
    const run = newRun('a1', alpha)
    world.reset([newTask('alpha', [run])])
    world.operate(run)
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle, canvas } = await surface(context)
    const first = invocationRow('', 'a1-op-1').name

    await userEvent.click(latest())
    await selects('call.a1-op-1', 'call.a1-op-1')

    // A newer Run of the same Task never takes over a Run that still runs.
    await press('Add run to newest task')
    await idle()
    await selects('call.a1-op-1', 'call.a1-op-1')
    await expect(address().get('record')).toBe(first)

    // Once that Run ends, live mode follows the Task's Run that runs.
    await press('End oldest running run')
    await selects('Run r2', 'run r2')
    await expect(address().get('record')).toBe(runRow('r2', alpha).name)
    await expect(address().get('live')).toBe('latest')

    // A newer Search of the Task never takes over the Run either. Once the Run ends,
    // live mode follows the Search.
    await press('Add search to newest task')
    await idle()
    await selects('Run r2', 'run r2')
    await press('End oldest running run')
    await selects('search 3', 'search 3')
    await expect(address().get('record')).toBe(toolRow(alpha, 'search-3').name)
    await expect(address().get('live')).toBe('latest')

    // A record you pick turns live mode off. Later activity never takes it back.
    await userEvent.click(canvas.getByRole('link', { name: 'run r2' }))
    await selects('Run r2', 'run r2')
    await waitFor(() => expect(address().has('live')).toBe(false))
    await press('Add run to newest task')
    await press('Add describe to newest task')
    await idle()
    await selects('Run r2', 'run r2')
    await expect(address().get('record')).toBe(runRow('r2', alpha).name)
  },
}

export const MovesToNewerWorkOnceItsRunEnds: Story = {
  beforeEach: () => {
    const old = newRun('old')
    world.reset([{ kind: 'run', run: old }])
    world.operate(old)
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, rail, address, press, selects, closed, idle } = await surface(context)

    await userEvent.click(latest())
    await selects('call.old-op-1', 'call.old-op-1')

    // A newer Run never takes over a Run that still runs.
    await press('Add standalone run')
    await idle()
    await selects('call.old-op-1', 'call.old-op-1')
    await expect(address().get('record')).toBe(invocationRow('', 'old-op-1').name)

    // Once it ends, live mode follows the newer Run.
    await press('End oldest running run')
    await selects('Run s2', 'run s2')
    await expect(address().get('record')).toBe(runRow('s2', '').name)

    // With nothing else that runs, live mode stays on and rests on the ended Run.
    await press('End oldest running run')
    await selects('Run s2', 'run s2')
    await idle()
    await expect(address().get('record')).toBe(runRow('s2', '').name)
    await expect(address().get('live')).toBe('latest')
    await expect(latest()).toHaveAttribute('aria-pressed', 'true')

    // A Run that starts later opens, although the ended Run is still shown.
    await press('Add standalone run')
    await selects('Run s3', 'run s3')
    await expect(address().get('record')).toBe(runRow('s3', '').name)

    // Closing the rail turns live mode off. Later activity never opens it again.
    await userEvent.click(within(rail()).getByRole('button', { name: 'Close details' }))
    await closed()
    await expect(address().has('live')).toBe(false)
    await press('End oldest running run')
    await press('Add standalone run')
    await idle()
    await closed()
  },
}

export const FollowsTheRecordsOfATask: Story = {
  beforeEach: () => {
    world.reset([newTask('alpha')])
    world.change('search')
    world.change('describe')
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)

    // A Task without a Run opens at its newest record: the Describe after the Search.
    await userEvent.click(latest())
    await selects('describe 2', 'describe 2')
    await expect(address().get('record')).toBe(toolRow(alpha, 'describe-2').name)

    // Its next Search opens, then its new Run itself while the Run has no record yet.
    await press('Add search to newest task')
    await selects('search 3', 'search 3')
    await press('Add run to newest task')
    await selects('Run r4', 'run r4')
    await idle()
    await selects('Run r4', 'run r4')
    await expect(address().get('record')).toBe(runRow('r4', alpha).name)
    await expect(address().getAll('expand')).toEqual([alpha, runRow('r4', alpha).name])

    // A newer Search or Describe of the Task never takes over the Run that runs.
    await press('Add search to newest task')
    await press('Add describe to newest task')
    await idle()
    await selects('Run r4', 'run r4')
    await expect(address().get('live')).toBe('latest')
  },
}

// A Run of the Task opens at its first record. A newer Search or Describe never takes
// over while the Run runs, and once it ends, live mode follows the Task's newest record.
export const FollowsARunsFirstRecordThenItsTask: Story = {
  beforeEach: () => {
    world.reset([newTask('alpha', [newRun('r1', alpha, 1)])])
    // Each record the play adds is newer than the Run.
    world.count = 1
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    await userEvent.click(latest())
    await selects('Run r1', 'run r1')

    await press('Add call')
    await selects('call.r1-op-2', 'call.r1-op-2')
    await expect(address().get('record')).toBe(invocationRow('', 'r1-op-2').name)

    await press('Add search to newest task')
    await press('Add describe to newest task')
    await idle()
    await selects('call.r1-op-2', 'call.r1-op-2')

    await press('End oldest running run')
    await selects('describe 4', 'describe 4')
    await expect(address().get('record')).toBe(toolRow(alpha, 'describe-4').name)
    await expect(address().get('live')).toBe('latest')
  },
}

// An exec submission opens while Coral compiles it. Once Coral accepts it, its Run opens
// before the Run has any record, then the Run's first record.
export const FollowsAnExecSubmissionIntoItsRun: Story = {
  beforeEach: () => {
    world.reset([newTask('alpha')])
    world.change('search')
    world.change('submit')
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    await userEvent.click(latest())
    await selects('exec 2', 'exec 2')
    await expect(address().get('record')).toBe(toolRow(alpha, 'exec-2').name)

    await press('Accept submission')
    const accepted = runRow('e3', alpha).name
    await waitFor(() => expect(address().get('record')).toBe(accepted), stays)
    await idle()
    await selects('Run e3', 'exec 2')
    await expect(address().get('record')).toBe(accepted)

    await press('Add call')
    await selects('call.e3-op-4', 'call.e3-op-4')
    await expect(address().get('record')).toBe(invocationRow('', 'e3-op-4').name)
  },
}

export const ShowsTheNewestRecordUntilWorkRuns: Story = {
  beforeEach: () => {
    const done = newRun('done')
    done.ended = true
    const run = newRun('c1', taskRow('closed').name)
    run.ended = true
    const closed = newTask('closed', [run])
    if (closed.kind === 'task') closed.row = { ...closed.row, outcome: 'succeeded', endedAt: at }
    world.reset([{ kind: 'run', run: done }, closed])
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects } = await surface(context)

    // Every Task and Run has ended: live mode shows the newest Run itself and holds nothing.
    await userEvent.click(latest())
    await selects('Run done', 'run done')
    await expect(latest()).toHaveAttribute('aria-pressed', 'true')

    // A new Run that runs opens.
    await press('Add standalone run')
    await selects('Run s1', 'run s1')
    await expect(address().get('live')).toBe('latest')
  },
}

export const PassesTasksOlderThanAnHour: Story = {
  beforeEach: () => {
    const old = newTask('old', [newRun('o1', taskRow('old').name)], TAKS_MAX_AGE + 60_000)
    world.reset([old])
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, closed, idle } = await surface(context)

    // The Task started over an hour ago: live mode opens neither it nor its Run.
    await userEvent.click(latest())
    await waitFor(() => expect(latest()).toHaveAttribute('aria-pressed', 'true'))
    await idle()
    await closed()
    await expect(address().has('expand')).toBe(false)

    // A Task that starts now opens.
    await press('Add task')
    await selects('Task t1', 'Task t1')
  },
}

export const AnApprovalHoldsItsRun: Story = {
  beforeEach: () => {
    const run = newRun('a1', taskRow('alpha').name)
    world.reset([newTask('alpha', [run])])
    world.operate(run, true)
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, reviews, idle } = await surface(context)
    const waiting = 'call.a1-op-1'

    // The newest Run waits for your approval: live mode opens its review.
    await userEvent.click(latest())
    await waitFor(
      () => expect(address().get('request')).toBe(approvalOf(invocationRow('', 'a1-op-1').name)),
      stays,
    )
    await reviews(waiting)

    // Newer activity never takes the selection while the approval waits.
    await press('Add standalone run')
    await idle()
    await reviews(waiting)

    // Once it is decided, live mode stays on its Run, not the newer one.
    await press('Approve elsewhere')
    await idle()
    await expect(address().get('record')).toBe(invocationRow('', 'a1-op-1').name)
    await expect(address().get('live')).toBe('latest')
  },
}

const work = taskRow('work', 'Task work')
export const FollowsTheTaskYouOpen: Story = {
  parameters: { initialEntry: `${list}?record=${work.name}` },
  beforeEach: () => {
    const runs = ['w1', 'w2', 'w3'].map((id) => newRun(id, work.name))
    runs[0].ended = true
    world.reset([newTask('work', runs)])
    world.operate(runs[1])
  },
  play: async (context) => {
    const { userEvent } = context
    const { railToggle, address, press, selects, latest, idle } = await surface(context)
    await selects('Task work', 'Task work')

    // From the Task, live mode starts on its first Run that has not ended.
    await userEvent.click(railToggle('Follow this task'))
    await selects('call.w2-op-1', 'call.w2-op-1')
    await expect(address().get('live')).toBe('task')
    await expect(railToggle('Follow this task')).toHaveAttribute('aria-pressed', 'true')
    await expect(latest()).toHaveAttribute('aria-pressed', 'false')

    // When that Run ends, it selects the Run itself. It never moves on to the
    // next Run, or to a new one.
    await press('End first running run')
    await selects('Run w2', 'run w2')
    await press('Add run to newest task')
    await idle()
    await selects('Run w2', 'run w2')

    // The pressed toggle turns it off in place.
    await userEvent.click(railToggle('Follow this task'))
    await waitFor(() => expect(address().has('live')).toBe(false))
    await selects('Run w2', 'run w2')
  },
}

export const OneScopeAtATime: Story = {
  parameters: {
    initialEntry: `${list}?tree=1&expand=${runRow('solo', '').name}&record=${runRow('solo', '').name}&live=1`,
  },
  beforeEach: () => {
    const solo = newRun('solo')
    world.reset([
      { kind: 'run', run: newRun('newer') },
      { kind: 'run', run: solo },
    ])
    world.operate(solo)
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, railToggle, rail, address, press, selects, closed, idle } =
      await surface(context)
    // Live mode on a Run follows that Run, not the newer one.
    await selects('call.solo-op-1', 'call.solo-op-1')

    // The page scope replaces it, and stays on the Run you have open, not the newer one.
    await userEvent.click(latest())
    await waitFor(() => expect(address().get('live')).toBe('latest'))
    await idle()
    await selects('call.solo-op-1', 'call.solo-op-1')
    await expect(railToggle('Follow latest activity')).toHaveAttribute('aria-pressed', 'true')
    await expect(within(rail()).queryByRole('button', { name: 'Follow latest record' })).toBeNull()

    // Closing the rail turns live mode off. New activity then opens nothing, and the
    // rail does not open again.
    await userEvent.click(within(rail()).getByRole('button', { name: 'Close details' }))
    await closed()
    await expect(address().has('live')).toBe(false)
    await waitFor(() => expect(latest()).toHaveAttribute('aria-pressed', 'false'))
    await press('Add standalone run')
    await idle()
    await closed()
  },
}

// You expanded 19 records. The newest Task and its Run do not fit together.
const yours = Array.from({ length: 19 }, (_, index) => `expand=${taskRow(`yours-${index}`).name}`)
export const StopsAtTheExpansionLimit: Story = {
  parameters: { initialEntry: `${list}?tree=1&${yours.join('&')}` },
  beforeEach: () => world.reset([newTask('alpha', [newRun('a1', taskRow('alpha').name)])]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, closed, idle } = await surface(context)
    const screen = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(latest())
    await idle()
    await closed()
    // Live mode expands neither, and its toggle says why.
    await expect(address().getAll('expand')).toHaveLength(19)
    await userEvent.hover(latest())
    await expect(
      // The tooltip itself, not the toggle's hidden description.
      await screen.findByText('Collapse a record before you follow the latest activity.', {
        selector: ':not([id])',
      }),
    ).toBeVisible()
    await expect(latest()).toHaveAccessibleDescription(
      'Collapse a record before you follow the latest activity.',
    )
    // Pressed, the toggle still turns live mode off.
    await expect(latest()).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(latest())
    await waitFor(() => expect(address().has('live')).toBe(false))
  },
}

export const HoldsOnlyARunThatRuns: Story = {
  beforeEach: () => {
    // `b1` waits for your approval. `b2` runs, and is newer.
    const waits = newRun('b1', alpha)
    const newer = newRun('b2', alpha, 1)
    world.reset([newTask('alpha', [waits, newer])])
    world.operate(waits, true)
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, reviews, idle } = await surface(context)
    const asked = invocationRow('', 'b1-op-1').name

    // The Run that waits for your action opens first, at its review, not the newer Run.
    await userEvent.click(latest())
    await waitFor(() => expect(address().get('request')).toBe(approvalOf(asked)), stays)
    await reviews('call.b1-op-1')
    await expect(address().get('record')).toBe(asked)

    // Once it is decided, live mode holds that Run while it runs, even against a
    // newer Run of the Task or a Run elsewhere.
    await press('Approve elsewhere')
    await press('Add standalone run')
    await idle()
    await selects('call.b1-op-1', 'call.b1-op-1')
    await expect(address().get('record')).toBe(asked)

    // When it ends, the Run submitted last opens: the one elsewhere, although the Task
    // has not ended and still runs `b2`.
    await press('End oldest running run')
    await selects('Run s2', 'run s2')
    await expect(address().get('record')).toBe(runRow('s2', '').name)
    await expect(address().get('live')).toBe('latest')
  },
}

const past = taskRow('past').name
// Page live mode shows an old record, then follows new work with no further click, and
// holds only a Run that runs.
export const FollowsNewWorkFromWhatItShows: Story = {
  beforeEach: () => {
    const run = newRun('p1', past)
    run.ended = true
    world.reset([newTask('past', [run])])
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    const pressed = () => expect(latest()).toHaveAttribute('aria-pressed', 'true')
    const opened = async (heading: string, row: string, record: string) => {
      await selects(heading, row)
      await expect(address().get('record')).toBe(record)
      await expect(address().get('live')).toBe('latest')
      await pressed()
    }

    // Nothing runs: live mode shows the newest record, in a Task that has not ended.
    await userEvent.click(latest())
    await opened('Run p1', 'run p1', runRow('p1', past).name)

    // A new Task arrives with a Search, a Describe and a Run that calls an operation. You
    // click nothing more. Once the old record showed long enough, live mode opens the
    // newest of that work, the Run's call, and skips the records in between.
    await press('Add task')
    await press('Add search to newest task')
    await press('Add describe to newest task')
    await press('Add run to newest task')
    await press('Add call')
    const call = invocationRow('', 'r4-op-5').name
    await opened('call.r4-op-5', 'call.r4-op-5', call)
    // The loader only ever opened the list, the old record and the call.
    await expect(new Set(world.reads.map(({ record }) => record))).toEqual(
      new Set([null, runRow('p1', past).name, call]),
    )

    // The Run holds live mode while it runs: a newer Task, or a Run submitted later in
    // the old Task, never takes over.
    await press('Add task')
    await press('Add run to oldest task')
    await idle()
    await opened('call.r4-op-5', 'call.r4-op-5', call)

    // Once it ends, live mode finds the Run that runs in the old Task.
    await press('End first running run')
    await opened('Run r7', 'run r7', runRow('r7', past).name)
  },
}

// An action elsewhere waits while the Run page live mode holds runs. Once that Run
// ends, the action goes first, before a Run submitted later.
export const AnActionGoesFirstOnceItsRunEnds: Story = {
  beforeEach: () => world.reset([newTask('past', [newRun('p0', past)])]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, reviews, idle } = await surface(context)
    const held = runRow('p0', past).name
    await userEvent.click(latest())
    await selects('Run p0', 'run p0')

    await press('Add task')
    await press('Add run to newest task')
    await press('Ask in newest running run')
    await press('Add standalone run')
    await idle()
    await selects('Run p0', 'run p0')
    await expect(address().get('record')).toBe(held)

    await press('End oldest running run')
    const waits = invocationRow('', 'r2-op-3').name
    await waitFor(() => expect(address().get('request')).toBe(approvalOf(waits)), stays)
    await reviews('call.r2-op-3')
    await expect(address().get('record')).toBe(waits)
    await expect(address().get('live')).toBe('latest')
  },
}

/** The older Task, whose window the list keeps once it ends. */
const keptTask = taskRow('older').name
/** The newer Task, where the next Run starts. */
const nextTask = taskRow('newer').name
// Once a Run and its Task end, the list can keep a window of the Task that still says
// the Run runs. Page live mode never reopens that Run, and follows the next one.
export const PassesARunThatAKeptWindowSaysRuns: Story = {
  // You expanded the older Task.
  parameters: { initialEntry: `${list}?tree=1&expand=${encodeURIComponent(keptTask)}` },
  beforeEach: () => {
    // `a` runs in the older Task, the newer Task has no record yet, and `a` was
    // submitted after every Run the newer Task will start.
    world.reset([newTask('newer', [], 0, 10), newTask('older', [newRun('a', keptTask, 9)])])
  },
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    const a = runRow('a', keptTask).name
    await userEvent.click(latest())
    await selects('Run a', 'run a')

    // `a` and its Task end. The older Task's window still says `a` runs. Nothing runs,
    // so live mode shows the newest record and stays there.
    await press('End oldest task')
    await selects('Task newer', 'Task newer')
    await idle()
    await selects('Task newer', 'Task newer')
    await expect(address().get('record')).toBe(nextTask)

    // A new Run of the newer Task opens, with no further click.
    await press('Add run to newest task')
    await selects('Run r1', 'run r1')
    await expect(address().get('record')).toBe(runRow('r1', nextTask).name)
    await expect(address().get('live')).toBe('latest')
    // From the newer Task on, every read served the kept window that says `a` runs,
    // and live mode never opened `a` again. On screen, a reopened Run gives way at
    // once, so the loader's reads are what show it.
    const first = world.reads.findIndex(({ record }) => record === nextTask)
    await expect(first).toBeGreaterThanOrEqual(0)
    const reads = world.reads.slice(first)
    await expect(reads.filter((read) => !read.kept.includes(keptTask))).toEqual([])
    await expect(reads.filter(({ record }) => record?.startsWith(a))).toEqual([])
  },
}

/**
 * The Run you follow ends, then a new Run of the same Task starts without any record.
 * Live mode rests on the ended Run itself after you `press` the end, then opens the new
 * Run itself. navigation.test.ts owns the target rule, and
 * FollowsARunsFirstRecordThenItsTask a Run's first record.
 */
function endsThenANewRunStarts(press: string): Story {
  return {
    beforeEach: () => {
      const run = newRun('a1', alpha)
      world.reset([newTask('alpha', [run])])
      world.operate(run)
    },
    play: async (context) => {
      const { userEvent } = context
      const { latest, address, press: click, selects, idle } = await surface(context)
      await userEvent.click(latest())
      await selects('call.a1-op-1', 'call.a1-op-1')

      // The Run ends. Live mode stays on and rests on the Run itself.
      await click(press)
      await idle()
      await selects('Run a1', 'run a1')
      await expect(address().get('record')).toBe(runRow('a1', alpha).name)
      await expect(address().get('live')).toBe('latest')

      // A new Run of the same Task opens before it has any record.
      await click('Add run to newest task')
      await selects('Run r2', 'run r2')
      await idle()
      await selects('Run r2', 'run r2')
      await expect(address().get('record')).toBe(runRow('r2', alpha).name)
      await expect(address().get('live')).toBe('latest')
    },
  }
}
// Both successful and failed Runs return to the Run before later work arrives.
export const ASucceededRunGivesWayToANewRun = endsThenANewRunStarts('End oldest running run')
export const AFailedRunGivesWayToANewRun = endsThenANewRunStarts('Fail oldest running run')

/** Opens the selected operation's response in its expanded viewer, with `find` in Find. */
async function inspect({ canvasElement, userEvent }: Play, find: string) {
  const page = within(canvasElement.ownerDocument.body)
  const rail = page.getByRole('dialog', { name: 'Activity details' })
  const expand = within(rail).getByRole('button', { name: 'Expand Provider response' })
  await userEvent.click(expand)
  const viewer = await page.findByRole('dialog', { name: 'Provider response' })
  await userEvent.type(within(viewer).getByRole('searchbox'), find)
  return { expand, viewer }
}
/** Changes what Coral holds while the expanded viewer covers the route's buttons. */
function arrive(kind: Parameters<typeof world.change>[0]) {
  world.change(kind)
  world.revalidate()
}

// Catches: live mode moving on while you inspect a payload expanded, which swaps the
// record and closes the viewer; the list no longer taking new activity meanwhile; or live
// mode staying held, or turned off, once you close the viewer.
export const HoldsWhileYouInspectAPayload: Story = {
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle, canvas } = await surface(context)
    const page = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(latest())
    await press('Add task')
    await selects('Task t1', 'Task t1')
    await press('Add run to newest task')
    await selects('Run r2', 'run r2')
    await press('Add operation')
    await selects('call.r2-op-3', 'call.r2-op-3')
    const inspected = address().get('record')
    const { expand, viewer } = await inspect(context, 'x')

    // A new operation arrives and the Run ends. The list takes both; the record stays.
    arrive('operation')
    await idle()
    arrive('end')
    await idle()
    await waitFor(() =>
      expect(canvas.getByRole('link', { name: 'call.r2-op-4', hidden: true })).toBeInTheDocument(),
    )
    await expect(address().get('record')).toBe(inspected)
    await expect(address().get('live')).toBe('latest')
    // Behind the open viewer, the toggle stays pressed.
    await expect(canvas.getByRole('button', { name: 'Live', hidden: true })).toHaveAttribute(
      'aria-pressed',
      'true',
    )
    // The same viewer stays open, as you left it.
    await expect(page.getByRole('dialog', { name: 'Provider response' })).toBe(viewer)
    await expect(within(viewer).getByRole('searchbox')).toHaveValue('x')

    // Closed, focus is back on Expand, then live mode moves on to the Run's newest record.
    await userEvent.keyboard('{Escape}')
    await waitFor(() =>
      expect(page.queryByRole('dialog', { name: 'Provider response' })).toBeNull(),
    )
    await waitFor(() => expect(address().get('record')).not.toBe(inspected))
    await expect(expand.isConnected).toBe(false)
    await expect(address().get('live')).toBe('latest')
  },
}

// Catches: a close that turns live mode back on after you turned it off meanwhile.
export const StaysOffWhenLiveEndsWhileYouInspect: Story = {
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    const page = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(latest())
    await press('Add task')
    await press('Add run to newest task')
    await press('Add operation')
    await selects('call.r2-op-3', 'call.r2-op-3')
    const inspected = address().get('record')
    const { expand } = await inspect(context, 'a')
    // Live mode ends while you inspect: its address drops it, as its toggle does.
    const off = new URLSearchParams(address())
    off.delete('live')
    world.navigate(`?${off}`)
    await waitFor(() => expect(address().has('live')).toBe(false))
    arrive('operation')
    await idle()
    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(expand).toHaveFocus())
    await idle()
    // Nothing moves on, and live mode stays off.
    await expect(address().get('record')).toBe(inspected)
    await expect(latest()).toHaveAttribute('aria-pressed', 'false')
    await expect(page.queryByRole('dialog', { name: 'Provider response' })).toBeNull()
  },
}

// Catches: a viewer that goes away open, as when you go back to another record, and leaves
// live mode held for good.
export const ResumesWhenTheOpenViewerGoesAway: Story = {
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, idle } = await surface(context)
    const page = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(latest())
    await press('Add task')
    await press('Add run to newest task')
    await press('Add operation')
    await selects('call.r2-op-3', 'call.r2-op-3')
    await inspect(context, 'a')
    // Another record opens in live mode, so the open viewer unmounts with its record.
    const run = new URLSearchParams(address())
    run.set('record', runRow('r2', '').name)
    world.navigate(`?${run}`)
    await waitFor(() =>
      expect(page.queryByRole('dialog', { name: 'Provider response' })).toBeNull(),
    )
    await idle()
    // Live mode holds no more: a new operation opens as it arrives.
    await press('Add operation')
    await selects('call.r2-op-4', 'call.r2-op-4')
    await expect(address().get('live')).toBe('latest')
  },
}

// Catches: an approval request that arrives while you inspect a payload expanded taking you
// to its review, which swaps the record and closes the viewer.
export const HoldsAnApprovalWhileYouInspect: Story = {
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, press, selects, reviews, idle, canvas } = await surface(context)
    const page = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(latest())
    await press('Add task')
    await press('Add run to newest task')
    await press('Add operation')
    await selects('call.r2-op-3', 'call.r2-op-3')
    const inspected = address().get('record')
    const { viewer } = await inspect(context, 'x')
    // An operation in the Run now waits for your approval. The list shows it; the record stays.
    arrive('ask')
    await idle()
    await waitFor(() =>
      expect(canvas.getByRole('link', { name: 'call.r2-op-4', hidden: true })).toBeInTheDocument(),
    )
    await expect(address().get('record')).toBe(inspected)
    await expect(address().has('request')).toBe(false)
    await expect(page.getByRole('dialog', { name: 'Provider response' })).toBe(viewer)
    // Closed, live mode opens the review.
    await userEvent.keyboard('{Escape}')
    await reviews('call.r2-op-4')
  },
}
