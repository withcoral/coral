import type { Meta, StoryObj } from '@storybook/react'
import {
  createRoutesStub,
  useLoaderData,
  useLocation,
  useNavigate,
  useNavigation,
  useRevalidator,
} from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { TAKS_MAX_AGE } from './navigation'
import { ActionReviewSession } from '../actions/review'
import {
  activityPage,
  approvalRequest,
  invocationRow,
  runRow,
  taskRow,
  workspace,
} from './fixtures'
import type { ActivityPage, ActivityRow, RequestDetail, RunDetail } from './types'

// Live mode's scopes in Chromium: the newest activity from the list, and one Task's
// Runs in order, while Tasks, Runs and records arrive. The target rule is
// navigation.test.ts's; live.stories.tsx owns how a followed Run moves.

const at = '2026-09-18T14:02:00.000Z'
type Run = Extract<ActivityRow, { kind: 'run' }>
type Operation = Extract<ActivityRow, { kind: 'invocation' }>
interface RunModel {
  row: Run
  operations: Operation[]
  ended: boolean
  /** The operation that waits for your approval, if any. */
  waiting: string | null
}
type Root =
  | { kind: 'task'; row: Extract<ActivityRow, { kind: 'task' }>; runs: RunModel[] }
  | { kind: 'run'; run: RunModel }

const approvalOf = (operation: string) => `${workspace}/approvals/${operation.split('/').at(-1)}`
const finalOf = (run: string) => `${run}/final`

function newRun(id: string, task = ''): RunModel {
  return {
    row: runRow(id, task, { actor: `run ${id}`, intent: `Run ${id}`, state: 'running' }),
    operations: [],
    ended: false,
    waiting: null,
  }
}
/** A Task that started `age` milliseconds ago: just now, unless a story says otherwise. */
function newTask(id: string, runs: RunModel[] = [], age = 0): Root {
  const startedAt = new Date(Date.now() - age).toISOString()
  return { kind: 'task', row: { ...taskRow(id, `Task ${id}`), startedAt }, runs }
}

/** What Coral holds, newest top-level record first. A play changes it, then revalidates. */
const world = {
  roots: [] as Root[],
  count: 0,
  reset(roots: Root[]) {
    this.roots = roots
    this.count = 0
  },
  runs(): RunModel[] {
    return this.roots.flatMap((root) => (root.kind === 'task' ? root.runs : [root.run]))
  },
  /** What a play's button changes: a new record, a decision elsewhere, or a Run's end. */
  change(kind: 'task' | 'standalone' | 'run' | 'operation' | 'approve' | 'end') {
    const runs = this.runs()
    if (kind === 'task') this.roots.unshift(newTask(`t${++this.count}`))
    if (kind === 'standalone') this.roots.unshift({ kind: 'run', run: newRun(`s${++this.count}`) })
    const task = this.roots.find((root) => root.kind === 'task')
    if (kind === 'run' && task?.kind === 'task')
      task.runs.push(newRun(`r${++this.count}`, task.row.name))
    // The latest Run by name: each new one's number is higher.
    const latest = runs.toSorted((a, b) => a.row.name.localeCompare(b.row.name)).at(-1)
    if (kind === 'operation' && latest) this.operate(latest)
    if (kind === 'approve') for (const model of runs) model.waiting = null
    const running = runs.find((model) => !model.ended)
    if (kind === 'end' && running) running.ended = true
  },
  /** Adds an operation to `run`, which waits for your approval if `waits`. */
  operate(run: RunModel, waits = false) {
    const id = `${run.row.name.split('/').at(-1)}-op-${++this.count}`
    const operation = invocationRow(run.row.name, id, {
      task: run.row.task,
      callPath: `call.${id}`,
      operation: `call.${id}`,
    })
    run.operations.push(operation)
    if (waits) run.waiting = operation.name
  },
}

const runView = (model: RunModel): Run => ({
  ...model.row,
  state: model.ended ? 'succeeded' : 'running',
  endedAt: model.ended ? at : null,
  requiresAction: Boolean(model.waiting),
  hasFinalValue: model.ended,
})
function children(model: RunModel): ActivityRow[] {
  const operations = model.operations.map((row): ActivityRow =>
    row.name === model.waiting ? { ...row, state: 'pending', requiresAction: true } : row,
  )
  if (!model.ended) return operations
  return [
    ...operations,
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
const branch = (rows: ActivityRow[]) => ({
  rows: rows.map((row, position) => ({ ...row, position })),
  total: String(rows.length),
  next: '',
  unavailable: false,
})

function pageFor(url: URL): ActivityPage {
  const query = url.searchParams
  const record = query.get('record')
  const expanded = query.has('tree') ? query.getAll('expand') : []
  const branches: ActivityPage['branches'] = {}
  for (const root of world.roots)
    if (root.kind === 'task' && expanded.includes(root.row.name))
      branches[root.row.name] = branch(root.runs.map(runView))
  for (const model of world.runs()) {
    const task = model.row.task ?? ''
    if (expanded.includes(model.row.name) && (!task || expanded.includes(task)))
      branches[model.row.name] = branch(children(model))
  }
  const task = world.roots.find((root) => root.kind === 'task' && root.row.name === record)
  const owner = world
    .runs()
    .find(
      (model) =>
        record === model.row.name ||
        record === finalOf(model.row.name) ||
        model.operations.some((row) => row.name === record),
    )
  const operation = owner?.operations.find((row) => row.name === record)
  const selectedRow: ActivityRow | null =
    task?.kind === 'task'
      ? task.row
      : operation
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
    terminal: model.ended ? { kind: 'succeeded', finalValue: '"done"' } : { kind: 'unavailable' },
  })
  // A waiting operation selected with its approval opens that approval's review.
  const review: RequestDetail | null =
    operation &&
    owner?.waiting === operation.name &&
    query.get('request') === approvalOf(operation.name)
      ? {
          ...approvalRequest,
          name: approvalOf(operation.name),
          run: owner.row.name,
          invocation: operation.name,
          callPath: operation.callPath,
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
    // The loader names the followed Run's waiting operation in every live scope.
    liveAction:
      query.has('live') && owner?.waiting
        ? { run: owner.row.name, record: owner.waiting, request: approvalOf(owner.waiting) }
        : undefined,
    detail:
      task?.kind === 'task'
        ? { kind: 'task', task: task.row }
        : operation && selectedRow?.kind === 'invocation'
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
      {button('Add operation', () => world.change('operation'))}
      {button('Approve elsewhere', () => world.change('approve'))}
      {button('End first running run', () => world.change('end'))}
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
    /** The toggle above the list. */
    latest: () => canvas.getByRole('button', { name: 'Follow latest activity' }),
    railToggle: (name: string) => within(rail()).getByRole('button', { name }),
    press: (name: string) => userEvent.click(canvas.getByRole('button', { name })),
    idle: async () => {
      await waitFor(() => expect(canvas.getByLabelText('Navigation')).toHaveTextContent('idle'))
      await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
    },
    /** Waits until the rail heads with `heading` and the table selects `row`, and only it. */
    selects: (heading: string, row: string) =>
      waitFor(() => {
        expect(within(rail()).getByRole('heading', { name: heading, level: 2 })).toBeVisible()
        const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
        expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([row])
      }),
    /** The rail reviews the approval of the operation `call`, and the table selects it. */
    reviews: (call: string) =>
      waitFor(() => {
        expect(within(rail()).getByRole('region', { name: `Review ${call}` })).toBeVisible()
        const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
        expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([call])
      }),
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
    await press('Add operation')
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

    // The Task's only Run has ended: live mode opens the Task, not that Run.
    await userEvent.click(latest())
    await selects('Task kept', 'Task kept')
    await expect(address().getAll('expand')).toEqual([task])
    await expect(address().has('liveExpanded')).toBe(false)

    // The open Task opens its own new Run. Live mode owns only the Run it expanded.
    await press('Add run to newest task')
    await selects('Run r1', 'run r1')
    await expect(address().getAll('liveExpanded')).toEqual([runRow('r1', task).name])

    // The Run's end keeps it open on its final value, while newer work arrives.
    await press('End first running run')
    await selects('Run r1', 'final value')
    await press('Add standalone run')
    await idle()
    await selects('Run r1', 'final value')

    // A Run you pick opens. As before, picking a record turns live mode off, and keeps
    // what live mode expanded open.
    await userEvent.click(canvas.getByRole('link', { name: 'run s2' }))
    await selects('Run s2', 'run s2')
    await waitFor(() => expect(address().has('live')).toBe(false))
    await expect(address().has('liveExpanded')).toBe(false)
    await expect(address().getAll('expand')).toContain(runRow('r1', task).name)
  },
}

export const WaitsForWorkThatRuns: Story = {
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
    const { latest, address, press, selects, closed, idle } = await surface(context)

    // Every Task and Run has ended: live mode opens none of them, and waits.
    await userEvent.click(latest())
    await waitFor(() => expect(latest()).toHaveAttribute('aria-pressed', 'true'))
    await idle()
    await closed()
    await expect(address().has('expand')).toBe(false)

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
    await waitFor(() =>
      expect(address().get('request')).toBe(approvalOf(invocationRow('', 'a1-op-1').name)),
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

    // When that Run ends, it stays open on its final value. It never moves on to the
    // next Run, or to a new one.
    await press('End first running run')
    await selects('Run w2', 'final value')
    await press('Add run to newest task')
    await idle()
    await selects('Run w2', 'final value')

    // The pressed toggle turns it off in place.
    await userEvent.click(railToggle('Follow this task'))
    await waitFor(() => expect(address().has('live')).toBe(false))
    await selects('Run w2', 'final value')
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

// You expanded 19 records. The newest Task fits, but its Run does not.
const yours = Array.from({ length: 19 }, (_, index) => `expand=${taskRow(`yours-${index}`).name}`)
export const StopsAtTheExpansionLimit: Story = {
  parameters: { initialEntry: `${list}?tree=1&${yours.join('&')}` },
  beforeEach: () => world.reset([newTask('alpha', [newRun('a1', taskRow('alpha').name)])]),
  play: async (context) => {
    const { userEvent } = context
    const { latest, address, canvas, closed, idle } = await surface(context)
    await userEvent.click(latest())
    await waitFor(() => expect(address().getAll('expand')).toHaveLength(20))
    await idle()
    await closed()
    await expect(canvas.getByText('Collapse a record before you expand another.')).toBeVisible()
    // Pressed, the toggle still turns live mode off.
    await expect(latest()).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(latest())
    await waitFor(() => expect(address().has('live')).toBe(false))
  },
}
