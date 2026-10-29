import type { Meta, StoryObj } from '@storybook/react-vite'
import {
  createRoutesStub,
  useLoaderData,
  useLocation,
  useNavigate,
  useNavigation,
  useRevalidator,
} from 'react-router'
import { useEffect, useState } from 'react'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { recordAnimations } from '../../../.storybook/browser'
import {
  activityPage,
  approvalRequest,
  invocationRow,
  runRow,
  taskRow,
  workspace,
} from './fixtures'
import type {
  ActivityPage,
  ActivityRow,
  RecordSourceLocation,
  RequestDetail,
  RunEvent,
} from './types'

// Live mode in Chromium: records that arrive while the rail follows them, what the
// rail and table select, the address, and focus. The step rule is navigation.test.ts's.

const at = '2026-09-18T14:02:00.000Z'
const task = taskRow('reconcile', 'Reconcile invoices')
const program = runRow('program', task.name, {
  actor: 'Reminder run',
  intent: 'Send reminders',
  state: 'running',
  endedAt: null,
})
const other = runRow('other', '', {
  actor: 'Other run',
  intent: 'Other run',
  state: 'running',
  endedAt: null,
})
type Child = Extract<ActivityRow, { kind: 'invocation' | 'output' }>
type Event = { row: Extract<ActivityRow, { kind: 'output' }>; event: RunEvent }

// Every Run's accepted source. Each record carries the line Coral stored for its call.
const source = [
  'export default async () => {',
  '  await coral.providers.mail.send({})',
  "  console.info('checked')",
  "  coral.emit('sent')",
  '}',
].join('\n')
const lineOf = { invocation: 2, 'program.logged': 3, 'program.emitted': 4 } as const
const storedAt = (line: number): RecordSourceLocation => ({
  document: { kind: 'program' },
  line,
  column: 3,
})

function tool(
  run: string,
  sequence: number,
  callPath: string,
  overrides: Partial<Extract<Child, { kind: 'invocation' }>> = {},
): Child {
  return invocationRow(run, `${run.split('/').at(-1)}-${sequence}`, {
    task: run === program.name ? task.name : '',
    tool: callPath,
    callPath,
    sourceLocation: storedAt(lineOf.invocation),
    ...overrides,
  })
}
function output(run: string, sequence: number, kind: 'program.logged' | 'program.emitted'): Event {
  const event: RunEvent =
    kind === 'program.logged'
      ? { kind, sequence: String(sequence), occurredAt: at, level: 'info', message: 'checked' }
      : { kind, sequence: String(sequence), occurredAt: at, value: '"sent"' }
  return {
    row: {
      kind: 'output',
      name: `${run}/events/${sequence}`,
      workspace,
      run,
      label: `${kind === 'program.logged' ? 'log' : 'emission'} #${sequence}`,
      startedAt: at,
      eventKind: kind,
      agent: '',
      task: run === program.name ? task.name : '',
      sourceLocation: storedAt(lineOf[kind]),
    },
    event,
  }
}
const finalRow: Child = {
  ...output(program.name, 99, 'program.emitted').row,
  sourceLocation: undefined,
  name: `${program.name}/final`,
  label: 'final value',
  eventKind: 'run.ended',
}

/** A tool of the program that waits for your approval. */
function waiting(sequence: number, callPath: string): Child {
  return tool(program.name, sequence, callPath, { state: 'pending', requiresAction: true })
}
const approvalOf = (invocation: string) => `${workspace}/approvals/${invocation.split('/').at(-1)}`

/** What Coral holds. A play changes it, then revalidates, as a live snapshot would. */
const world = {
  children: [] as Child[],
  others: [] as Child[],
  events: new Map<string, RunEvent>(),
  ended: false,
  /** How many children one branch read returns. */
  window: 100,
  request: null as RequestDetail | null,
  /** Each waiting tool's approval state, by tool. */
  approvals: new Map<string, string>(),
  /**
   * How the next decision ends: saved, refused with an error, held until released, or
   * lost to another reviewer's opposite decision.
   */
  decision: 'saved' as 'saved' | 'error' | 'held' | 'lost',
  release: () => {},
  /** The records the address selected, in order. */
  visited: [] as (string | null)[],
  loads: 0,
  /** Adds a record without a click, so focus stays where the play put it. */
  arrive: (() => {}) as (child: Child | Event | 'end') => void,
  reset(
    children: (Child | Event)[],
    options: { window?: number; request?: RequestDetail; ended?: boolean } = {},
  ) {
    this.children = []
    this.others = []
    this.events.clear()
    this.approvals.clear()
    this.decision = 'saved'
    this.visited = []
    this.ended = options.ended ?? false
    this.window = options.window ?? 100
    this.request = options.request ?? null
    for (const child of children) this.add(child)
  },
  add(child: Child | Event, run = program.name) {
    const row = 'event' in child ? child.row : child
    if ('event' in child) this.events.set(row.name, child.event)
    if (row.kind === 'invocation' && row.requiresAction) this.approvals.set(row.name, 'pending')
    ;(run === program.name ? this.children : this.others).push(row)
  },
}

/**
 * The oldest approval you can decide in the program, as the loader names it while live
 * mode follows the program or a review shows a decided approval. Its tests own the rule.
 */
function liveActionFor(url: URL, reviewed: string | undefined) {
  const decided = reviewed !== undefined && reviewed !== 'pending'
  if (url.searchParams.get('live') !== '1' && !decided) return undefined
  const oldest = world.children
    .filter((row) => world.approvals.get(row.name) === 'pending')
    .toSorted((a, b) => a.startedAt.localeCompare(b.startedAt) || a.name.localeCompare(b.name))[0]
  return oldest && { run: program.name, record: oldest.name, request: approvalOf(oldest.name) }
}

/** The approval route: it saves, refuses or holds the decision as `world.decision` says. */
async function decide({ request }: { request: Request }) {
  const form = await request.formData()
  const name = String(form.get('name'))
  if (world.decision === 'error') return { error: 'The decision did not save. Try again.' }
  const call = [...world.approvals.keys()].find((row) => approvalOf(row) === name)!
  const approve = (form.get('intent') === 'approve') !== (world.decision === 'lost')
  const state = approve ? 'approved' : 'declined'
  // Coral records a held decision first, so a read meanwhile already names the next action.
  world.approvals.set(call, state)
  if (world.decision === 'held') await new Promise<void>((resolve) => (world.release = resolve))
  return { state }
}

function branch(rows: Child[], url: URL, run: string) {
  const total = rows.length
  // Without an offset, an anchor starts the window at its record, as Coral does.
  const offsetAsked = url.searchParams.get(`offset.${run}`)
  const anchor = rows.findIndex((row) => row.name === url.searchParams.get(`anchor.${run}`))
  const asked = offsetAsked === null && anchor >= 0 ? anchor : Number(offsetAsked ?? 0)
  // An offset at or past the end reads the last window, as Coral does.
  const offset = asked >= total ? Math.max(0, total - world.window) : asked
  return {
    offset,
    total: String(total),
    rows: rows
      .slice(offset, offset + world.window)
      .map((row, index) => ({ ...row, position: offset + index })),
    next: '',
    unavailable: false,
  }
}

function pageFor(url: URL): ActivityPage {
  const record = url.searchParams.get('record')
  const expanded = url.searchParams.has('tree') ? url.searchParams.getAll('expand') : []
  const run = { ...program, state: world.ended ? 'succeeded' : 'running' } as typeof program
  // A decided tool no longer waits; it runs on.
  const current = world.children.map((row): Child => {
    const approval = world.approvals.get(row.name)
    return approval && row.kind === 'invocation'
      ? {
          ...row,
          requiresAction: approval === 'pending',
          state: approval === 'pending' ? 'pending' : 'dispatching',
        }
      : row
  })
  const children = world.ended ? [...current, finalRow] : current
  const rows: ActivityRow[] = [task, other]
  const branches: ActivityPage['branches'] = {}
  if (expanded.includes(task.name))
    branches[task.name] = {
      rows: [{ ...run, position: 0 }],
      total: '1',
      next: '',
      unavailable: false,
    }
  if (expanded.includes(task.name) && expanded.includes(run.name))
    branches[run.name] = branch(children, url, run.name)
  if (expanded.includes(other.name)) branches[other.name] = branch(world.others, url, other.name)
  const child = [...children, ...world.others].find((row) => row.name === record)
  const selectedRow =
    record === run.name || record === finalRow.name
      ? run
      : record === other.name
        ? other
        : record === task.name
          ? task
          : (child ?? null)
  const runDetail = (row: typeof run) => ({
    kind: 'run' as const,
    run: row,
    request: row === run ? world.request : null,
    source,
    linkedFunctions: [],
    terminal:
      row === run && world.ended
        ? { kind: 'succeeded' as const, finalValue: '"done"' }
        : { kind: 'unavailable' as const },
  })
  // Coral reads a child record with its Run, so the drawer shows that Run's source.
  const contextOf = (row: Child) => runDetail(row.run === run.name ? run : other)
  // A waiting tool selected with its approval opens that approval's review.
  const approval =
    selectedRow?.kind === 'invocation' &&
    url.searchParams.get('request') === approvalOf(selectedRow.name)
      ? world.approvals.get(selectedRow.name)
      : undefined
  const review: RequestDetail | null =
    approval && selectedRow?.kind === 'invocation'
      ? {
          ...approvalRequest,
          name: approvalOf(selectedRow.name),
          run: run.name,
          invocation: selectedRow.name,
          callPath: selectedRow.callPath,
          state: approval,
          actionable: approval === 'pending',
        }
      : null
  return activityPage({
    rows,
    expanded,
    branches,
    selected: selectedRow ? record : null,
    selectedRow,
    liveAction: liveActionFor(url, approval),
    detail:
      selectedRow?.kind === 'run'
        ? runDetail(selectedRow)
        : selectedRow?.kind === 'task'
          ? { kind: 'task', task: selectedRow }
          : selectedRow?.kind === 'invocation'
            ? {
                kind: 'invocation',
                invocation: selectedRow,
                arguments: '{}',
                result: { kind: 'upstream_outcome', value: '{}' },
                requests: review ? [{ kind: 'approval', name: review.name }] : [],
                request: review,
                requestUnavailable: false,
                context: contextOf(selectedRow),
              }
            : selectedRow?.kind === 'output'
              ? {
                  kind: 'output',
                  run: selectedRow.run,
                  event: world.events.get(selectedRow.name)!,
                  context: contextOf(selectedRow),
                }
              : null,
  })
}

function LiveRoute() {
  const page = useLoaderData<ActivityPage>()
  const navigate = useNavigate()
  const { search } = useLocation()
  const { state } = useNavigation()
  const revalidator = useRevalidator()
  const [connected, setConnected] = useState(true)
  const arrive = (child: Child | Event | 'end', run = program.name) => {
    if (child === 'end') world.ended = true
    else world.add(child, run)
    void revalidator.revalidate()
  }
  useEffect(() => {
    world.arrive = arrive
  })
  const selected = new URLSearchParams(search).get('record')
  useEffect(() => {
    world.visited.push(selected)
  }, [selected])
  const next = () => world.children.length + 1
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
      <button type="button" onClick={() => navigate(1)}>
        History forward
      </button>
      <button type="button" onClick={() => arrive(tool(program.name, next(), 'mail.send'))}>
        Add tool
      </button>
      <button type="button" onClick={() => arrive(output(program.name, next(), 'program.logged'))}>
        Add log
      </button>
      <button type="button" onClick={() => arrive(output(program.name, next(), 'program.emitted'))}>
        Add emission
      </button>
      <button type="button" onClick={() => arrive('end')}>
        End run
      </button>
      <button
        type="button"
        onClick={() => {
          // Another reviewer approves the oldest waiting call; the live read shows it.
          const oldest = [...world.approvals].find(([, approval]) => approval === 'pending')
          if (oldest) world.approvals.set(oldest[0], 'approved')
          void revalidator.revalidate()
        }}
      >
        Approve elsewhere
      </button>
      <button
        type="button"
        onClick={() => arrive(tool(other.name, world.others.length + 1, 'other.call'), other.name)}
      >
        Add to other run
      </button>
      <button type="button" onClick={() => setConnected((value) => !value)}>
        {connected ? 'Disconnect' : 'Reconnect'}
      </button>
      <Activity
        page={page}
        windowKey="live"
        live={{ connected, followRoot: true, atStart() {}, viewport() {} }}
      />
    </ActionReviewSession>
  )
}

const expand = `tree=1&expand=${task.name}&expand=${program.name}&expand=${other.name}`
const meta = {
  title: 'Activity/Live',
  parameters: {
    layout: 'fullscreen',
    initialEntry: `/workspaces/finance/activity?${expand}&record=${program.name}`,
  },
  decorators: [
    (_, context) => {
      const Stub = createRoutesStub([
        { path: '/activity/resolve', action: decide },
        {
          path: '*',
          Component: LiveRoute,
          HydrateFallback: () => null,
          loader: ({ request }) => {
            world.loads++
            return pageFor(new URL(request.url))
          },
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
  // An approval review names the drawer for its decision.
  const rail = () => screen.getByRole('dialog', { name: /^(Activity details|Review action)$/ })
  const toggle = () => within(rail()).getByRole('button', { name: 'Follow latest record' })
  const address = () => new URLSearchParams(canvas.getByLabelText('Address').textContent ?? '')
  const idle = async () => {
    await waitFor(() => expect(canvas.getByLabelText('Navigation')).toHaveTextContent('idle'))
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
  }
  return {
    canvas,
    table,
    rail,
    toggle,
    address,
    idle,
    link: (label: string) => within(table).getByRole('link', { name: label }),
    press: (name: string) => userEvent.click(canvas.getByRole('button', { name })),
    /** Waits until the rail heads with `heading` and the table selects `row`, and only it. */
    selects: (heading: string, row: string) =>
      waitFor(() => {
        expect(within(rail()).getByRole('heading', { name: heading, level: 2 })).toBeVisible()
        const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
        expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([row])
      }),
    /** Live mode is on in the address and on the toggle, or off in both. */
    following: (on: boolean) =>
      waitFor(() => {
        expect(address().get('live')).toBe(on ? '1' : null)
        expect(toggle()).toHaveAttribute('aria-pressed', String(on))
      }),
  }
}

/** No record arrives and no load starts while `frames` frames pass. */
async function settled(frames = 10) {
  const loads = world.loads
  for (let frame = 0; frame < frames; frame++)
    await new Promise((resolve) => requestAnimationFrame(resolve))
  await expect(world.loads).toBe(loads)
}

export const FollowsNewRecords: Story = {
  beforeEach: () => world.reset([tool(program.name, 1, 'mail.list')]),
  play: async (context) => {
    const { userEvent } = context
    const { toggle, address, press, selects, following, idle, canvas } = await surface(context)
    await selects('Send reminders', 'Reminder run')
    await following(false)
    const filtered = address().toString()

    // On a Run, live mode selects its newest record, and focus stays on the toggle.
    await userEvent.click(toggle())
    await following(true)
    await selects('mail.list', 'mail.list')
    await waitFor(() => expect(toggle()).toHaveFocus())

    // Each new log, emission or tool of the Run replaces the selection.
    await press('Add log')
    await selects('console.info #2', 'log #2')
    // A record that arrives while you work elsewhere leaves focus where it is.
    await expect(canvas.getByRole('button', { name: 'Add log' })).toHaveFocus()
    await press('Add emission')
    await selects('coral.emit #3', 'emission #3')
    await press('Add tool')
    await selects('mail.send', 'mail.send')
    await following(true)

    // Another Run's records never take the selection.
    await press('Add to other run')
    await idle()
    await selects('mail.send', 'mail.send')

    // The final value is the Run's last record.
    await press('End run')
    await selects('Send reminders', 'final value')
    await expect(address().get('record')).toBe(`${program.name}/final`)
    await following(true)
    // An ended Run changes nothing more: no step repeats and no load starts.
    await idle()
    await settled()

    // Following added no history entries: Back returns to before you turned it on.
    await press('History back')
    await selects('Send reminders', 'Reminder run')
    await following(false)
    await expect(address().toString()).toBe(filtered)
    await press('History forward')
    await selects('Send reminders', 'final value')
    await following(true)
  },
}

export const YourSelectionStopsIt: Story = {
  parameters: {
    // A reload of a live address follows from the record it names.
    initialEntry: `/workspaces/finance/activity?${expand}&record=${tool(program.name, 1, 'mail.list').name}&live=1&filter=kept`,
  },
  beforeEach: () =>
    world.reset([tool(program.name, 1, 'mail.list'), output(program.name, 2, 'program.logged')]),
  play: async (context) => {
    const { userEvent } = context
    const { toggle, address, press, selects, following, link, rail, idle } = await surface(context)
    await selects('console.info #2', 'log #2')
    await following(true)
    await expect(address().get('filter')).toBe('kept')

    // A record you pick turns live mode off, and new records leave it selected.
    await userEvent.click(link('mail.list'))
    await following(false)
    await selects('mail.list', 'mail.list')
    await press('Add emission')
    await idle()
    await selects('mail.list', 'mail.list')

    // On a child record, live mode selects its Run's newest record.
    await userEvent.click(toggle())
    await following(true)
    await selects('coral.emit #3', 'emission #3')

    // A key step turns it off too.
    rail().focus()
    await userEvent.keyboard('{ArrowUp}')
    await following(false)
    await selects('console.info #2', 'log #2')
    await press('Add log')
    await idle()
    await selects('console.info #2', 'log #2')

    // Collapsing the Run you follow turns it off.
    await userEvent.click(toggle())
    await selects('console.info #4', 'log #4')
    await userEvent.click(
      within(link('Reminder run').closest<HTMLElement>('[role="row"]')!).getByRole('button', {
        name: 'Collapse record',
      }),
    )
    await waitFor(() => expect(address().get('live')).toBeNull())
    await selects('Send reminders', 'Reminder run')

    // Closing the rail turns it off, and the next record opens without it.
    await userEvent.click(toggle())
    await following(true)
    await selects('console.info #4', 'log #4')
    await userEvent.click(within(rail()).getByRole('button', { name: 'Close details' }))
    await waitFor(() => expect(address().has('record')).toBe(false))
    await expect(address().has('live')).toBe(false)
    await expect(address().get('filter')).toBe('kept')
  },
}

export const WaitsForRecords: Story = {
  beforeEach: () => world.reset([]),
  play: async (context) => {
    const { userEvent } = context
    const { toggle, press, selects, following, idle, rail } = await surface(context)
    // With no children yet, the Run stays selected and live mode waits.
    await userEvent.click(toggle())
    await following(true)
    await idle()
    await settled()
    await selects('Send reminders', 'Reminder run')
    await press('Add tool')
    await selects('mail.send', 'mail.send')

    // Focus on the old record's content returns to the rail, not to the page.
    const focusable = rail().querySelector<HTMLElement>('summary')!
    focusable.focus()
    await expect(rail().contains(document.activeElement)).toBe(true)
    world.arrive(output(program.name, 2, 'program.emitted'))
    await selects('coral.emit #2', 'emission #2')
    await waitFor(() => expect(rail()).toHaveFocus())

    // While the connection is lost, the record holds; it follows again once it is back.
    await press('Disconnect')
    await press('Add log')
    await idle()
    await settled()
    await selects('coral.emit #2', 'emission #2')
    await press('Reconnect')
    await selects('console.info #3', 'log #3')
  },
}

export const ReadsTheEndOfALongRun: Story = {
  beforeEach: () =>
    world.reset(
      [1, 2, 3, 4, 5].map((sequence) => output(program.name, sequence, 'program.logged')),
      { window: 2, ended: true },
    ),
  play: async (context) => {
    const { userEvent } = context
    const { toggle, address, selects, following, idle } = await surface(context)
    // The table holds the first two of six children; live mode reads the end. The
    // Run has ended, so its final value is its newest record.
    await userEvent.click(toggle())
    await following(true)
    await selects('Send reminders', 'final value')
    await expect(address().get(`offset.${program.name}`)).not.toBe('0')
    await idle()
    await settled()
  },
}

export const UnsentCommentHolds: Story = {
  beforeEach: () => world.reset([], { request: approvalRequest }),
  play: async (context) => {
    const { userEvent } = context
    const { toggle, rail, address, press, selects, following, idle } = await surface(context)
    await userEvent.click(toggle())
    await following(true)
    const summary = within(rail()).getByText('Add a comment')
    if (!summary.closest('details')!.open) await userEvent.click(summary)
    const comment = within(rail()).getByRole('textbox', { name: 'Comment (optional)' })
    await userEvent.type(comment, 'Check the amount')

    // A new record waits while your comment is unsent. No discard prompt opens.
    const held = address().toString()
    await press('Add tool')
    await idle()
    await settled()
    await expect(address().toString()).toBe(held)
    await expect(within(document.body).queryByRole('alertdialog')).toBeNull()
    await expect(comment).toHaveValue('Check the amount')

    // Once the comment is gone, live mode follows again.
    await userEvent.clear(comment)
    await selects('mail.send', 'mail.send')
  },
}

export const FollowsInTheSourceSplit: Story = {
  beforeEach: () => world.reset([tool(program.name, 1, 'mail.list')]),
  play: async (context) => {
    const { userEvent } = context
    const { rail, toggle, address, press, selects, following } = await surface(context)
    const drawer = () => within(rail())
    const highlighted = () =>
      [...rail().querySelectorAll('[data-line][data-highlighted]')].map((line) =>
        Number(line.getAttribute('data-line')),
      )
    /** The selected record's own header holds the toggle beside the close control. */
    const ownHeader = async (title: string) => {
      const selected = drawer().getByRole('region', { name: 'Selected record' })
      const headings = within(selected).getAllByRole('heading', { level: 2 })
      await expect(headings.map((heading) => heading.textContent)).toEqual([title])
      const header = headings[0]!.closest('header')!
      await expect(header).toContainElement(toggle())
      await expect(header).toContainElement(drawer().getByRole('button', { name: 'Close details' }))
    }
    /** Live mode selects `record`, and the source shows the line Coral stored for it. */
    const shows = async (heading: string, row: string, record: string, line: number) => {
      await selects(heading, row)
      await waitFor(() => expect(highlighted()).toEqual([line]))
      await expect(address().get('record')).toBe(record)
      await following(true)
      await ownHeader(heading)
    }
    await selects('Send reminders', 'Reminder run')
    await ownHeader('Send reminders')
    await following(false)

    // Your split size holds while live mode replaces the record, and the source follows it.
    const separator = () =>
      drawer().getByRole('separator', { name: 'Resize detail and source panes' })
    separator().focus()
    await userEvent.keyboard('{ArrowUp}')
    const split = separator().getAttribute('aria-valuenow')
    await expect(split).not.toBe('50')

    await userEvent.click(toggle())
    await shows('mail.list', 'mail.list', tool(program.name, 1, 'mail.list').name, 2)
    await press('Add log')
    await shows('console.info #2', 'log #2', `${program.name}/events/2`, 3)
    await press('Add emission')
    await shows('coral.emit #3', 'emission #3', `${program.name}/events/3`, 4)
    await press('Add tool')
    await shows('mail.send', 'mail.send', tool(program.name, 4, 'mail.send').name, 2)
    await expect(separator()).toHaveAttribute('aria-valuenow', split)

    // The toggle in the record's header turns live mode off.
    await userEvent.click(toggle())
    await following(false)
    await press('Add log')
    await selects('mail.send', 'mail.send')
    await expect(highlighted()).toEqual([2])
  },
}

/**
 * Each new tool, log or emission that live mode selects slides in by Run order,
 * and the final value replaces it at once. Detail motion owns the rule.
 */
export const SlidesToEachNewRecord: Story = {
  beforeEach: () => world.reset([tool(program.name, 1, 'mail.list')]),
  play: async (context) => {
    const { toggle, press, selects } = await surface(context)
    await selects('Send reminders', 'Reminder run')
    await context.userEvent.click(toggle())
    await selects('mail.list', 'mail.list')
    const recorder = recordAnimations()
    try {
      /** The first transform of each animation since the last call, once they end. */
      const starts = async () => {
        const moves = recorder.started.map(({ transforms }) => transforms[0])
        await Promise.all(recorder.started.map(({ animation }) => animation.finished))
        recorder.clear()
        return moves
      }
      const fromBelow = ['translateY(100%)', 'translateY(0)']
      await press('Add tool')
      await selects('mail.send', 'mail.send')
      await expect(await starts()).toEqual(fromBelow)
      await press('Add log')
      await selects('console.info #3', 'log #3')
      await expect(await starts()).toEqual(fromBelow)
      await press('Add emission')
      await selects('coral.emit #4', 'emission #4')
      await expect(await starts()).toEqual(fromBelow)
      await press('End run')
      await selects('Send reminders', 'final value')
      await expect(await starts()).toEqual([])
    } finally {
      recorder.stop()
    }
  },
}

// The oldest approval in the followed program, then the program's newer records.
const first = waiting(1, 'mail.first')
const second = waiting(4, 'mail.second')
const withActions = () =>
  world.reset([
    first,
    output(program.name, 2, 'program.logged'),
    output(program.name, 3, 'program.logged'),
    second,
    output(program.name, 5, 'program.logged'),
  ])

/** The rail reviews the approval of `call`, and the table selects that tool only. */
async function reviews(context: Play, call: Child) {
  const { rail, table, address } = await surface(context)
  const label = call.kind === 'invocation' ? call.callPath : call.label
  await waitFor(() => {
    expect(within(rail()).getByRole('region', { name: `Review ${label}` })).toBeVisible()
    const selected = [...table.querySelectorAll('[role="row"][aria-selected="true"]')]
    expect(selected.map((item) => item.querySelector('a[href]')?.textContent)).toEqual([label])
  })
  await expect(address().get('record')).toBe(call.name)
  await expect(address().get('request')).toBe(approvalOf(call.name))
}

export const ActionsComeFirst: Story = {
  parameters: {
    // The table reads the end of the program, so its oldest approval is out of the window.
    initialEntry: `/workspaces/finance/activity?${expand}&record=${program.name}&offset.${program.name}=5`,
  },
  beforeEach: () => {
    withActions()
    world.window = 2
  },
  play: async (context) => {
    const { userEvent } = context
    const { rail, toggle, address, press, selects, following, idle } = await surface(context)
    await selects('Send reminders', 'Reminder run')
    const drawer = () => within(rail())
    const separator = () =>
      drawer().getByRole('separator', { name: 'Resize detail and source panes' })
    separator().focus()
    await userEvent.keyboard('{ArrowUp}')
    const split = separator().getAttribute('aria-valuenow')

    // Live mode reviews the oldest approval, not the newest log, and reads its window.
    await userEvent.click(toggle())
    await reviews(context, first)
    await following(true)
    await expect(address().has(`offset.${program.name}`)).toBe(false)

    // A newer record leaves the approval selected.
    const held = address().toString()
    await press('Add log')
    await idle()
    await settled()
    await expect(address().toString()).toBe(held)

    // Approving it reviews the next approval at once, with no stop at a newer record.
    world.visited = []
    await userEvent.click(drawer().getByRole('button', { name: 'Approve this call' }))
    await reviews(context, second)
    await following(true)
    await expect(world.visited).toEqual([second.name])

    // Denying the last one follows the newest record again.
    await userEvent.click(drawer().getByRole('button', { name: 'Deny this call' }))
    await selects('console.info #6', 'log #6')
    await following(true)
    await expect(address().has('request')).toBe(false)

    // The split keeps your size through each step.
    await expect(separator()).toHaveAttribute('aria-valuenow', split)
  },
}

export const ADecisionAdvancesWithLiveModeOff: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?${expand}&record=${first.name}&request=${approvalOf(first.name)}`,
  },
  beforeEach: () => {
    withActions()
    // The table holds two records at a time, so each advance reads another window.
    world.window = 2
  },
  play: async (context) => {
    const { userEvent } = context
    const { rail, toggle, address, press, selects, idle } = await surface(context)
    await reviews(context, first)
    await expect(toggle()).toHaveAttribute('aria-pressed', 'false')

    // Denying reviews the next approval and leaves live mode off. The review rests there.
    await userEvent.click(within(rail()).getByRole('button', { name: 'Deny this call' }))
    await reviews(context, second)
    await idle()
    await settled()
    await reviews(context, second)
    await expect(address().has('live')).toBe(false)
    await expect(toggle()).toHaveAttribute('aria-pressed', 'false')

    // A record arrives past the window, and live mode stays off.
    await press('Add log')
    await idle()
    await settled()
    await reviews(context, second)

    // Approving the last one reads the end of the program and selects its newest
    // record, still with live mode off.
    await userEvent.click(within(rail()).getByRole('button', { name: 'Approve this call' }))
    await selects('console.info #6', 'log #6')
    await expect(address().has('live')).toBe(false)
    await expect(toggle()).toHaveAttribute('aria-pressed', 'false')

    // It advanced once: a new record leaves the selection where it is.
    await press('Add log')
    await idle()
    await settled()
    await selects('console.info #6', 'log #6')
  },
}

export const AFailedOrUnfinishedDecisionHolds: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?${expand}&record=${first.name}&request=${approvalOf(first.name)}`,
  },
  beforeEach: withActions,
  play: async (context) => {
    const { userEvent } = context
    const { rail, toggle, address, press, idle, following, selects } = await surface(context)
    await reviews(context, first)
    let held = address().toString()
    const approve = () => within(rail()).getByRole('button', { name: 'Approve this call' })
    /** The approval stays selected while a record arrives and the page reads again. */
    const holds = async () => {
      await press('Add log')
      await waitFor(() => expect(world.loads).toBeGreaterThan(loads))
      await settled()
      await expect(address().toString()).toBe(held)
    }
    let loads = world.loads

    // A decision that fails shows its error and keeps the approval selected.
    world.decision = 'error'
    await userEvent.click(approve())
    await expect(
      await within(rail()).findByText('The decision did not save. Try again.'),
    ).toBeVisible()
    await idle()
    loads = world.loads
    await holds()

    // While a decision is under way, the approval stays selected, even once a read
    // already names the next one.
    world.decision = 'held'
    await userEvent.click(approve())
    await waitFor(() => expect(approve()).toHaveAttribute('aria-busy', 'true'))
    await expect(approve()).toHaveAttribute('aria-disabled', 'true')
    loads = world.loads
    await holds()

    // Once it saves, the review advances, with live mode still off.
    world.release()
    await reviews(context, second)
    await expect(address().has('live')).toBe(false)

    // With live mode on, a decision under way holds the review the same way.
    await userEvent.click(toggle())
    await following(true)
    await idle()
    held = address().toString()
    await userEvent.click(approve())
    await waitFor(() => expect(approve()).toHaveAttribute('aria-busy', 'true'))
    await expect(approve()).toHaveAttribute('aria-disabled', 'true')
    loads = world.loads
    await holds()
    world.release()
    await selects('console.info #8', 'log #8')
    await following(true)
  },
}

export const AnUnsentCommentHoldsTheAction: Story = {
  parameters: {
    // You review the newer approval, so the older one waits outside it.
    initialEntry: `/workspaces/finance/activity?${expand}&record=${second.name}&request=${approvalOf(second.name)}`,
  },
  beforeEach: withActions,
  play: async (context) => {
    const { userEvent } = context
    const { rail, toggle, address, idle, following } = await surface(context)
    await reviews(context, second)
    const summary = within(rail()).getByText('Add a comment')
    if (!summary.closest('details')!.open) await userEvent.click(summary)
    const comment = within(rail()).getByRole('textbox', { name: 'Comment (optional)' })
    await userEvent.type(comment, 'Check the amount')

    // Live mode turns on, but your comment holds the review. No discard prompt opens.
    await userEvent.click(toggle())
    await following(true)
    await idle()
    await settled()
    await expect(address().get('record')).toBe(second.name)
    await expect(within(document.body).queryByRole('alertdialog')).toBeNull()
    await expect(comment).toHaveValue('Check the amount')

    // Once the comment is gone, live mode reviews the older approval.
    await userEvent.clear(comment)
    await reviews(context, first)
  },
}

export const AnOppositeDecisionThatWonHolds: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?${expand}&record=${first.name}&request=${approvalOf(first.name)}`,
  },
  beforeEach: withActions,
  play: async (context) => {
    const { userEvent } = context
    const { rail, address, idle } = await surface(context)
    await reviews(context, first)
    const held = address().toString()

    // Another reviewer denied the call first. Your approval lost, so the review stays
    // and shows the decision that won.
    world.decision = 'lost'
    await userEvent.click(within(rail()).getByRole('button', { name: 'Approve this call' }))
    await expect(await within(rail()).findByText('Denied')).toBeVisible()
    await idle()
    await settled()
    await expect(address().toString()).toBe(held)
  },
}

export const LiveModeFollowsActionsResolvedElsewhere: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?${expand}&record=${first.name}&request=${approvalOf(first.name)}&live=1`,
  },
  beforeEach: withActions,
  play: async (context) => {
    const { address, press, selects, following } = await surface(context)
    await reviews(context, first)
    await following(true)

    // Another reviewer approves the call you review; live mode reviews the next one.
    world.visited = []
    await press('Approve elsewhere')
    await reviews(context, second)
    await following(true)
    await expect(world.visited).toEqual([second.name])

    // Once no action is left, it follows the newest record.
    await press('Approve elsewhere')
    await selects('console.info #5', 'log #5')
    await following(true)
    await expect(address().has('request')).toBe(false)
  },
}

export const YourPickDuringADecisionStays: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?${expand}&record=${first.name}&request=${approvalOf(first.name)}`,
  },
  beforeEach: withActions,
  play: async (context) => {
    const { userEvent } = context
    const { rail, address, link, selects, idle } = await surface(context)
    await reviews(context, first)

    // You pick a record while your decision is under way. Once it saves, the review
    // does not take the selection back.
    world.decision = 'held'
    const approve = () => within(rail()).getByRole('button', { name: 'Approve this call' })
    await userEvent.click(approve())
    await waitFor(() => expect(approve()).toHaveAttribute('aria-busy', 'true'))
    await expect(approve()).toHaveAttribute('aria-disabled', 'true')
    await userEvent.click(link('log #2'))
    await selects('console.info #2', 'log #2')
    world.release()
    await idle()
    await settled()
    await selects('console.info #2', 'log #2')
    await expect(address().has('request')).toBe(false)
    await expect(address().has('live')).toBe(false)
  },
}
