import type { Meta, StoryObj } from '@storybook/react'
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
import {
  activityPage,
  approvalRequest,
  invocationRow,
  runRow,
  taskRow,
  workspace,
} from './fixtures'
import type { ActivityPage, ActivityRow, RequestDetail, RunEvent } from './types'

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

function operation(run: string, sequence: number, callPath: string): Child {
  return invocationRow(run, `${run.split('/').at(-1)}-${sequence}`, {
    task: run === program.name ? task.name : '',
    operation: callPath,
    callPath,
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
    },
    event,
  }
}
const finalRow: Child = {
  ...output(program.name, 99, 'program.emitted').row,
  name: `${program.name}/final`,
  label: 'final value',
  eventKind: 'run.ended',
}

/** What Coral holds. A play changes it, then revalidates, as a live snapshot would. */
const world = {
  children: [] as Child[],
  others: [] as Child[],
  events: new Map<string, RunEvent>(),
  ended: false,
  /** How many children one branch read returns. */
  window: 100,
  request: null as RequestDetail | null,
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
    this.ended = options.ended ?? false
    this.window = options.window ?? 100
    this.request = options.request ?? null
    for (const child of children) this.add(child)
  },
  add(child: Child | Event, run = program.name) {
    const row = 'event' in child ? child.row : child
    if ('event' in child) this.events.set(row.name, child.event)
    ;(run === program.name ? this.children : this.others).push(row)
  },
}

function branch(rows: Child[], url: URL, run: string) {
  const total = rows.length
  const asked = Number(url.searchParams.get(`offset.${run}`) ?? 0)
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
  const children = world.ended ? [...world.children, finalRow] : world.children
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
    source: 'await coral.providers.mail.send()',
    linkedFunctions: [],
    terminal:
      row === run && world.ended
        ? { kind: 'succeeded' as const, finalValue: '"done"' }
        : { kind: 'unavailable' as const },
  })
  return activityPage({
    rows,
    expanded,
    branches,
    selected: selectedRow ? record : null,
    selectedRow,
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
                requests: [],
                request: null,
                requestUnavailable: false,
              }
            : selectedRow?.kind === 'output'
              ? { kind: 'output', run: selectedRow.run, event: world.events.get(selectedRow.name)! }
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
  const next = () => world.children.length + 1
  return (
    <ActionReviewSession
      request={page.detail?.kind === 'run' ? (page.detail.request ?? null) : null}
    >
      <code aria-label="Address">{search}</code>
      <output aria-label="Navigation">{state === 'idle' ? revalidator.state : state}</output>
      <button type="button" onClick={() => navigate(-1)}>
        History back
      </button>
      <button type="button" onClick={() => navigate(1)}>
        History forward
      </button>
      <button type="button" onClick={() => arrive(operation(program.name, next(), 'mail.send'))}>
        Add operation
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
        onClick={() =>
          arrive(operation(other.name, world.others.length + 1, 'other.call'), other.name)
        }
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
  const rail = () => screen.getByRole('dialog', { name: 'Activity details' })
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
  beforeEach: () => world.reset([operation(program.name, 1, 'mail.list')]),
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

    // Each new log, emission or operation of the Run replaces the selection.
    await press('Add log')
    await selects('console.info #2', 'log #2')
    // A record that arrives while you work elsewhere leaves focus where it is.
    await expect(canvas.getByRole('button', { name: 'Add log' })).toHaveFocus()
    await press('Add emission')
    await selects('coral.emit #3', 'emission #3')
    await press('Add operation')
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
    initialEntry: `/workspaces/finance/activity?${expand}&record=${operation(program.name, 1, 'mail.list').name}&live=1&filter=kept`,
  },
  beforeEach: () =>
    world.reset([
      operation(program.name, 1, 'mail.list'),
      output(program.name, 2, 'program.logged'),
    ]),
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
    await press('Add operation')
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
    await press('Add operation')
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
