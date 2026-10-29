import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useSearchParams } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { activityPage } from './fixtures'
import type { ActivityPage, ActivityRow, RecordSourceLocation, RunDetail, RunEvent } from './types'

const workspace = 'workspaces/finance'
const runName = `${workspace}/runs/reminder-batch`
const startedAt = '2026-09-18T14:02:01.000Z'
const at = (seconds: number) => new Date(Date.parse(startedAt) + seconds * 1000).toISOString()

const run: Extract<ActivityRow, { kind: 'run' }> = {
  kind: 'run',
  name: runName,
  workspace,
  agent: '',
  actor: 'opaque-principal',
  actorLabel: 'Collections agent',
  workspaceLabel: 'Finance',
  intent: 'Send invoice reminders',
  task: '',
  state: 'failed',
  startedAt,
  endedAt: at(18),
  durationNanos: '18000000000',
  requiresAction: false,
}
// Each record carries the location Coral stored when the author's code made it.
const savedFunction = `${workspace}/savedFunctions/notify`
const source = [
  'export default async function reminders() {',
  "  const invoices = await coral.providers.billing.listInvoices({ status: 'overdue' })",
  '  await coral.providers.mail.send({',
  '    to: invoices[0].ownerEmail,',
  '  })',
  ...Array.from({ length: 60 }, (_, i) => `  // First batch note ${i}`),
  '  await coral.providers.mail.send({',
  '    to: invoices[1].ownerEmail,',
  '  })',
  ...Array.from({ length: 60 }, (_, i) => `  // Second batch note ${i}`),
  "  console.info('sent', 2)",
  '  coral.emit({ sent: 2 })',
  '  await coral.functions.billing.notify()',
  '}',
].join('\n')
const snapshot = 'export default async () => {\n  await coral.providers.mail.archive({})\n}'
/** The stored location of the call to `name` on `line`: its column is where that name starts. */
function stored(line: number, name: string, document: RecordSourceLocation['document']) {
  const text = (document.kind === 'program' ? source : snapshot).split('\n')[line - 1]!
  return { document, line, column: text.indexOf(name) + 1 }
}
const inProgram = (line: number, name: string) => stored(line, name, { kind: 'program' })

function tool(
  slug: string,
  callPath: string,
  position: number,
  sourceLocation?: RecordSourceLocation,
): Extract<ActivityRow, { kind: 'invocation' }> {
  return {
    kind: 'invocation',
    name: `${runName}/invocations/${slug}`,
    workspace,
    actor: run.actor,
    agent: '',
    task: '',
    run: runName,
    provider: `${workspace}/providers/${callPath.split('.')[0]}`,
    tool: callPath.split('.').at(-1)!,
    callPath,
    state: 'ended',
    outcome: 'upstream_outcome',
    startedAt: at(position),
    endedAt: at(position),
    durationNanos: '180000000',
    requiresAction: false,
    dispatchedAt: at(position),
    waitingNanos: '0',
    position,
    sourceLocation,
  }
}
function output(
  sequence: number,
  event: RunEvent,
  position: number,
  sourceLocation?: RecordSourceLocation,
): ActivityRow {
  return {
    kind: 'output',
    name: `${runName}/events/${sequence}`,
    workspace,
    run: runName,
    label: event.kind,
    startedAt: event.occurredAt,
    eventKind: event.kind,
    agent: '',
    position,
    sourceLocation,
  }
}

const listInvoices = tool('list', 'billing.listInvoices', 0, inProgram(2, 'listInvoices'))
// The two sends share a call path. The later record ran the earlier line: each keeps its own.
const laterSend = tool('send-later', 'mail.send', 1, inProgram(66, 'send'))
const earlierSend = tool('send-earlier', 'mail.send', 3, inProgram(3, 'send'))
// A record from before Coral stored locations: its call matches line 3, yet it has no location.
const legacySend = tool('send-legacy', 'mail.send', 4)
const archived = tool(
  'archive',
  'mail.archive',
  9,
  stored(2, 'archive', { kind: 'saved_function', savedFunction }),
)
// The record of the later send ending: it links to its tool inside the same Run.
const ended: RunEvent = {
  kind: 'invocation.ended',
  sequence: '2',
  occurredAt: at(2),
  invocation: laterSend.name,
  outcome: 'upstream_outcome',
}
const logged: RunEvent = {
  kind: 'program.logged',
  sequence: '7',
  occurredAt: at(7),
  level: 'info',
  message: 'sent 2',
}
const emitted: RunEvent = {
  kind: 'program.emitted',
  sequence: '8',
  occurredAt: at(8),
  value: '{"sent":2}',
}
const events: Record<string, RunEvent> = {
  [`${runName}/events/2`]: ended,
  [`${runName}/events/7`]: logged,
  [`${runName}/events/8`]: emitted,
}
const records: ActivityRow[] = [
  listInvoices,
  laterSend,
  output(2, ended, 2),
  earlierSend,
  legacySend,
  output(7, logged, 7, inProgram(129, 'info')),
  output(8, emitted, 8, inProgram(130, 'emit')),
  archived,
]

const context: RunDetail = {
  kind: 'run',
  run,
  request: null,
  requestUnavailable: false,
  source,
  linkedFunctions: [{ savedFunction, path: 'billing.notify', source: snapshot }],
  terminal: {
    kind: 'failed',
    code: 'program_execution_failed',
    message: 'Reminder batch failed.',
    error: {
      name: 'Error',
      message: 'failed',
      frames: [{ source: 'Program', line: 1, column: 1 }],
      toolCode: '',
      toolTraceId: '',
      toolStatus: 0,
      toolBody: '',
      toolBodyTruncated: false,
      cause: null,
      errors: [],
      truncated: false,
      causeOmitted: false,
      omittedErrors: 0,
      circular: false,
    },
  },
}

function detailFor(row: ActivityRow | null): ActivityPage['detail'] {
  if (row?.kind === 'run') return context
  if (row?.kind === 'invocation')
    return {
      kind: 'invocation',
      invocation: row,
      arguments: '{"to":"owner@example.com"}',
      result: { kind: 'upstream_outcome', value: '{"data":{},"metadata":{"status":202}}' },
      requests: [],
      request: null,
      requestUnavailable: false,
      context,
    }
  if (row?.kind === 'output')
    return { kind: 'output', run: runName, event: events[row.name]!, context }
  return null
}

function NavigableLocations() {
  const [query] = useSearchParams()
  const selected = query.get('record')
  const row = [run, ...records].find((candidate) => candidate.name === selected) ?? null
  return (
    <Activity
      page={activityPage({
        rows: [run],
        branches: { [runName]: { rows: records, next: '', unavailable: false } },
        expanded: [runName],
        selected: row?.name ?? null,
        selectedRow: row,
        detail: detailFor(row),
      })}
    />
  )
}

const meta = {
  title: 'Activity/Source locations',
  component: NavigableLocations,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [1440] } },
  decorators: [
    (Story, { parameters }) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: () => (
            <ActionReviewSession>
              <Story />
            </ActionReviewSession>
          ),
        },
      ])
      const record = (parameters as { record?: string }).record ?? runName
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub
              initialEntries={[
                `/workspaces/finance/activity?tree=1&expand=${encodeURIComponent(runName)}&record=${encodeURIComponent(record)}`,
              ]}
            />
          </PageLayout>
        </div>
      )
    },
  ],
} satisfies Meta<typeof NavigableLocations>
export default meta
type Story = StoryObj<typeof meta>

const highlighted = (dialog: HTMLElement) =>
  [...dialog.querySelectorAll('[data-line]')]
    .filter((line) => line.hasAttribute('data-highlighted'))
    .map((line) => Number(line.getAttribute('data-line')))

/** The selected record's own header: the split's only header, with the drawer's close control. */
async function expectOwnHeader(dialog: HTMLElement, title: string) {
  const selected = within(dialog).getByRole('region', { name: 'Selected record' })
  const headings = within(selected).getAllByRole('heading', { level: 2 })
  await expect(headings.map((heading) => heading.textContent)).toEqual([title])
  const close = within(dialog).getByRole('button', { name: 'Close details' })
  await expect(headings[0]!.closest('header')).toContainElement(close)
  await expect(close).toBeVisible()
}

/** One selection: its record's own detail above, its highlighted call in the source below.
 * A Saved Function snapshot is named; the accepted program, the pane's default, is not. */
function selection(
  record: string,
  title: string,
  documentLabel: string | null,
  expected: number[],
): Story {
  return {
    parameters: { record },
    play: async ({ canvasElement }) => {
      const dialog = await within(canvasElement.ownerDocument.body).findByRole('dialog', {
        name: 'Activity details',
      })
      const drawer = within(dialog)
      await expectOwnHeader(dialog, title)
      const pane = within(drawer.getByRole('region', { name: 'Accepted source' }))
      if (documentLabel) await expect(pane.getByText(documentLabel)).toBeVisible()
      await waitFor(() => expect(highlighted(dialog)).toEqual(expected))
      if (expected.length) {
        await expect(drawer.queryByText('Source location unavailable')).not.toBeInTheDocument()
      } else {
        await expect(drawer.getByText('Source location unavailable')).toBeVisible()
      }
      await expect(drawer.queryByText(/estimated|inferred|prototype/i)).not.toBeInTheDocument()
    },
  }
}

// The accepted program is the pane's default document, so it carries no name.
const program = null
/** A failed Run's retained error frame, only for the Run's own selection. */
export const FailedRunFrame = selection(runName, 'Send invoice reminders', program, [1])
export const SingleTool = selection(listInvoices.name, 'billing.listInvoices', program, [2])
/** Two records of one call path, loaded out of source order, each at its own stored line. */
export const EarlierLineLaterRecord = selection(laterSend.name, 'mail.send', program, [66])
export const LaterLineEarlierRecord = selection(earlierSend.name, 'mail.send', program, [3])
/** Its call matches the source exactly, but Coral stored no location for it. */
export const RecordWithoutLocation = selection(legacySend.name, 'mail.send', program, [])
export const SavedFunctionCall = selection(
  archived.name,
  'mail.archive',
  'Saved-function snapshot: billing.notify',
  [2],
)
export const ConsoleLog = selection(`${runName}/events/7`, 'console.info #7', program, [129])
export const Emission = selection(`${runName}/events/8`, 'coral.emit #8', program, [130])

/** Moving to another record of the same Run swaps the selected record's own header and
 * keeps the split and the source pane, which reveals the new record's line. A Run event
 * with no call of its own says nothing of a location. */
export const SameRunNavigation: Story = {
  parameters: { record: `${runName}/events/2` },
  play: async ({ canvasElement, userEvent }) => {
    const dialog = await within(canvasElement.ownerDocument.body).findByRole('dialog')
    const drawer = within(dialog)
    await expectOwnHeader(dialog, 'Tool call ended: completed')
    await expect(drawer.queryByText('Source location unavailable')).toBeNull()
    const code = dialog.querySelector('[data-line]')!.closest('pre')!
    // The source pane's own scrolling viewport, not a wrapper that never scrolls.
    let viewport = code.parentElement!
    while (!/auto|scroll/.test(getComputedStyle(viewport).overflowY))
      viewport = viewport.parentElement!
    await expect(dialog).toContainElement(viewport)
    await expect(viewport.scrollHeight).toBeGreaterThan(viewport.clientHeight)

    const separator = drawer.getByRole('separator', { name: 'Resize detail and source panes' })
    await expect(separator).toHaveAttribute('aria-valuenow', '50')
    separator.focus()
    await userEvent.keyboard('{ArrowUp}')
    const split = separator.getAttribute('aria-valuenow')!
    await expect(split).not.toBe('50')

    viewport.scrollTop = 0
    const below = () =>
      dialog.querySelector('[data-line="66"]')!.getBoundingClientRect().top >=
      viewport.getBoundingClientRect().bottom
    await waitFor(() => expect(below()).toBe(true))

    await userEvent.click(drawer.getByRole('link', { name: 'Open tool call' }))
    await drawer.findByRole('heading', { name: 'mail.send' })
    await expectOwnHeader(dialog, 'mail.send')
    await expect(drawer.queryByRole('heading', { name: 'Send invoice reminders' })).toBeNull()
    await waitFor(() => expect(highlighted(dialog)).toEqual([66]))
    await expect(dialog.querySelector('[data-line]')!.closest('pre')).toBe(code)
    await expect(separator).toHaveAttribute('aria-valuenow', split)
    // Line 66 starts out of view; the pane scrolls to it with no toggle to ask.
    await waitFor(() => {
      const line = dialog.querySelector('[data-line="66"]')!.getBoundingClientRect()
      const bounds = viewport.getBoundingClientRect()
      expect(line.top).toBeGreaterThanOrEqual(bounds.top)
      expect(line.bottom).toBeLessThanOrEqual(bounds.bottom)
    })
  },
}
