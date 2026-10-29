import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useSearchParams } from 'react-router'
import { expect, spyOn, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { activityPage } from './fixtures'
import type { ActivityPage, ActivityRow, RecordSourceLocation, RunDetail, RunEvent } from './types'
import { resizeViewport } from '../../../.storybook/browser'

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

/** The source's own scrolling viewport in `scope`, not a wrapper that never scrolls. */
function viewportOf(scope: HTMLElement) {
  let viewport = scope.querySelector('[data-line]')!.closest('pre')!.parentElement!
  while (!/auto|scroll/.test(getComputedStyle(viewport).overflowY))
    viewport = viewport.parentElement!
  return viewport
}

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
 * with no call of its own says nothing of a location. On a wide screen the rail docks
 * beside the list, and the split fills its height. */
export const SameRunNavigation: Story = {
  parameters: { record: `${runName}/events/2` },
  play: async ({ canvasElement, userEvent }) => {
    // Storybook's own UI cannot resize its viewport; there the rail keeps the default width.
    const wide = await resizeViewport(1440, 900)
    const dialog = await within(canvasElement.ownerDocument.body).findByRole('dialog')
    const drawer = within(dialog)
    const sourcePane = drawer.getByRole('region', { name: 'Accepted source' })
    const detail = drawer.getByRole('region', { name: 'Selected record' })
    const height = (element: Element) => element.getBoundingClientRect().height
    if (wide) {
      const table = within(canvasElement).getByRole('table', { name: 'Activity records' })
      await waitFor(() => {
        // Docked, the rail starts after the table ends.
        expect(table.getBoundingClientRect().right).toBeLessThanOrEqual(
          dialog.getBoundingClientRect().left,
        )
        // The rail runs as low as the table, and its two panes share that height rather
        // than shrinking to the split's 360px minimum.
        expect(dialog.getBoundingClientRect().bottom).toBeGreaterThanOrEqual(
          table.getBoundingClientRect().bottom - 1,
        )
        expect(height(detail) + height(sourcePane)).toBeGreaterThan(height(dialog) * 0.75)
      })
    }
    await expectOwnHeader(dialog, 'Tool call ended: completed')
    await expect(drawer.queryByText('Source location unavailable')).toBeNull()
    const code = dialog.querySelector('[data-line]')!.closest('pre')!
    const viewport = viewportOf(dialog)
    await expect(dialog).toContainElement(viewport)
    await expect(viewport.scrollHeight).toBeGreaterThan(viewport.clientHeight)

    const separator = drawer.getByRole('separator', { name: 'Resize detail and source panes' })
    await expect(separator).toHaveAttribute('aria-valuenow', '50')
    // The divider spans the drawer from border to border, through its padding, while the
    // panes keep theirs.
    const inner = dialog.getBoundingClientRect()
    await expect(separator.getBoundingClientRect().left).toBeCloseTo(
      inner.left + dialog.clientLeft,
      0,
    )
    await expect(separator.getBoundingClientRect().right).toBeCloseTo(
      inner.right - dialog.clientLeft,
      0,
    )
    await expect(detail.getBoundingClientRect().left).toBeGreaterThan(
      separator.getBoundingClientRect().left,
    )
    separator.focus()
    const before = height(detail)
    await userEvent.keyboard('{ArrowUp}')
    const split = separator.getAttribute('aria-valuenow')!
    await expect(split).not.toBe('50')
    // The detail pane gives its height to the source pane.
    await waitFor(() => expect(height(detail)).toBeLessThan(before))

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

/** The drawer and its source pane, with the record `parameters.record` selected. */
async function openDrawerSource(canvasElement: HTMLElement) {
  const page = within(canvasElement.ownerDocument.body)
  const dialog = await page.findByRole('dialog', { name: 'Activity details' })
  const pane = within(dialog).getByRole('region', { name: 'Accepted source' })
  return { page, dialog, pane }
}

/**
 * Catches: an expanded source that loses the record's line, its language or where it is;
 * a dialog without Wax's header; or a close that leaves focus, the inline source's DOM or
 * scroll, the split or the detail's scroll anywhere but where you left them.
 */
export const ExpandsTheSource: Story = {
  parameters: { record: laterSend.name },
  play: async ({ canvasElement, userEvent }) => {
    const { page, dialog, pane } = await openDrawerSource(canvasElement)
    await waitFor(() => expect(highlighted(pane)).toEqual([66]))
    const code = pane.querySelector('pre')!
    const viewport = viewportOf(pane)
    const separator = within(dialog).getByRole('separator', {
      name: 'Resize detail and source panes',
    })
    separator.focus()
    await userEvent.keyboard('{ArrowUp}')
    const split = separator.getAttribute('aria-valuenow')
    viewport.scrollTop = 200
    await waitFor(() => expect(viewport.scrollTop).toBe(200))
    const record = within(dialog).getByRole('region', { name: 'Selected record' })
    const detail = [...record.querySelectorAll<HTMLElement>('*')].find((element) =>
      /auto|scroll/.test(getComputedStyle(element).overflowY),
    )!
    // The record's detail is scrolled too, so its return to that place shows.
    await expect(detail.scrollHeight - detail.clientHeight).toBeGreaterThanOrEqual(40)
    detail.scrollTop = 40
    await waitFor(() => expect(detail.scrollTop).toBe(40))

    const expand = within(pane).getByRole('button', { name: 'Expand Accepted source' })
    await userEvent.click(expand)
    const expanded = await page.findByRole('dialog', { name: 'Accepted source' })
    // Wax's header: the title at the left, its close control at the top right.
    const title = within(expanded).getByRole('heading', { name: 'Accepted source' })
    const close = within(expanded).getByRole('button', { name: 'Close' })
    await expect(title.closest('header')).toContainElement(close)
    await expect(title.getBoundingClientRect().left).toBeLessThan(
      close.getBoundingClientRect().left,
    )
    await waitFor(() => expect(within(expanded).getByText('TypeScript')).toBeVisible())
    await expect(
      within(expanded)
        .getAllByRole('status')
        .map((status) => status.textContent),
    ).toContain('Line 66')
    await expect(highlighted(expanded)).toEqual([66])
    // It opens on the record's line.
    const inner = viewportOf(expanded)
    await waitFor(() => {
      const line = expanded.querySelector('[data-line="66"]')!.getBoundingClientRect()
      const bounds = inner.getBoundingClientRect()
      expect(line.top).toBeGreaterThanOrEqual(bounds.top)
      expect(line.bottom).toBeLessThanOrEqual(bounds.bottom)
    })

    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(page.queryByRole('dialog', { name: 'Accepted source' })).toBeNull())
    await waitFor(() => expect(expand).toHaveFocus())
    await expect(pane.querySelector('pre')).toBe(code)
    await expect(viewport.scrollTop).toBe(200)
    await expect(separator).toHaveAttribute('aria-valuenow', split!)
    await expect(detail.scrollTop).toBe(40)
    await expect(highlighted(pane)).toEqual([66])
  },
}

/**
 * Catches: a copy or download of anything but the exact source the pane shows, such as
 * its rendered rows, inline or expanded, or a download with no sensible file name.
 */
function copiesAndDownloads(record: string, title: string, code: string, fileName: string): Story {
  return {
    parameters: { record },
    play: async ({ canvasElement, userEvent }) => {
      const { page, pane } = await openDrawerSource(canvasElement)
      const copied = spyOn(navigator.clipboard, 'writeText').mockResolvedValue()
      const blobs: Blob[] = []
      const created = spyOn(URL, 'createObjectURL').mockImplementation((blob) => {
        blobs.push(blob as Blob)
        return 'blob:source'
      })
      const names: string[] = []
      const clicked = spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
        this: HTMLAnchorElement,
      ) {
        names.push(this.download)
      })
      try {
        const inline = within(pane)
        await userEvent.click(inline.getByRole('button', { name: `Copy ${title}` }))
        await userEvent.click(inline.getByRole('button', { name: `Download ${title}` }))
        await userEvent.click(inline.getByRole('button', { name: `Expand ${title}` }))
        const expanded = within(await page.findByRole('dialog', { name: title }))
        await userEvent.click(expanded.getByRole('button', { name: `Copy ${title}` }))
        await userEvent.click(expanded.getByRole('button', { name: `Download ${title}` }))
        await waitFor(() => expect(copied.mock.calls).toEqual([[code], [code]]))
        await expect(await Promise.all(blobs.map((blob) => blob.text()))).toEqual([code, code])
        await expect(names).toEqual([fileName, fileName])
      } finally {
        copied.mockRestore()
        created.mockRestore()
        clicked.mockRestore()
      }
    },
  }
}
export const CopiesAndDownloadsTheProgram = copiesAndDownloads(
  laterSend.name,
  'Accepted source',
  source,
  'program-reminder-batch.ts',
)
export const CopiesAndDownloadsASnapshot = copiesAndDownloads(
  archived.name,
  'Saved-function snapshot: billing.notify',
  snapshot,
  'billing.notify.ts',
)

/** The lines that hold the current match's parts in `scope`. */
const currentLines = (scope: HTMLElement) => [
  ...new Set(
    within(scope)
      .queryAllByRole('mark', { current: true })
      .map((part) => Number(part.closest('[data-line]')!.getAttribute('data-line'))),
  ),
]

/**
 * Catches: a Find that misses a late line of the exact source, does not count or step
 * through its matches, leaves an offscreen match out of view, or drops the syntax
 * colouring or the record's highlighted line; or a close that leaves the inline source
 * anywhere but where you left it.
 */
export const FindsALateMatchInTheSource: Story = {
  parameters: { record: laterSend.name },
  play: async ({ canvasElement, userEvent }) => {
    const { page, dialog, pane } = await openDrawerSource(canvasElement)
    await waitFor(() => expect(highlighted(pane)).toEqual([66]))
    const viewport = viewportOf(pane)
    viewport.scrollTop = 120
    await waitFor(() => expect(viewport.scrollTop).toBe(120))
    const separator = within(dialog).getByRole('separator', {
      name: 'Resize detail and source panes',
    })
    const split = separator.getAttribute('aria-valuenow')
    // Inline, the pane keeps only its compact actions.
    await expect(within(pane).queryByRole('searchbox')).toBeNull()

    const expand = within(pane).getByRole('button', { name: 'Expand Accepted source' })
    await userEvent.click(expand)
    const expanded = await page.findByRole('dialog', { name: 'Accepted source' })
    const find = within(expanded).getByRole('searchbox', { name: 'Find in Accepted source' })
    await userEvent.type(find, 'batch note 59')
    const status = within(expanded).getByText(/of 2$/)
    await expect(status).toHaveTextContent('1 of 2')
    await waitFor(() => expect(currentLines(expanded)).toEqual([65]))

    // The late match, far below the view, comes into view, its colouring kept.
    await userEvent.click(within(expanded).getByRole('button', { name: 'Next match' }))
    await expect(status).toHaveTextContent('2 of 2')
    const inner = viewportOf(expanded)
    await waitFor(() => {
      expect(currentLines(expanded)).toEqual([128])
      const mark = within(expanded).getAllByRole('mark', { current: true })[0]!
      const [box, bounds] = [mark, inner].map((each) => each.getBoundingClientRect())
      expect(box.top).toBeGreaterThanOrEqual(bounds.top)
      expect(box.bottom).toBeLessThanOrEqual(bounds.bottom)
    })
    const late = within(expanded).getAllByRole('mark', { current: true })[0]!
    const line = late.closest('[data-line]')!
    await expect(line).toHaveTextContent('// Second batch note 59')
    // The mark sits in the comment's own colour, not plain text's.
    const plain = getComputedStyle(line.lastElementChild!).color
    await expect(getComputedStyle(late.parentElement!).color).not.toBe(plain)
    await expect(getComputedStyle(late).color).toBe(getComputedStyle(late.parentElement!).color)
    // The record's own line stays highlighted, and named.
    await expect(highlighted(expanded)).toEqual([66])
    await expect(within(expanded).getByText('Line 66')).toBeVisible()
    // Enter steps on from the field too, and wraps.
    await userEvent.click(find)
    await userEvent.keyboard('{Enter}')
    await expect(status).toHaveTextContent('1 of 2')
    await userEvent.click(within(expanded).getByRole('button', { name: 'Previous match' }))
    await expect(status).toHaveTextContent('2 of 2')

    await userEvent.keyboard('{Escape}')
    await waitFor(() => expect(page.queryByRole('dialog', { name: 'Accepted source' })).toBeNull())
    await waitFor(() => expect(expand).toHaveFocus())
    await expect(viewport.scrollTop).toBe(120)
    await expect(separator).toHaveAttribute('aria-valuenow', split!)
    await expect(within(pane).queryAllByRole('mark')).toHaveLength(0)
  },
}
