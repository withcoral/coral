import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { expect, fn, userEvent, waitFor, within } from 'storybook/test'
import {
  absorbPage,
  nextTraceRequest,
  openTrace,
  receiveTrace,
  traceQuery,
  type ActivityTraceStore,
  type TracePreference,
} from '@/lib/activity-trace'
import type { ActivityTraceReader } from '@/lib/activity-trace-reader'
import { coral } from '@/lib/activity-trace.test-support'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { click, press, resizeViewport } from '../../../.storybook/browser'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { workspace } from './fixtures'
import { freezeStoryClock, storyNow } from './story-clock'
import {
  T0,
  traceCall,
  traceEvent,
  tracePage,
  traceRun,
  traceStore,
  traceTask,
  traceTool,
} from './trace-fixtures'
import type { ActivityPage, ActivityRow } from './types'

const task = traceTask()
const linear = { provider: `${workspace}/providers/linear` }

/** A Task of one search and one Run that made two calls, logged once and emitted once. */
function shortChildren() {
  const run = traceRun('short', 2_000, 9_000, { contextImpactBytes: '-4180' })
  return {
    [task.name]: [traceTool('search', 'search', 200, 900), run],
    [run.name]: [
      traceEvent(run, 1, 2_000, 'run.started'),
      traceCall(run, 'list', [2_300, 2_300, 4_800], {
        ...linear,
        contextImpactBytes: '-3200',
      }),
      traceEvent(run, 2, 5_000, 'program.logged', '40'),
      traceCall(run, 'get', [5_200, 5_200, 7_900], {
        ...linear,
        callPath: 'coral.providers.linear.getIssue',
        contextImpactBytes: '-1500',
      }),
      traceEvent(run, 3, 8_400, 'program.emitted', '180'),
      traceEvent(run, 4, 9_000, 'run.ended', '300'),
    ],
  }
}

const meta = {
  title: 'Activity/Waterfall',
  component: Activity,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1440] } },
  decorators: [
    (Story, context) => {
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
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub
              initialEntries={[
                context.parameters.initialEntry ?? '/workspaces/finance/activity?tree=1',
              ]}
            />
          </PageLayout>
        </div>
      )
    },
  ],
} satisfies Meta<typeof Activity>
export default meta
type Story = StoryObj<typeof meta>

const records = (canvasElement: HTMLElement) =>
  within(canvasElement).findByRole('table', { name: 'Activity records' })
/** The words that state the range of time the axis shows. */
const range = (canvasElement: HTMLElement) =>
  within(within(canvasElement).getByRole('group', { name: 'Timeline controls' })).getByText(/UTC/)
const rowNames = (table: HTMLElement) =>
  within(table)
    .getAllByRole('row')
    .slice(1)
    .map((row) => row.querySelector('[data-record-link]')?.textContent)

/** The parts of the time breakdown, by name. */
const timeParts = (canvasElement: HTMLElement) =>
  within(within(canvasElement).getByRole('list', { name: 'Time by part' }))
    .getAllByRole('listitem')
    .map((item) => item.firstElementChild?.textContent)
/**
 * How far below the breakdown's last part the timeline controls start. The breakdown is as
 * tall as its parts, so no blank band parts them.
 */
const gapUnderBreakdown = (canvasElement: HTMLElement) => {
  const canvas = within(canvasElement)
  const parts = canvas.getByRole('list', { name: 'Time by part' }).getBoundingClientRect()
  const controls = canvas.getByRole('group', { name: 'Timeline controls' }).getBoundingClientRect()
  return controls.top - parts.bottom
}

/**
 * Whether the breakdown says that work is in progress. A note that went keeps its place
 * unseen, so no screen reader reads it.
 */
const inProgress = (canvasElement: HTMLElement) => {
  const notes = within(canvasElement).queryAllByText('Work is in progress. These values change.')
  for (const note of notes)
    if (note.getAttribute('role') !== 'status') {
      expect(note).not.toBeVisible()
      expect(note).toHaveAttribute('aria-hidden', 'true')
    }
  return notes.some((note) => note.getAttribute('role') === 'status')
}
/** Whether the breakdown waits for its values. */
const busy = (canvasElement: HTMLElement) =>
  within(canvasElement).getByRole('list', { name: 'Time by part' }).getAttribute('aria-busy') ===
  'true'
/** How far below the breakdown's line, which says why no value shows, the controls start. */
const gapUnderBreakdownLine = (canvasElement: HTMLElement) => {
  const canvas = within(canvasElement)
  const line = canvas.getByText(/^Time is unavailable/).getBoundingClientRect()
  const controls = canvas.getByRole('group', { name: 'Timeline controls' }).getBoundingClientRect()
  return controls.top - line.bottom
}

/** A short root: every record on its own row, and the Run's log and emission as marks. */
export const ShortTask: Story = {
  args: { page: tracePage(task, shortChildren()) },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    await records(canvasElement)
    // The range label states the range the axis shows.
    await expect(range(canvasElement)).toHaveTextContent('14:00:00.000 – 14:01:00.000 UTC')
    // The breakdown reads the whole root: where its time went, part by part. A search outside
    // a Run took time, so its part shows.
    await expect(timeParts(canvasElement)).toEqual([
      'Tool calls',
      'Waiting',
      'Programs',
      'Search and describe',
      'Agent model',
    ])
    // Each part reads as its value, with what it measures; no title replaces them.
    for (const item of within(canvas.getByRole('list', { name: 'Time by part' })).getAllByRole(
      'listitem',
    ))
      await expect(item).toHaveAccessibleName('')
    const model = canvas.getByText('Agent model').closest('li')!
    await expect(model).toHaveTextContent(/^Agent model\S+\d+%/)
    await expect(model).toHaveTextContent(/It is not a direct measurement of the model\.$/)
    // Each part is one row: its name, value and share side by side, its values in one column.
    const [label, value, part] = [...model.children].map((cell) => cell.getBoundingClientRect())
    await expect(Math.abs(label!.top - value!.top)).toBeLessThan(4)
    await expect(value!.left).toBeGreaterThan(label!.right)
    await expect(part!.left).toBeGreaterThan(value!.right)
    const ends = [...canvas.getByRole('list', { name: 'Time by part' }).children].map(
      (item) => item.children[1]!.getBoundingClientRect().right,
    )
    await expect(new Set(ends).size).toBe(1)
    await expect(gapUnderBreakdown(canvasElement)).toBeLessThan(24)
    // The overview says in words what its lines draw: the Run, its two calls and the search.
    await expect(
      canvas.getByRole('slider', { name: 'Visible time range' }),
      // Then, in Call time, what its shaded bands mean.
    ).toHaveAccessibleDescription(/^1 run and 3 tool calls\. Call time compresses long gaps/)
  },
}

/** Three Runs minutes apart. Call time folds the gaps; Wall time shows them to scale. */
export const LongWithGaps: Story = {
  args: {
    page: (() => {
      const long = traceTask({
        endedAt: new Date(T0 + 1_500_000).toISOString(),
      })
      const runs = [0, 600_000, 1_400_000].map((start, index) =>
        traceRun(`gap-${index}`, start + 1_000, start + 9_000),
      )
      return tracePage(long, {
        [long.name]: runs,
        ...Object.fromEntries(
          runs.map((run, index) => [
            run.name,
            [
              traceCall(
                run,
                `gap-call-${index}`,
                [index * 600_000 + 2_000, index * 600_000 + 2_000, index * 600_000 + 8_000],
                linear,
              ),
            ],
          ]),
        ),
      })
    })(),
  },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    await records(canvasElement)
    const folds = () => canvasElement.querySelectorAll('[data-timeline-fold]')
    await expect(canvas.getByRole('radio', { name: 'Call time' })).toBeChecked()
    await waitFor(() => expect(folds().length).toBeGreaterThan(0))
    await expect(range(canvasElement)).toHaveTextContent(/gaps compressed/)
    // The compressed axis still states the true range.
    await expect(range(canvasElement)).toHaveTextContent('14:00:00.000 – 14:25:00.000 UTC')
    // No search or describe outside a Run took time, so its part does not show.
    await expect(timeParts(canvasElement)).toEqual([
      'Tool calls',
      'Waiting',
      'Programs',
      'Agent model',
    ])
    await expect(gapUnderBreakdown(canvasElement)).toBeLessThan(24)
    await userEvent.click(canvas.getByRole('radio', { name: 'Wall time' }))
    await waitFor(() => expect(folds()).toHaveLength(0))
    await expect(range(canvasElement)).not.toHaveTextContent(/compressed/)
    // A closed Run keeps its toggle and its label where an open Run beside it has them.
    const table = await records(canvasElement)
    // Each Run is named by who asked for it.
    const run = (index: number) => {
      const link = within(table).getAllByRole('link', { name: 'you' })[index]!
      return { link, row: within(link.closest<HTMLElement>('[role=row]')!) }
    }
    await userEvent.click(run(1).row.getByRole('button', { name: 'Collapse record' }))
    const left = (element: Element) => element.getBoundingClientRect().left
    await expect(left(run(1).row.getByRole('button', { name: 'Expand record' }))).toBe(
      left(run(0).row.getByRole('button', { name: 'Collapse record' })),
    )
    await expect(left(run(1).link)).toBe(left(run(0).link))
  },
}

/** One Run with hundreds of calls, some at the same time. */
export const ManyCalls: Story = {
  args: {
    page: (() => {
      const run = traceRun('many', 500, 62_000)
      return tracePage(traceTask({ endedAt: new Date(T0 + 63_000).toISOString() }), {
        [task.name]: [run],
        [run.name]: Array.from({ length: 300 }, (_, index) =>
          traceCall(
            run,
            `many-${index}`,
            [
              1_000 + index * 200,
              1_000 + index * 200,
              1_000 + index * 200 + 150 + (index % 7) * 90,
            ],
            linear,
          ),
        ),
      })
    })(),
  },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const table = await records(canvasElement)
    // The column heading, the Run and its calls: the Task's header names the Task above.
    expect(within(table).getAllByRole('row')).toHaveLength(302)
    // Collapsing the Run hides its calls.
    await userEvent.click(within(table).getByRole('button', { name: 'Collapse record' }))
    await waitFor(() => expect(within(table).getAllByRole('row')).toHaveLength(2))
  },
}

/**
 * Work in progress beside every other end: a call that waits for an approval, a call
 * Coral failed, a search with no recorded completion, and a Run that runs.
 */
export const Running: Story = {
  beforeEach: freezeStoryClock,
  args: {
    page: (() => {
      const open = traceTask({ outcome: 'open', endedAt: null })
      const since = storyNow - T0 - 40_000
      const run = traceRun('live', since, null, {
        requiresAction: true,
        actionPage: {
          total: '1',
          next: '',
          actions: [
            {
              kind: 'approval',
              name: `${workspace}/approvals/one`,
              callPath: 'coral.providers.mail.send',
              providerLabel: 'Mail',
              deadline: '2030-09-22T09:15:00Z',
            },
          ],
        },
      })
      return tracePage(open, {
        [open.name]: [traceTool('lost', 'search', since - 20_000, null), run],
        [run.name]: [
          traceCall(run, 'read', [since + 500, since + 500, since + 1_500], {
            ...linear,
            contextImpactBytes: '-1200',
          }),
          traceCall(run, 'failed', [since + 2_000, since + 2_000, since + 6_000], {
            ...linear,
            outcome: 'coral_failure',
          }),
          traceCall(run, 'waits', [since + 8_000, null, null], {
            requiresAction: true,
            callPath: 'coral.providers.mail.send',
            tool: 'send',
          }),
          traceEvent(run, 3, since + 9_000, 'run.cancellation_requested'),
        ],
      })
    })(),
  },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const table = await records(canvasElement)
    // Each end has its own shape and text, not only a colour.
    await expect(within(table).getByText('Waiting for approval')).toBeVisible()
    // A bar is a shape with no role. Each row says in words what its bar's end shows.
    const rows = (words: RegExp) =>
      within(table)
        .getAllByRole('row')
        .filter((row) => words.test(row.textContent ?? ''))
    expect(rows(/In progress\./).length).toBeGreaterThan(0)
    expect(rows(/No recorded end\./)).toHaveLength(1)
    expect(rows(/It waits before dispatch\./)).toHaveLength(1)
    await expect(within(table).getByRole('row', { description: 'result unknown' })).toBeVisible()
    await expect(within(table).getByRole('row', { description: 'failed' })).toBeVisible()
    await expect(
      within(canvasElement).getByText('Work is in progress. These values change.'),
    ).toBeVisible()
  },
}

/** A Run with no Task is a root itself. */
export const StandaloneRun: Story = {
  args: {
    page: (() => {
      const run = traceRun('alone', 0, 4_000, {
        task: '',
        intent: 'Count open issues',
      })
      return tracePage(run, {
        [run.name]: [
          traceCall(run, 'alone-call', [500, 1_500, 3_000], {
            ...linear,
            task: '',
          }),
          { ...traceEvent(run, 2, 3_500, 'program.emitted'), task: '' },
        ],
      })
    })(),
  },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const table = await records(canvasElement)
    // A Run with no Task names itself by who asked for it.
    expect(rowNames(table)).toEqual(['you', 'coral.providers.linear.listIssues'])
    // The call waited one second before Coral sent it.
    await expect(within(table).getByText(/Wait before dispatch: 1\.00s\./)).toBeInTheDocument()
  },
}

/** Children the page does not hold show as a gap, and the breakdown states no value. */
export const UnloadedAndUnavailable: Story = {
  args: {
    page: (() => {
      const children = shortChildren()
      const [, run] = children[task.name] as [ActivityRow, ActivityRow]
      const other = traceRun('withheld', 20_000, 30_000)
      const page = tracePage(task, {
        ...children,
        [task.name]: [...children[task.name]!, other],
      })
      const branch = page.branches[run.name]!
      page.branches[run.name] = { ...branch, total: '40' }
      page.branches[other.name] = { rows: [], next: '', unavailable: true }
      return page
    })(),
  },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const table = await records(canvasElement)
    await expect(within(table).getByText('Not loaded: 34 records.')).toBeVisible()
    await expect(within(table).getByText('This history is unavailable.')).toBeVisible()
    await expect(
      within(canvasElement).getByText('Time is unavailable. Some records are unavailable.'),
    ).toBeVisible()
    // Nothing more loads, so no room waits for the parts.
    await expect(within(canvasElement).queryByText('Agent model')).toBeNull()
    await expect(gapUnderBreakdownLine(canvasElement)).toBeLessThan(24)
  },
}

/** A narrow screen shows the waterfall alone, with a way back to the list. */
export const NarrowWaterfall: Story = {
  args: { page: tracePage(task, shortChildren()) },
  parameters: { chromatic: { viewports: [390] } },
  play: async ({ canvasElement }) => {
    await resizeViewport(390, 800)
    const canvas = within(canvasElement)
    await expect(await records(canvasElement)).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'Back to list' })).toBeVisible()
    await expect(canvas.queryByRole('table', { name: 'Tasks and runs' })).not.toBeInTheDocument()
    // The controls of the axis stay inside the page.
    const controls = canvas.getByRole('group', { name: 'Timeline controls' })
    await expect(controls).toBeVisible()
    await expect(controls.getBoundingClientRect().right).toBeLessThanOrEqual(390)
    await expect(canvas.getByText('Agent model')).toBeVisible()
    await expect(gapUnderBreakdown(canvasElement)).toBeLessThan(24)
  },
}

/** A Run of 1,000 calls, each with a name of its own. */
const longRun = traceRun('long', 500, 59_000)
const longCall = (index: number) =>
  traceCall(
    longRun,
    `long-${index}`,
    [1_000 + index * 50, 1_000 + index * 50, 1_040 + index * 50],
    {
      ...linear,
      callPath: `coral.providers.linear.issue${index}`,
    },
  )
const longSource = coral(
  {
    [task.name]: [longRun],
    [longRun.name]: Array.from({ length: 1000 }, (_, i) => longCall(i)),
  },
  task,
)
/** The page a link to the call at `ordinal` opens: the Run's window that starts at `offset`. */
function longPage(ordinal: number, offset: number): ActivityPage {
  const selected = { ...longCall(ordinal), position: ordinal }
  return {
    ...longSource.load(
      new URLSearchParams([
        ['expand', task.name],
        ['expand', longRun.name],
        [`offset.${longRun.name}`, String(offset)],
      ]),
    ),
    selected: selected.name,
    selectedRow: selected,
    detail: {
      kind: 'invocation',
      invocation: selected,
      arguments: '{}',
      result: { kind: 'unknown' },
      requests: [],
      request: null,
      requestUnavailable: false,
    },
  }
}
/** Asks the `automatic` reader of a story for its next window. */
const READ_NEXT = 'story:read-next'
const readNext = () => window.dispatchEvent(new Event(READ_NEXT))
/** What the waterfall asked its reader to read first. */
const asked = fn<(wanted: TracePreference[]) => void>()

/**
 * The Activity page with a reader that reads `longSource` as the trace reader reads Coral:
 * `automatic` reads the windows in order, one each time the story asks, else it reads only
 * the window the page asks for.
 */
function Reading({
  page,
  automatic,
  source = longSource,
}: {
  page: ActivityPage
  automatic: boolean
  source?: typeof longSource
}) {
  const [store, setStore] = useState(() => absorbPage(openTrace(task.name), page))
  const read = useCallback(
    (wanted: TracePreference[] = []) =>
      setStore((held) => {
        const request = nextTraceRequest(held, wanted)
        return request ? receiveTrace(held, request, source.load(traceQuery(held, request))) : held
      }),
    [source],
  )
  // In order, one window each time the story asks, so each step of the play is exact.
  useEffect(() => {
    if (!automatic) return
    const next = () => read()
    window.addEventListener(READ_NEXT, next)
    return () => window.removeEventListener(READ_NEXT, next)
  }, [automatic, read])
  const prefer = useCallback(
    (wanted: TracePreference[]) => {
      asked(wanted)
      if (!automatic) setTimeout(() => read(wanted), 20)
    },
    [automatic, read],
  )
  const trace = useMemo<ActivityTraceReader>(
    () => ({ store, retry: () => {}, prefer }),
    [store, prefer],
  )
  return <Activity page={page} trace={trace} />
}

const longRow = (table: HTMLElement, ordinal: number) =>
  within(table)
    .queryByRole('link', { name: `coral.providers.linear.issue${ordinal}` })
    ?.closest<HTMLElement>('[role=row]')
/** Whether `row` shows in `table`, under its pinned heading. */
function inView(table: HTMLElement, row: HTMLElement | null | undefined) {
  if (!row) return false
  const head = within(table).getAllByRole('row')[0]!.getBoundingClientRect().bottom
  const box = row.getBoundingClientRect()
  return box.top >= head - 1 && box.bottom <= table.getBoundingClientRect().bottom + 1
}

/**
 * A link to a call deep in a long Run. The rows you read stay where they are while the
 * records before and after them load, and the page asks first for the ones beside them.
 */
export const LoadsBesideTheReader: Story = {
  args: { page: longPage(650, 600) },
  parameters: {
    initialEntry: '/workspaces/finance/activity?tree=1&record=x',
    chromatic: { disableSnapshot: true },
  },
  render: ({ page }) => <Reading page={page} automatic={false} />,
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    asked.mockClear()
    const table = await records(canvasElement)
    // The selected call is in view when the page opens.
    await waitFor(() => expect(inView(table, longRow(table, 650))).toBe(true))
    expect(longRow(table, 650)).toHaveAttribute('aria-selected', 'true')
    const head = () => within(table).getAllByRole('row')[0]!.getBoundingClientRect().bottom
    await within(document.body).findByRole('dialog')
    // Measured in the table, which the rail that opens beside it can move on the page.
    const top = (ordinal: number) =>
      longRow(table, ordinal)!.getBoundingClientRect().top - table.getBoundingClientRect().top

    // You scroll up until the records that are not loaded come into view above the first call.
    const above = within(table).getByText('Loading 600 records…')
    table.scrollTop += above.getBoundingClientRect().bottom - head() - 4
    await waitFor(() => expect(asked).toHaveBeenLastCalledWith([{ parent: longRun.name, at: 599 }]))
    const before = top(600)
    // The hundred calls that end there load, and the call you read stays where it was.
    await waitFor(() => expect(longRow(table, 500)).toBeTruthy())
    await waitFor(() => expect(Math.abs(top(600) - before)).toBeLessThanOrEqual(1))
    expect(inView(table, longRow(table, 600))).toBe(true)
    expect(longRow(table, 499)).toBeFalsy()

    // You scroll down to the records that are not loaded after the last call.
    const below = within(table).getByText('Loading 300 records…')
    table.scrollTop += below.getBoundingClientRect().top - table.getBoundingClientRect().bottom + 4
    await waitFor(() => expect(asked).toHaveBeenLastCalledWith([{ parent: longRun.name, at: 700 }]))
    const last = top(699)
    await waitFor(() => expect(longRow(table, 700)).toBeTruthy())
    await waitFor(() => expect(Math.abs(top(699) - last)).toBeLessThanOrEqual(1))
    // The selection is the same record throughout.
    expect(longRow(table, 650)).toHaveAttribute('aria-selected', 'true')
  },
}

/**
 * A link to a call the first window does not hold. The page shows what it holds, keeps
 * the rows you see in place while the rest loads, and brings the call into view when it loads.
 */
export const SelectedRecordArrives: Story = {
  args: { page: { ...longPage(650, 0) } },
  parameters: {
    initialEntry: '/workspaces/finance/activity?tree=1&record=x',
    chromatic: { disableSnapshot: true },
  },
  render: ({ page }) => <Reading page={page} automatic />,
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const table = await records(canvasElement)
    // The first calls show at once, and stay in place while later windows load under them.
    // Measured in the table, which the rail that opens beside it can move on the page.
    const first = () =>
      longRow(table, 0)!.getBoundingClientRect().top - table.getBoundingClientRect().top
    await within(document.body).findByRole('dialog')
    const before = first()
    readNext()
    readNext()
    await waitFor(() => expect(longRow(table, 250)).toBeTruthy())
    expect(longRow(table, 650)).toBeFalsy()
    expect(Math.abs(first() - before)).toBeLessThanOrEqual(1)
    // Its window loads: the selected call comes into view by itself.
    for (let window = 300; window < 700; window += 100) readNext()
    await waitFor(() => expect(inView(table, longRow(table, 650))).toBe(true))
    expect(longRow(table, 650)).toHaveAttribute('aria-selected', 'true')
    // The rest loads under it, and it stays in view.
    for (let window = 700; window < 1000; window += 100) readNext()
    await waitFor(() => expect(longRow(table, 999)).toBeTruthy())
    expect(inView(table, longRow(table, 650))).toBe(true)
  },
}

/** A Task with a search of its own, and a Run of 250 calls that loads window by window. */
const searchedRun = traceRun('searched', 1_000, 40_000)
const searchedSource = coral(
  {
    [task.name]: [traceTool('own-search', 'search', 0, 500), searchedRun],
    [searchedRun.name]: Array.from({ length: 250 }, (_, index) =>
      traceCall(searchedRun, `searched-${index}`, [
        2_000 + index * 100,
        2_000 + index * 100,
        2_050 + index * 100,
      ]),
    ),
  },
  task,
)

/**
 * The breakdown holds the room of the parts it will show while records load, so the table
 * under it stays put when the values replace the loading line.
 */
export const BreakdownKeepsItsRoom: Story = {
  args: {
    page: searchedSource.load(
      new URLSearchParams([
        ['expand', task.name],
        ['expand', searchedRun.name],
      ]),
    ),
  },
  parameters: { chromatic: { disableSnapshot: true } },
  render: ({ page }) => <Reading page={page} automatic source={searchedSource} />,
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    const table = await records(canvasElement)
    // While records load, each part keeps its name, and a placeholder stands for its value:
    // no number a screen reader would read, and one status that says the records load.
    const parts = canvas.getByRole('list', { name: 'Time by part' })
    await expect(parts).toHaveAttribute('aria-busy', 'true')
    await expect(timeParts(canvasElement)).toEqual([
      'Tool calls',
      'Waiting',
      'Programs',
      'Search and describe',
      'Agent model',
    ])
    await expect(parts.textContent).not.toMatch(/\d/)
    await expect(
      canvas
        .getAllByRole('status')
        .filter((status) => status.textContent!.startsWith('Loading records…')),
    ).toHaveLength(1)
    const top = table.getBoundingClientRect().top
    const rows = () =>
      [...parts.children].map((item) => item.firstElementChild!.getBoundingClientRect().top)
    const loading = rows()
    for (let read = 0; read < 5 && busy(canvasElement); read++) {
      readNext()
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    await waitFor(() => expect(busy(canvasElement)).toBe(false))
    await expect(timeParts(canvasElement)).toContain('Search and describe')
    // Each part's name stays where it was.
    await expect(rows()).toEqual(loading)
    await expect(Math.abs(table.getBoundingClientRect().top - top)).toBeLessThanOrEqual(1)
  },
}

/** Moves `Stages` to its next trace, or with `reset`, back to its first. */
const STAGE = 'waterfall-stage'
const stage = (reset = false) => window.dispatchEvent(new CustomEvent(STAGE, { detail: { reset } }))

/** The Activity page over `stores`, one after the other, as Live reads them. */
function Stages({ page, stores }: { page: ActivityPage; stores: ActivityTraceStore[] }) {
  const [at, setAt] = useState(0)
  // A reset mounts the page afresh, so nothing it held for an earlier width stays.
  const [mount, setMount] = useState(0)
  useEffect(() => {
    const next = (event: Event) => {
      if ((event as CustomEvent<{ reset: boolean }>).detail.reset) {
        setMount((count) => count + 1)
        setAt(0)
      } else setAt((index) => Math.min(index + 1, stores.length - 1))
    }
    window.addEventListener(STAGE, next)
    return () => window.removeEventListener(STAGE, next)
  }, [stores])
  const trace = useMemo<ActivityTraceReader>(
    () => ({ store: stores[at]!, retry: () => {}, prefer: () => {} }),
    [stores, at],
  )
  return <Activity key={mount} page={page} trace={trace} />
}

/**
 * Steps through `states` at 1440px and at 390px, and checks that the table under the
 * breakdown starts where it started: `moves` says by how many of its own rows it may move
 * down at each state, once and for good.
 */
async function holdsTheTable(
  canvasElement: HTMLElement,
  states: { reached: () => unknown; moves?: number; arrived?: () => Element }[],
) {
  const canvas = within(canvasElement)
  for (const width of [1440, 390]) {
    await resizeViewport(width, 800)
    stage(true)
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
    const table = await records(canvasElement)
    await waitFor(() => expect(busy(canvasElement)).toBe(false))
    const row = canvas.getByText('Agent model').getBoundingClientRect().height
    let top = table.getBoundingClientRect().top
    const scrolled = table.scrollTop
    for (const [index, { reached, moves = 0, arrived }] of states.entries()) {
      stage()
      await waitFor(reached)
      await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
      const now = table.getBoundingClientRect().top
      await expect({ width, state: index + 1, moved: Math.round(now - top) }).toEqual({
        width,
        state: index + 1,
        moved: moves ? expect.any(Number) : 0,
      })
      // A move is down only, by no more than the lines that arrived.
      if (moves) {
        await expect(now - top).toBeGreaterThanOrEqual(0)
        // A line that arrived wraps as it must at this width: it counts at its own height.
        const lines = arrived ? arrived().getBoundingClientRect().height + 8 : 0
        await expect(now - top).toBeLessThanOrEqual((moves - (arrived ? 1 : 0)) * (row + 8) + lines)
      }
      // The rows you read stay where they were in the table.
      await expect(table.scrollTop).toBe(scrolled)
      top = now
    }
  }
}

/** A story `beforeEach`: the page reads the time the live Run ends at, as Live would. */
const atLiveEnd = () => {
  const realNow = Date.now
  Date.now = () => T0 + 9_000
  return () => {
    Date.now = realNow
  }
}
const liveTask = traceTask({ outcome: 'open', endedAt: null })
const liveRun = traceRun('live', 0, null)
const liveFirst = traceCall(liveRun, 'live-1', [1_000, 1_000, 2_000], linear)
const liveEnded = traceRun('live', 0, 9_000)
const liveLast = traceCall(liveEnded, 'live-2', [3_000, 3_000, 4_000], linear)
/** A search of the Task's own that never recorded its end. */
const lostSearch = traceTool('lost', 'search', 500, null)

/**
 * A Run of a live Task ends. The breakdown said that work goes on and that a record has no
 * recorded end; it then waits for the Run's last records, and states the values without
 * work in progress. The table under it stays put throughout.
 */
export const LiveRunEnds: Story = {
  beforeEach: atLiveEnd,
  args: { page: tracePage(liveTask, { [liveTask.name]: [lostSearch, liveRun] }) },
  parameters: { chromatic: { disableSnapshot: true } },
  render: ({ page }) => {
    const going = traceStore(liveTask, {
      [liveTask.name]: [lostSearch, liveRun],
      [liveRun.name]: [liveFirst],
    })
    const waiting = absorbPage(
      going,
      tracePage(liveTask, { [liveTask.name]: [lostSearch, liveEnded] }),
    )
    const ended = absorbPage(
      waiting,
      tracePage(liveTask, { [liveEnded.name]: [liveFirst, liveLast] }),
    )
    return <Stages page={page} stores={[going, waiting, ended]} />
  },
  play: async ({ canvasElement }) => {
    await holdsTheTable(canvasElement, [
      { reached: () => expect(busy(canvasElement)).toBe(true) },
      { reached: () => expect(inProgress(canvasElement)).toBe(false) },
    ])
  },
}

/**
 * A live Task makes its first search of its own, and later a Run ends. The search's part
 * then shows, so the table moves down once by that part, and stays there.
 */
export const LiveTaskSearches: Story = {
  beforeEach: atLiveEnd,
  args: { page: tracePage(liveTask, { [liveTask.name]: [liveRun] }) },
  parameters: { chromatic: { disableSnapshot: true } },
  render: ({ page }) => {
    const search = traceTool('first', 'search', 2_500, 2_700)
    const going = traceStore(liveTask, {
      [liveTask.name]: [liveRun],
      [liveRun.name]: [liveFirst],
    })
    const searched = absorbPage(going, tracePage(liveTask, { [liveTask.name]: [liveRun, search] }))
    const waiting = absorbPage(
      searched,
      tracePage(liveTask, { [liveTask.name]: [liveEnded, search] }),
    )
    const ended = absorbPage(
      waiting,
      tracePage(liveTask, { [liveEnded.name]: [liveFirst, liveLast] }),
    )
    return <Stages page={page} stores={[going, searched, waiting, ended]} />
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await holdsTheTable(canvasElement, [
      { reached: () => expect(canvas.getByText('Search and describe')).toBeVisible(), moves: 1 },
      { reached: () => expect(busy(canvasElement)).toBe(true) },
      { reached: () => expect(inProgress(canvasElement)).toBe(false) },
    ])
  },
}

/**
 * The Agent model part explains its limits from an info control beside its name: a
 * button that Tab reaches and Enter, a click or a touch opens. Its values still read.
 */
export const AgentModelInfo: Story = {
  args: { page: tracePage(task, shortChildren()) },
  play: async ({ canvasElement }) => {
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    await waitFor(() => expect(busy(canvasElement)).toBe(false))
    const model = canvas.getByText('Agent model').closest('li')!
    const info = within(model).getByRole('button', { name: 'About Agent model' })
    // The part still reads as its name, its value and its share.
    await expect(model).toHaveTextContent(/^Agent model\S+\d+%/)
    const explanation = () =>
      within(document.body).queryByRole('dialog', { name: 'About Agent model' })

    // The keyboard reaches it, opens it and closes it, and focus comes back.
    info.focus()
    await expect(info).toHaveFocus()
    await press('{Enter}')
    await waitFor(() => expect(explanation()).toBeVisible())
    await expect(explanation()).toHaveTextContent(
      'It is not a direct measurement of the model. It can also include time that Coral did not record',
    )
    await press('{Escape}')
    await waitFor(() => expect(explanation()).toBeNull())
    await expect(info).toHaveFocus()

    // A click opens it.
    await click(info)
    await waitFor(() => expect(explanation()).toBeVisible())
    await press('{Escape}')
    await waitFor(() => expect(explanation()).toBeNull())

    // A touch opens it too: a touch screen has no hover.
    await userEvent.pointer({ keys: '[TouchA]', target: info })
    await waitFor(() => expect(explanation()).toBeVisible())
  },
}

/**
 * A live Task's first search of its own arrives while it still runs: Coral records the
 * search before its end. Its part and the note that a record has no recorded end show
 * at once, so the table moves down once by that part and that note, which wraps on a
 * narrow screen, and then stays put while the search ends and a Run ends.
 */
export const LiveSearchInProgress: Story = {
  beforeEach: atLiveEnd,
  args: { page: tracePage(liveTask, { [liveTask.name]: [liveRun] }) },
  parameters: { chromatic: { disableSnapshot: true } },
  render: ({ page }) => {
    const going = traceStore(liveTask, {
      [liveTask.name]: [liveRun],
      [liveRun.name]: [liveFirst],
    })
    const started = absorbPage(
      going,
      tracePage(liveTask, {
        [liveTask.name]: [liveRun, traceTool('first', 'search', 2_500, null)],
      }),
    )
    const done = traceTool('first', 'search', 2_500, 2_700)
    const searched = absorbPage(started, tracePage(liveTask, { [liveTask.name]: [liveRun, done] }))
    const waiting = absorbPage(
      searched,
      tracePage(liveTask, { [liveTask.name]: [liveEnded, done] }),
    )
    const ended = absorbPage(
      waiting,
      tracePage(liveTask, { [liveEnded.name]: [liveFirst, liveLast] }),
    )
    return <Stages page={page} stores={[going, started, searched, waiting, ended]} />
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await holdsTheTable(canvasElement, [
      {
        reached: () => expect(canvas.getByText(/^1 record has no recorded end\./)).toBeVisible(),
        moves: 2,
        arrived: () => canvas.getByText(/^1 record has no recorded end\./),
      },
      {
        reached: () =>
          expect(canvas.getByText(/^1 record has no recorded end\./)).not.toBeVisible(),
      },
      { reached: () => expect(busy(canvasElement)).toBe(true) },
      { reached: () => expect(inProgress(canvasElement)).toBe(false) },
    ])
  },
}

/**
 * Each row has two lines: its record and its numbers, then its track across the table.
 * Every track starts and ends where the overview and the axis do, however deep its record
 * sits. Zoomed in, a bar that goes on past an end of its track says so at that end.
 */
export const TwoLineTracks: Story = {
  args: { page: tracePage(task, shortChildren()) },
  play: async ({ canvasElement }) => {
    for (const width of [1440, 390]) {
      await resizeViewport(width, 900)
      const canvas = within(canvasElement)
      const table = await records(canvasElement)
      await waitFor(() => expect(busy(canvasElement)).toBe(false))
      await userEvent.click(canvas.getByRole('button', { name: 'Fit the whole timeline' }))
      const tracks = [...table.querySelectorAll<HTMLElement>('[role=row] [data-timeline-track]')]
      await expect(tracks.length).toBeGreaterThan(2)
      const box = (element: Element) => element.getBoundingClientRect()
      // The overview's box, inside whose border it draws its lines.
      const minimap = box(canvas.getByRole('slider', { name: 'Visible time range' }).parentElement!)
      // The axis's track: the one above the table that is not the overview.
      const axis = box(
        [...canvasElement.querySelectorAll('[data-timeline-track]')].find(
          (track) => !table.contains(track) && !track.closest('[role=slider]'),
        )!,
      )
      for (const track of tracks) {
        // One width for every track, the axis and the inside of the overview's border.
        await expect(Math.round(box(track).left)).toBe(Math.round(axis.left))
        await expect(Math.round(box(track).right)).toBe(Math.round(axis.right))
        await expect(Math.round(box(track).left)).toBe(Math.round(minimap.left + 1))
        await expect(Math.round(box(track).right)).toBe(Math.round(minimap.right - 1))
        // The track is the row's second line, under its record.
        const row = track.closest('[role=row]')!
        const record = row.querySelector('[data-record-link]')!
        await expect(box(track).top).toBeGreaterThanOrEqual(box(record).bottom)
        // Two lines of set heights, and a border under every row but the last.
        await expect([60, 61]).toContain(Math.round(box(row).height))
      }
      // Fit: each Run's line in the overview starts and ends where its bar does.
      const spans = [
        ...canvas.getByRole('slider', { name: 'Visible time range' }).parentElement!.children,
      ].filter((element) => element.getAttribute('style')?.includes('--'))
      for (const run of table.querySelectorAll('[role=row][data-record-key*="/runs/"]')) {
        const bar = box(run.querySelector('[data-timeline-bar]')!)
        await expect(
          spans.some(
            (span) =>
              Math.abs(box(span).left - bar.left) < 0.5 &&
              Math.abs(box(span).right - bar.right) < 0.5,
          ),
        ).toBe(true)
      }
      // Fit: every bar is in view, so no end says that one goes on past it.
      const edges = () => [...table.querySelectorAll<HTMLElement>('[data-timeline-edge]')]
      for (const edge of edges()) await expect(getComputedStyle(edge).opacity).toBe('0')

      // Zoomed in, an end shows exactly where its bar goes on past the track.
      for (let step = 0; step < 4; step++)
        await userEvent.click(canvas.getByRole('button', { name: 'Zoom in' }))
      await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)))
      let shown = 0
      for (const edge of edges()) {
        const track = box(edge.closest('[data-timeline-track]')!)
        const bar = box(edge.parentElement!.querySelector('[data-timeline-bar]')!)
        const past =
          edge.dataset.timelineEdge === 'before'
            ? bar.left < track.left - 1
            : bar.right > track.right + 1
        await expect({
          edge: edge.dataset.timelineEdge,
          shown: getComputedStyle(edge).opacity === '1',
        }).toEqual({
          edge: edge.dataset.timelineEdge,
          shown: past,
        })
        if (past) shown++
      }
      await expect(shown).toBeGreaterThan(0)
    }
  },
}
