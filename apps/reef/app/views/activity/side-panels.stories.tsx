import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { listStyles } from '@/wax/components/list'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import type { ConnectionState } from '../accounts/types'
import { ActionReviewSession } from '../actions/review'
import { Activity } from './activity'
import { stored } from './discovery/fixtures'
import {
  approval,
  approvalAction,
  approvalDetail,
  at,
  connection,
  connectionAction,
  connectionDetail,
  discovery,
  endedInvocation,
  endedRun,
  failureMessages,
  groupedError,
  interaction,
  invocation,
  invocationDetail,
  invocationResults,
  later,
  outputRow,
  panelPage,
  providerFilteredSearch,
  run,
  runDetail,
  searched,
  settle,
  task,
  toolRow,
  workspace,
  type ApprovalState,
  type CancellationActor,
  type InteractionOutcome,
  type LogLevel,
  type TaskOutcome,
} from './side-panel-fixtures'
import { freezeStoryClock } from './story-clock'
import type {
  ActivityPage,
  DiscoveryContext,
  InteractionEvidence,
  InteractionRow,
  InvocationDetail,
  InvocationOutcome,
  InvocationState,
  RunDetail,
  RunEvent,
  RunFailureCode,
  RunState,
} from './types'

/**
 * Every Activity side panel, opened on load. A story names the record kind and
 * the state Coral reports for it. `evidence` is exact retained text the open
 * panel must show, whatever its layout.
 */
const meta = {
  title: 'Activity/Side Panels',
  component: Activity,
  beforeEach: freezeStoryClock,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [1440] } },
  render: (args) => (
    <ActionReviewSession
      request={
        args.page.detail?.kind === 'run' || args.page.detail?.kind === 'invocation'
          ? args.page.detail.request
          : null
      }
    >
      <Activity {...args} />
    </ActionReviewSession>
  ),
  decorators: [
    (Story, context) => {
      const { page } = context.args
      const query = new URLSearchParams()
      if (page.selected) query.set('record', page.selected)
      const detail = page.detail
      const request =
        detail?.kind === 'run' || detail?.kind === 'invocation' ? detail.request?.name : undefined
      if (request) query.set('request', request)
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub initialEntries={[`${page.listPath}?${query}`]} />
          </PageLayout>
        </div>
      )
    },
  ],
  play: async ({ canvasElement, parameters, args }) => {
    const opened = await within(canvasElement.ownerDocument.body).findByRole('dialog')
    await waitFor(() => expect(opened).toBeVisible())
    for (const text of [parameters.evidence ?? []].flat())
      await expect(opened).toHaveTextContent(text)
    // A tool, log or emission shows beside its Run's accepted program, highlighted at
    // the line Coral stored for it, where that program makes its call. Every one has one here.
    const row = args.page.selectedRow
    const detail = args.page.detail
    const call =
      row?.kind === 'invocation'
        ? row.callPath.slice(row.callPath.lastIndexOf('.') + 1)
        : row?.kind === 'output' && row.eventKind === 'program.emitted'
          ? 'emit'
          : row?.kind === 'output' && row.eventKind === 'program.logged'
            ? /^(debug|info|warn|error|log)$/
            : null
    if (call && (row?.kind === 'invocation' || row?.kind === 'output')) {
      const location = row.sourceLocation!
      await expect(location).toBeDefined()
      const program =
        detail?.kind === 'invocation' || detail?.kind === 'output' ? detail.context?.source : null
      const named = program!.split('\n')[location.line - 1]!.slice(location.column - 1)
      const called = named.split('(')[0]!
      if (typeof call === 'string') await expect(called).toBe(call)
      else await expect(called).toMatch(call)
      const source = within(opened).getByRole('region', { name: 'Accepted source' })
      await waitFor(() =>
        expect(
          [...source.querySelectorAll<HTMLElement>('[data-highlighted]')].map(
            (line) => line.dataset.line,
          ),
        ).toEqual([`${location.line}`]),
      )
      await expect(within(source).getByRole('status')).toHaveTextContent(`Line ${location.line}`)
    }
    // Beside a Run's source, a decision sits in the record pane's own footer, below the
    // scrolling evidence, so the evidence's fade never covers it. A review's one named
    // region holds both its evidence and that decision.
    const selected = within(opened).queryByRole('region', { name: 'Selected record' })
    const decisions = selected
      ? within(selected).queryAllByRole('button', { name: /this call$|^Decline$|^Connect / })
      : []
    for (const control of decisions) {
      const footer = control.closest('footer')!
      await expect(selected).toContainElement(footer)
      await expect(footer.previousElementSibling).toContainElement(
        within(selected!).getAllByRole('heading')[0]!,
      )
    }
    const approvals = selected
      ? within(selected).queryAllByRole('button', { name: /this call$/ })
      : []
    if (approvals.length) {
      const reviews = within(opened).getAllByRole('region', { name: /^Review / })
      await expect(reviews).toHaveLength(1)
      await expect(reviews[0]!.lastElementChild).toBe(approvals[0]!.closest('footer'))
      await expect(reviews[0]!.querySelector('header')).not.toBeNull()
    }
  },
} satisfies Meta<typeof Activity>
export default meta
type Story = StoryObj<typeof meta>

const reconnecting = { connected: false, followRoot: false, atStart() {}, viewport() {} }
/** A short, narrow phone screen. Chromatic reads `viewports`; Vitest reads the global. */
const narrow = {
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  parameters: { chromatic: { viewports: [320] } },
}

/** One panel story: `page` opens on load and shows each `evidence` text. */
function panel(
  name: string,
  page: ActivityPage,
  options: { evidence?: string | string[]; live?: typeof reconnecting; narrow?: boolean } = {},
): Story {
  return {
    name,
    args: { page, ...(options.live && { live: options.live }) },
    ...(options.narrow && { globals: narrow.globals }),
    parameters: {
      evidence: options.evidence,
      ...(options.narrow && narrow.parameters),
    },
  }
}
/**
 * The panel's header reads `label` as its badge, or no badge when null, and none of Coral's
 * outcome names (`coral_failure`, `run_terminated`, `indeterminate`) leak into it.
 */
function headerSays(story: Story, label: string | null): Story {
  return {
    ...story,
    play: async (context) => {
      await (story.play ?? meta.play)(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const header = dialog.querySelector('header')!
      await expect(header).not.toHaveTextContent(/coral failure|run terminated|indeterminate|_/)
      if (label) await expect(within(header).getByText(label)).toBeVisible()
      else
        for (const badge of ['retained', 'diagnostic'])
          await expect(header).not.toHaveTextContent(badge)
      await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    },
  }
}
const runPage = (detail: RunDetail, overrides?: Partial<ActivityPage>) =>
  panelPage(
    {
      ...detail.run,
      kind: 'run',
      submission: detail.submission,
      hasFinalValue: detail.terminal.kind === 'succeeded' && detail.terminal.finalValue !== null,
    },
    detail,
    overrides,
  )
const invocationPage = (detail: InvocationDetail) =>
  panelPage({ ...detail.invocation, kind: 'invocation' }, detail)
const toolPage = (value: InteractionRow, evidence: InteractionEvidence) =>
  panelPage(toolRow(value), {
    kind: 'tool',
    task: toolRow(value).task,
    interaction: value,
    evidence,
  })
/** A Run event, beside the Run's accepted program, as Coral's loader reads it. */
const eventPage = (event: RunEvent, label: string) =>
  panelPage(outputRow(event, label), {
    kind: 'output',
    run: run().name,
    event,
    context: runDetail(run()),
  })

// Tasks ---------------------------------------------------------------------

const longGoal =
  'Find every overdue invoice in the Finance workspace, group them by account owner, and send each owner one reminder that lists the invoice numbers, amounts, and due dates.'
const tasks = {
  open: contextStory(
    panel(
      'Task · open goal, partial context savings',
      taskPage(task({ contextImpactBytes: '-55500', contextImpactPartial: true })),
      { evidence: 'Find overdue invoices and notify their owners' },
    ),
    { label: '−55.5 kB', meaning: '55.5 kB saved so far', tone: 'saved' },
  ),
  success: contextStory(
    panel(
      'Task · goal succeeded, with context savings and completion feedback',
      taskPage(
        task({ outcome: 'success', endedAt: later, contextImpactBytes: '-2200000' }),
        'Sent 24 reminders to 9 owners.',
      ),
      { evidence: 'Sent 24 reminders to 9 owners.' },
    ),
    { label: '−2.2 MB', meaning: '2.2 MB saved', tone: 'saved' },
  ),
  failure: contextStory(
    panel(
      'Task · goal failed, with context cost and completion feedback',
      taskPage(
        task({ outcome: 'failure', endedAt: later, contextImpactBytes: '14500' }),
        'I could not find a tool to export invoices.\nSearch returned no matching tools.',
      ),
      { evidence: 'I could not find a tool to export invoices.' },
    ),
    { label: '+14.5 kB', meaning: '14.5 kB added', tone: 'added' },
  ),
} satisfies Record<TaskOutcome, Story>
function taskPage(row: ReturnType<typeof task>, feedback?: string) {
  return panelPage(row, { kind: 'task', task: row, ...(feedback !== undefined && { feedback }) })
}
/**
 * The Task's Context row leads its list and matches its table cell exactly: the same
 * text, meaning and colour. Savings read green and costs red, each with a sign.
 */
function contextStory(
  story: Story,
  expected: { label: string; meaning: string; tone: 'saved' | 'added' | 'neutral' },
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const body = within(context.canvasElement.ownerDocument.body)
      const opened = await body.findByRole('dialog')
      const terms = within(opened)
        .getAllByRole('term')
        .map((term) => term.textContent)
      await expect(terms.slice(0, 2)).toEqual(['Context', 'Started'])
      const shown = within(opened).getByLabelText(expected.meaning)
      await expect(shown).toHaveTextContent(expected.label)
      const cell = within(body.getByRole('table')).getByLabelText(expected.meaning)
      const color = (element: Element) => getComputedStyle(element).color
      await expect(color(shown)).toBe(color(cell))
      const [red, green] = color(shown)
        .match(/\d+(\.\d+)?/g)!
        .map(Number)
      if (expected.tone === 'saved') await expect(green).toBeGreaterThan(red)
      if (expected.tone === 'added') await expect(red).toBeGreaterThan(green)
    },
  }
}
export const TaskOpen = tasks.open
export const TaskSucceeded = tasks.success
export const TaskFailed = tasks.failure
export const TaskNoContextChange = contextStory(
  panel(
    'Task · no net context change',
    taskPage(task({ outcome: 'success', endedAt: later, contextImpactBytes: '0' })),
  ),
  { label: '0 B', meaning: 'No net context change', tone: 'neutral' },
)
export const TaskContextUnavailable = contextStory(
  panel('Task · context total unavailable', taskPage(task({ outcome: 'success', endedAt: later }))),
  { label: '—', meaning: 'Context impact unavailable', tone: 'neutral' },
)
export const TaskLongGoalNarrow = panel(
  'Task · long goal, narrow screen',
  taskPage(task({ intent: longGoal })),
  { evidence: longGoal, narrow: true },
)

// Tool calls: search and describe evidence -------------------------------------

const searchCall = interaction('search', 'succeeded')
const describeCall = (callPath: string) =>
  interaction('describe', 'succeeded', {
    intent: 'Inspect the inputs before calling it',
    name: `${searchCall.name}-${callPath}`,
  })
export const SearchResults = panel(
  'Search · results, complete coverage',
  toolPage(searchCall, discovery(searched, stored.complete)),
  { evidence: ['github pulls', 'coral.providers.github.pulls.merge', 'Results (5+)'] },
)
/** Compact results on a phone: long call paths wrap inside their row, beside their link. */
export const SearchResultsNarrow: Story = {
  ...panel(
    'Search · results, narrow screen',
    toolPage(searchCall, discovery(searched, stored.complete)),
    {
      evidence: ['coral.providers.github.pulls.getMergeAsyncResult', 'Results (5+)'],
      narrow: true,
    },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    const rows = within(
      within(dialog).getByRole('list', { name: 'Results in rank order' }),
    ).getAllByRole('listitem')
    await expect(rows).toHaveLength(5)
    for (const row of rows) {
      const edge = row.getBoundingClientRect()
      for (const part of [row.querySelector('code')!, within(row).getByRole('link')]) {
        const box = part.getBoundingClientRect()
        await expect(box.left >= edge.left && box.right <= edge.right).toBe(true)
      }
    }
    // Each card's own Details opens inside it; long signatures and descriptions wrap.
    for (const row of rows) {
      const summary = within(row).getByText('Details', { selector: 'summary' })
      await context.userEvent.click(summary)
      const details = summary.closest('details')!
      await expect(details).toHaveAttribute('open')
      await expect(within(details).getByText('Signature')).toBeVisible()
      await expect(row.scrollWidth).toBeLessThanOrEqual(row.clientWidth)
      await expect(details.querySelector('pre')!.scrollWidth).toBeLessThanOrEqual(
        details.querySelector('pre')!.clientWidth,
      )
    }
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    for (const row of rows) await context.userEvent.click(within(row).getByText('Details'))
    for (const row of rows) await expect(row.querySelector('details')).not.toHaveAttribute('open')
  },
}
export const SearchProviderFiltered = panel(
  'Search · one provider, saved functions skipped',
  toolPage(searchCall, discovery({ ...searched, provider: 'github' }, providerFilteredSearch)),
  { evidence: ['github pulls', 'Results (5+)', 'skipped by the provider filter'] },
)
export const SearchPartialCoverage = panel(
  'Search · partial coverage',
  toolPage(searchCall, discovery(searched, stored.partial)),
  { evidence: ['github pulls', 'Results (3)', 'Results may be missing.'] },
)
export const SearchFailedCoverage = panel(
  'Search · a kind could not be searched',
  toolPage(searchCall, discovery(searched, stored.error)),
  { evidence: ['github pulls', 'Results (1)', 'Results are missing.'] },
)
/** A later page counts only its own results; nothing matched and nothing remains. */
export const SearchLaterPageEmpty = panel(
  'Search · a later page with no results',
  toolPage(
    searchCall,
    discovery({ ...searched, cursor: 'next-page' }, JSON.stringify({ hits: [], has_more: false })),
  ),
  { evidence: ['a later page, up to 5 results', 'Results (0)', 'This page has no results.'] },
)
export const SearchMixedResults = panel(
  'Search · tools and saved functions',
  toolPage(searchCall, discovery({ ...searched, query: 'github issues by label' }, stored.mixed)),
  { evidence: ['github issues by label', 'Results (6+)'] },
)
/** Only a result that retained documentation gets Details; the other keeps its summary. */
export const SearchSparseResults: Story = {
  ...panel(
    'Search · a result without retained documentation',
    toolPage(searchCall, discovery(searched, stored.sparse)),
    { evidence: ['Results (2)', 'coral.providers.github.pulls.get'] },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const [documented, bare] = within(
      within(dialog).getByRole('list', { name: 'Results in rank order' }),
    ).getAllByRole('listitem')
    await expect(within(bare).getByText('coral.providers.github.pulls.get')).toBeVisible()
    await expect(bare.querySelector('details')).toBeNull()
    await expect(within(bare).queryByText('Details')).toBeNull()
    const summary = within(documented).getByText('Details', { selector: 'summary' })
    const description = within(documented).getByText(/^Lists the open issues of one repository/)
    await expect(description).not.toBeVisible()
    await context.userEvent.click(summary)
    await expect(description).toBeVisible()
    await expect(within(documented).queryByText('Guidance')).toBeNull()
  },
}
export const SearchTargetsGone = panel(
  'Search · results no longer in the catalog',
  toolPage(searchCall, discovery(searched, stored.complete, { gone: true })),
  { evidence: 'coral.providers.github.pulls.merge' },
)
export const SearchEarlierShape = panel(
  'Search · response stored in an earlier shape',
  toolPage(searchCall, discovery(searched, stored.earlierShape)),
  { evidence: 'typescript_declaration' },
)
const describedTool: Extract<DiscoveryContext, { kind: 'describe' }> = {
  kind: 'describe',
  callPath: 'coral.providers.github.issues.create',
  types: [],
}
const describedFunction: Extract<DiscoveryContext, { kind: 'describe' }> = {
  kind: 'describe',
  callPath: 'coral.functions.github.openIssuesByLabel',
  types: ['Issue', 'Label'],
}
/** A panel's one list of facts: no heading, and the same gap between every pair of rows, so
 * the times read on from the parameters before them. Details may hold lists of its own. */
async function expectOneEvenList(dialog: HTMLElement, terms: string[]) {
  const lists = [...dialog.querySelectorAll('dl')].filter((list) => !list.closest('details'))
  await expect(lists).toHaveLength(1)
  const [list] = lists
  await expect(list.closest('section')!.querySelector('h4')).toBeNull()
  await expect(
    within(list)
      .getAllByRole('term')
      .map((term) => term.textContent),
  ).toEqual(terms)
  const rows = [...list.children].map((row) => row.getBoundingClientRect())
  const gaps = rows.slice(1).map((row, index) => Math.round(row.top - rows[index].bottom))
  for (const gap of gaps) await expect(gap).toBe(gaps[0])
}
/** A describe reads like a search: what it asked, its times, a compact Result, then a closed
 * Details that holds every retained part of the documentation. Opening shows `folded`;
 * closing hides it again. */
function describeStory(
  story: Story,
  folded: string[],
  options: { terms: string[]; fits?: boolean; carded?: boolean },
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const body = within(context.canvasElement.ownerDocument.body)
      const dialog = await body.findByRole('dialog')
      const opened = within(dialog)
      // What it asked and its times read as one evenly spaced list, without a Describe heading.
      await expect(opened.queryByRole('heading', { name: 'Describe' })).toBeNull()
      await expectOneEvenList(dialog, options.terms)
      await expect(opened.queryByText('Technical details')).toBeNull()
      const summary = opened.getByText('Details', { selector: 'summary' })
      const details = summary.closest('details')!
      await expect(details).not.toHaveAttribute('open')
      // A described target keeps Details at the foot of its Result card.
      const card = details.parentElement!
      if (options.carded) {
        const result = opened.getByRole('region', { name: 'Result' })
        await expect(result).toContainElement(card)
        await expect(card.lastElementChild).toBe(details)
        await expect(within(card).getAllByRole('link').length).toBeGreaterThan(0)
      }
      // A describe never shows its raw response, inline or folded.
      await expect(opened.queryByText('Raw response')).toBeNull()
      await expect(dialog).not.toHaveTextContent('"opaque"')
      await expect(dialog.querySelectorAll('details')).toHaveLength(1)
      const closedHeight = card.getBoundingClientRect().height
      await context.userEvent.click(summary)
      await expect(details).toHaveAttribute('open')
      for (const text of folded) {
        const shown = within(details).getByText(text)
        await expect(shown).toBeVisible()
        if (options.carded) await expect(card).toContainElement(shown)
      }
      if (options.carded) {
        await expect(card.getBoundingClientRect().height).toBeGreaterThan(closedHeight)
        await expect(details.getBoundingClientRect().width).toBeGreaterThan(card.clientWidth * 0.8)
      }
      if (options.fits) {
        await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
        // The card clips, so check that nothing inside it is wider than the card either.
        await expect(card.scrollWidth).toBeLessThanOrEqual(card.clientWidth)
      }
      await context.userEvent.click(summary)
      await expect(details).not.toHaveAttribute('open')
    },
  }
}
export const DescribeTool = describeStory(
  panel(
    'Describe · tool',
    toolPage(describeCall('issues.create'), discovery(describedTool, stored.describeTool)),
    { evidence: ['Create an issue', 'GitHub', 'Result'] },
  ),
  ['Declaration', 'API method documentation'],
  { terms: ['Call path', 'Provider', 'Intent', 'Started', 'Completed'], carded: true },
)
export const DescribeSavedFunction = describeStory(
  panel(
    'Describe · saved function, with diagnostics',
    toolPage(
      describeCall('openIssuesByLabel'),
      discovery(describedFunction, stored.describeFunction),
    ),
    { evidence: ['1 error', 'Issue', 'Label'] },
  ),
  ['Source', "Cannot find name 'coral'."],
  { terms: ['Call path', 'Types', 'Intent', 'Started', 'Completed'], carded: true },
)
/** Long call paths and long documentation stay inside a phone screen, open or closed. */
export const DescribeNarrow = describeStory(
  panel(
    'Describe · long documentation, narrow screen',
    toolPage(describeCall('issues.create'), discovery(describedTool, stored.describeTool)),
    { evidence: 'coral.providers.github.issues.create', narrow: true },
  ),
  ['Declaration'],
  {
    terms: ['Call path', 'Provider', 'Intent', 'Started', 'Completed'],
    fits: true,
    carded: true,
  },
)
/** A describe that retained nothing to describe shows its notice and times, then ends: no
 * Details holding only the record name, no raw response and no rule left below the notice. */
function noticeStory(story: Story, notice: string, times: string[]): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      const shown = opened.getByText(notice)
      await expect(shown).toBeVisible()
      const timing = [...dialog.querySelectorAll('dl')].at(-1)!
      await expect(
        within(timing)
          .getAllByRole('term')
          .map((term) => term.textContent),
      ).toEqual(times)
      await expect(dialog.querySelector('details')).toBeNull()
      for (const gone of ['Details', 'Raw response', 'Technical details'])
        await expect(opened.queryByText(gone)).toBeNull()
      await expect(dialog).not.toHaveTextContent('"opaque"')
      const after = [...dialog.querySelectorAll('*')].filter(
        (element) =>
          shown.compareDocumentPosition(element) & Node.DOCUMENT_POSITION_FOLLOWING &&
          !shown.contains(element),
      )
      // Only what renders can draw a rule; an empty footer slot does not render.
      const rules = after.filter((element) => {
        const style = getComputedStyle(element)
        return (
          element.getClientRects().length > 0 &&
          (element.tagName === 'HR' ||
            (style.borderTopStyle !== 'none' && style.borderTopWidth !== '0px'))
        )
      })
      await expect(rules.map((element) => `${element.tagName}.${element.className}`)).toEqual([])
    },
  }
}
export const DescribeRejected = noticeStory(
  panel(
    'Describe · rejected',
    toolPage(
      interaction('describe', 'rejected', {
        intent: 'Inspect the inputs before calling it',
        name: `${searchCall.name}-rejected-describe`,
      }),
      { kind: 'rejection', code: 'invalid_argument', message: 'Unknown call path.' },
    ),
    { evidence: ['Rejected: invalid_argument', 'Unknown call path.'] },
  ),
  'Unknown call path.',
  ['Started', 'Completed'],
)
export const DescribeUnknown = noticeStory(
  panel(
    'Describe · completion unknown',
    toolPage(
      interaction('describe', 'unknown', {
        intent: 'Inspect the inputs before calling it',
        name: `${searchCall.name}-unknown-describe`,
      }),
      { kind: 'unknown' },
    ),
    { evidence: 'Completion is unknown or incomplete.' },
  ),
  'Completion is unknown or incomplete. External effects may be uncertain.',
  ['Started'],
)
export const DescribeContextUnavailable = noticeStory(
  panel(
    'Describe · request context unavailable',
    toolPage(describeCall('opaque'), discovery({ kind: 'unknown' }, '{"opaque":true}')),
    { evidence: 'The request context is unavailable.' },
  ),
  'The request context is unavailable.',
  ['Started', 'Completed'],
)

// Tool calls: other evidence -------------------------------------------------------

const execCall = (outcome: InteractionOutcome, overrides: Partial<InteractionRow> = {}) =>
  interaction('exec', outcome, { intent: 'Send invoice reminders', ...overrides })
const admissionError = "program.ts(3,7): error TS2304: Cannot find name 'invoices'."
/** A tsc report with a long line, indentation and a caret, as Coral stores it. */
const longAdmissionError = [
  "program.ts(12,41): error TS2345: Argument of type '{ owner: string; repo: string; state: \"open\"; labels: string[]; per_page: number; }' is not assignable to parameter of type 'ListForRepoInput'.",
  "  Object literal may only specify known properties, and 'labels' does not exist in type 'ListForRepoInput'.",
  '    const issues = await coral.providers.github.issues.listForRepo({ owner, repo, state: "open", labels })',
  '                                                                                         ^',
].join('\n')
/**
 * An exec submission states its times and intent, its outcome only in the badge, then the exact error of a
 * rejection in the card a search result uses, or a notice. It keeps no technical details and no
 * Run link: the submission's outcome is not the Run's.
 */
function execStory(
  story: Story,
  expected: {
    times: string[]
    outcome: string
    error?: string
    notice?: string
    fits?: boolean
    unknown?: boolean
  },
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      const lists = dialog.querySelectorAll('dl')
      await expect(lists).toHaveLength(1)
      await expect(
        within(lists[0])
          .getAllByRole('term')
          .map((term) => term.textContent),
      ).toEqual(expected.times)
      // The title is the intent, so no Intent row repeats it.
      await expect(opened.queryByText('Intent', { selector: 'dt' })).toBeNull()
      // The header badge states the submission outcome, once; no row repeats it.
      const header = dialog.querySelector('header')!
      await expect(within(header).getByText(expected.outcome)).toBeVisible()
      await expect(opened.getAllByText(expected.outcome)).toHaveLength(1)
      await expect(dialog).not.toHaveTextContent('Submission outcome')
      for (const heading of ['Context', 'Call'])
        await expect(opened.queryByRole('heading', { name: heading })).toBeNull()
      for (const gone of ['Technical details', 'Run accepted', 'Open execution', 'Rejected:'])
        await expect(dialog).not.toHaveTextContent(gone)
      await expect(dialog.querySelector('details')).toBeNull()
      await expect(opened.queryAllByRole('link')).toHaveLength(0)
      if (expected.notice) await expect(opened.getByText(expected.notice)).toBeVisible()
      if (expected.unknown) {
        // The warning icon and the badge say it; no sentence repeats it.
        await expect(within(header).getByText('result unknown')).toBeVisible()
        await expect(header.querySelector('svg')).toBeVisible()
        await expect(dialog).not.toHaveTextContent('Completion is unknown')
        await expect(dialog).not.toHaveTextContent('External effects may be uncertain')
      }
      const region = opened.queryByRole('region', { name: 'Error' })
      if (expected.error === undefined) {
        await expect(region).toBeNull()
        return
      }
      // The same Wax list and card a search result uses, holding only the exact error.
      await expect(region).toHaveClass(listStyles.container)
      const card = region!.firstElementChild as HTMLElement
      await expect(card).toHaveClass(listStyles.item)
      const code = card.querySelector('pre')!
      await expect(code).toBeVisible()
      await expect(code.textContent).toBe(expected.error)
      await expect(card.textContent).toBe(expected.error)
      if (expected.fits) {
        // A long diagnostic wraps inside its card, keeping its lines and indentation.
        await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
        await expect(card.scrollWidth).toBeLessThanOrEqual(card.clientWidth)
        await expect(code.scrollWidth).toBeLessThanOrEqual(code.clientWidth)
        await expect(getComputedStyle(code).whiteSpace).toBe('pre-wrap')
        const edge = card.getBoundingClientRect()
        const box = code.getBoundingClientRect()
        await expect(box.left >= edge.left && box.right <= edge.right).toBe(true)
      }
    },
  }
}
const saveCall = (outcome: InteractionOutcome) =>
  interaction('save_function', outcome, { intent: 'Save the reminder batch' })
const deleteCall = (outcome: InteractionOutcome) =>
  interaction('delete_function', outcome, { intent: 'Remove the old reminder batch' })
/** A save or a delete retains only the function's identity, a Coral-generated ID. */
const savedId = '0199a7c2-3b4d-7e5f-8a6b-1c2d3e4f5a6b'
const savedReference = `${workspace}/savedFunctions/${savedId}`
const saveError = 'reminders.ts(1,1): error TS1128: Declaration or statement expected.\n  }\n  ^'
const deleteError = "Saved function 'billing.oldReminders' does not exist."
/**
 * A save or a delete states its intent, the function it changed (its ID, with no link: no route
 * opens a function by ID), then its times, in one evenly spaced list. Its outcome shows only in
 * the badge. A rejection shows the exact error in the card a search result uses; otherwise a
 * notice may follow. The call retained no source, declaration or diagnostics, so nothing folds
 * into Details, and no record path or workspace shows.
 */
function functionStory(
  story: Story,
  expected: {
    times: string[]
    outcome: string
    changed?: 'Deleted'
    error?: string
    notice?: string
    /** Text a save without that evidence must not show. */
    silent?: string[]
    fits?: boolean
  },
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      for (const text of expected.silent ?? []) await expect(dialog).not.toHaveTextContent(text)
      // The title is the intent, so no Intent row repeats it.
      await expectOneEvenList(dialog, [
        ...(expected.changed ? [expected.changed] : []),
        ...expected.times,
      ])
      const header = dialog.querySelector('header')!
      await expect(within(header).getByText(expected.outcome)).toBeVisible()
      await expect(opened.getAllByText(expected.outcome)).toHaveLength(1)
      for (const heading of ['Context', 'Call', 'Result'])
        await expect(opened.queryByRole('heading', { name: heading })).toBeNull()
      for (const gone of [
        'Submission outcome',
        'Technical details',
        'Workspace',
        'workspaces/',
        'Rejected:',
        'Completion is unknown',
      ])
        await expect(dialog).not.toHaveTextContent(gone)
      await expect(dialog.querySelector('details')).toBeNull()
      await expect(opened.queryAllByRole('link')).toHaveLength(0)
      if (expected.notice) await expect(opened.getByText(expected.notice)).toBeVisible()
      if (expected.changed) {
        const value = opened.getByText(expected.changed, { selector: 'dt' }).nextElementSibling!
        await expect(value.textContent).toBe(savedId)
        await expect(value.querySelector('code')).toBeVisible()
      } else {
        // A save states no target: it retained only an ID that no page opens.
        await expect(opened.queryByText('Saved', { selector: 'dt' })).toBeNull()
        await expect(dialog).not.toHaveTextContent(savedId)
      }
      const error = opened.queryByRole('region', { name: 'Error' })
      let card: HTMLElement | null = null
      if (expected.error === undefined) await expect(error).toBeNull()
      else {
        await expect(error).toHaveClass(listStyles.container)
        card = error!.firstElementChild as HTMLElement
        await expect(card).toHaveClass(listStyles.item)
        await expect(card.textContent).toBe(expected.error)
        await expect(card.querySelector('pre')).toBeVisible()
      }
      if (expected.fits) {
        await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
        const edge = (card ?? dialog.querySelector('dl')!).getBoundingClientRect()
        for (const part of (card ?? dialog.querySelector('dl')!).querySelectorAll('code, pre')) {
          const box = part.getBoundingClientRect()
          await expect(box.left >= edge.left && box.right <= edge.right).toBe(true)
        }
        if (card) await expect(card.scrollWidth).toBeLessThanOrEqual(card.clientWidth)
      }
    },
  }
}
const toolEvidence = {
  discovery: SearchResults,
  run: execStory(
    panel(
      'Exec submission · accepted, run started',
      toolPage(execCall('succeeded', { acceptedRun: run().name }), {
        kind: 'run',
        reference: run().name,
      }),
      { evidence: 'Send invoice reminders' },
    ),
    { times: ['Started', 'Completed'], outcome: 'succeeded' },
  ),
  function: functionStory(
    panel(
      'Save function · saved',
      toolPage(saveCall('succeeded'), { kind: 'function', reference: savedReference }),
      { evidence: 'Save the reminder batch' },
    ),
    { times: ['Started', 'Completed'], outcome: 'succeeded' },
  ),
  rejection: execStory(
    panel(
      'Exec submission · rejected',
      toolPage(execCall('rejected'), {
        kind: 'rejection',
        code: 'program_admission_failed',
        message: admissionError,
      }),
      { evidence: "Cannot find name 'invoices'." },
    ),
    { times: ['Started', 'Completed'], outcome: 'rejected', error: admissionError },
  ),
  unknown: execStory(
    panel(
      'Exec submission · completion unknown',
      toolPage(execCall('unknown', { completedAt: null }), { kind: 'unknown' }),
      { evidence: 'Send invoice reminders' },
    ),
    { times: ['Started'], outcome: 'result unknown', unknown: true },
  ),
} satisfies Record<InteractionEvidence['kind'], Story>
export const ExecAccepted = toolEvidence.run
export const ExecRejected = toolEvidence.rejection
export const ExecCompletionUnknown = toolEvidence.unknown
/** A long tsc report on a phone: it wraps inside its card and keeps its lines. */
export const ExecRejectedNarrow = execStory(
  panel(
    'Exec submission · rejected, long diagnostic, narrow screen',
    toolPage(execCall('rejected'), {
      kind: 'rejection',
      code: 'program_admission_failed',
      message: longAdmissionError,
    }),
    { evidence: "'labels' does not exist in type 'ListForRepoInput'", narrow: true },
  ),
  {
    times: ['Started', 'Completed'],
    outcome: 'rejected',
    error: longAdmissionError,
    fits: true,
  },
)
/** A rejection that kept no message still says it failed, and why if Coral kept a code. */
export const ExecRejectedWithoutMessage = execStory(
  panel(
    'Exec submission · rejected, no message',
    toolPage(execCall('rejected'), {
      kind: 'rejection',
      code: 'program_admission_failed',
      message: '',
    }),
    { evidence: 'Rejected with no message: program_admission_failed' },
  ),
  {
    times: ['Started', 'Completed'],
    outcome: 'rejected',
    notice: 'Rejected with no message: program_admission_failed',
  },
)
/** A submission whose stored evidence is missing says so, without guessing a Run. */
export const ExecEvidenceUnavailable = execStory(
  panel(
    'Exec submission · evidence unavailable',
    toolPage(execCall('succeeded'), { kind: 'unknown' }),
    { evidence: 'The submission evidence is unavailable.' },
  ),
  {
    times: ['Started', 'Completed'],
    outcome: 'succeeded',
    notice: 'The submission evidence is unavailable.',
  },
)
export const SaveFunctionSaved = toolEvidence.function
/** A save on a phone: its intent and times, nothing wider than the screen. */
export const SaveFunctionNarrow = functionStory(
  panel(
    'Save function · saved, narrow screen',
    toolPage(saveCall('succeeded'), { kind: 'function', reference: savedReference }),
    { evidence: 'Save the reminder batch', narrow: true },
  ),
  { times: ['Started', 'Completed'], outcome: 'succeeded', fits: true },
)
/** A save that retained no intent is titled by its tool. Its function ID opens no page, so it
 * shows nowhere, not even as the title. */
const unnamedSave = functionStory(
  panel(
    'Save function · saved, no intent retained',
    toolPage(saveCall('succeeded'), { kind: 'function', reference: savedReference }),
    { evidence: 'Save function' },
  ),
  { times: ['Started', 'Completed'], outcome: 'succeeded' },
)
export const SaveFunctionWithoutIntent: Story = {
  ...unnamedSave,
  args: {
    page: toolPage(interaction('save_function', 'succeeded', { intent: '' }), {
      kind: 'function',
      reference: savedReference,
    }),
  },
}
export const SaveFunctionRejected = functionStory(
  panel(
    'Save function · rejected',
    toolPage(saveCall('rejected'), {
      kind: 'rejection',
      code: 'saved_function_admission_failed',
      message: saveError,
    }),
    { evidence: 'Declaration or statement expected.' },
  ),
  { times: ['Started', 'Completed'], outcome: 'rejected', error: saveError },
)
/** A long compiler report on a phone wraps inside its card and keeps its lines. */
export const SaveFunctionRejectedNarrow = functionStory(
  panel(
    'Save function · rejected, long diagnostic, narrow screen',
    toolPage(saveCall('rejected'), {
      kind: 'rejection',
      code: 'saved_function_admission_failed',
      message: longAdmissionError,
    }),
    { evidence: "'labels' does not exist in type 'ListForRepoInput'", narrow: true },
  ),
  { times: ['Started', 'Completed'], outcome: 'rejected', error: longAdmissionError, fits: true },
)
export const SaveFunctionRejectedWithoutMessage = functionStory(
  panel(
    'Save function · rejected, no message',
    toolPage(saveCall('rejected'), {
      kind: 'rejection',
      code: 'saved_function_admission_failed',
      message: '',
    }),
    { evidence: 'Save the reminder batch' },
  ),
  {
    times: ['Started', 'Completed'],
    outcome: 'rejected',
    silent: ['Rejected with no message', 'saved_function_admission_failed'],
  },
)
export const SaveFunctionCompletionUnknown = functionStory(
  panel('Save function · completion unknown', toolPage(saveCall('unknown'), { kind: 'unknown' }), {
    evidence: 'Save the reminder batch',
  }),
  { times: ['Started'], outcome: 'result unknown' },
)
export const SaveFunctionEvidenceUnavailable = functionStory(
  panel(
    'Save function · evidence unavailable',
    toolPage(saveCall('succeeded'), { kind: 'unknown' }),
    { evidence: 'Save the reminder batch' },
  ),
  { times: ['Started', 'Completed'], outcome: 'succeeded', silent: ['evidence is unavailable'] },
)
export const DeleteFunctionDeleted = functionStory(
  panel(
    'Delete function · deleted',
    toolPage(deleteCall('succeeded'), { kind: 'function', reference: savedReference }),
    { evidence: savedId },
  ),
  { times: ['Started', 'Completed'], outcome: 'succeeded', changed: 'Deleted' },
)
export const DeleteFunctionNarrow = functionStory(
  panel(
    'Delete function · deleted, narrow screen',
    toolPage(deleteCall('succeeded'), { kind: 'function', reference: savedReference }),
    { evidence: savedId, narrow: true },
  ),
  { times: ['Started', 'Completed'], outcome: 'succeeded', changed: 'Deleted', fits: true },
)
export const DeleteFunctionRejected = functionStory(
  panel(
    'Delete function · rejected',
    toolPage(deleteCall('rejected'), {
      kind: 'rejection',
      code: 'not_found',
      message: deleteError,
    }),
    { evidence: deleteError },
  ),
  { times: ['Started', 'Completed'], outcome: 'rejected', error: deleteError },
)
export const DeleteFunctionRejectedNarrow = functionStory(
  panel(
    'Delete function · rejected, long message, narrow screen',
    toolPage(deleteCall('rejected'), {
      kind: 'rejection',
      code: 'not_found',
      message: longAdmissionError,
    }),
    { evidence: "'labels' does not exist in type 'ListForRepoInput'", narrow: true },
  ),
  { times: ['Started', 'Completed'], outcome: 'rejected', error: longAdmissionError, fits: true },
)
export const DeleteFunctionCompletionUnknown = functionStory(
  panel(
    'Delete function · completion unknown',
    toolPage(deleteCall('unknown'), { kind: 'unknown' }),
    { evidence: 'Remove the old reminder batch' },
  ),
  { times: ['Started'], outcome: 'result unknown' },
)
export const DeleteFunctionEvidenceUnavailable = functionStory(
  panel(
    'Delete function · evidence unavailable',
    toolPage(deleteCall('succeeded'), { kind: 'unknown' }),
    { evidence: 'The delete evidence is unavailable.' },
  ),
  {
    times: ['Started', 'Completed'],
    outcome: 'succeeded',
    notice: 'The delete evidence is unavailable.',
  },
)
/** A rejected search states its outcome in the badge and its exact error in the card a search
 * result uses: no "Rejected: code" line, no results and no raw response. */
function searchRejectedStory(story: Story, code: string, message: string, fits = false): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      await expect(opened.getAllByText('rejected')).toHaveLength(1)
      await expectOneEvenList(dialog, ['Started', 'Completed'])
      for (const gone of ['Rejected:', code, 'Results', 'Raw response', 'Technical details'])
        await expect(dialog).not.toHaveTextContent(gone)
      await expect(dialog.querySelector('details')).toBeNull()
      const error = opened.getByRole('region', { name: 'Error' })
      await expect(error).toHaveClass(listStyles.container)
      const card = error.firstElementChild as HTMLElement
      await expect(card).toHaveClass(listStyles.item)
      await expect(card.textContent).toBe(message)
      await expect(card.querySelector('pre')).toBeVisible()
      if (fits) {
        await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
        await expect(card.scrollWidth).toBeLessThanOrEqual(card.clientWidth)
      }
    },
  }
}
export const SearchRejected = searchRejectedStory(
  panel(
    'Search · rejected',
    toolPage(interaction('search', 'rejected'), {
      kind: 'rejection',
      code: 'invalid_cursor',
      message: 'Search cursor does not match this request',
    }),
    { evidence: 'Search cursor does not match this request' },
  ),
  'invalid_cursor',
  'Search cursor does not match this request',
)
const longSearchError = [
  'Search cursor does not match this request: eyJ2ZXJzaW9uIjoxLCJwb3NpdGlvbiI6NSwiZGlnZXN0IjoiNWMyMDc2MWY2MzJkZjYyZWI2NzY0MDNiMjY3Y2Q4NGMxZTE2NzNlM2Y0MTMyMDI2Y2M0N2ZhNDRiOTcyYWE3NiJ9',
  '  query: "github pulls"  provider: all',
].join('\n')
/** A long error on a phone wraps inside its card and keeps its lines. */
export const SearchRejectedNarrow = searchRejectedStory(
  panel(
    'Search · rejected, long error, narrow screen',
    toolPage(interaction('search', 'rejected'), {
      kind: 'rejection',
      code: 'invalid_cursor',
      message: longSearchError,
    }),
    { evidence: 'query: "github pulls"', narrow: true },
  ),
  'invalid_cursor',
  longSearchError,
  true,
)

// Program Runs ---------------------------------------------------------------

const finalValue = '{"reminded":24,"owners":9}'
export const RunRunning: Story = {
  ...panel('Program run · running', runPage(runDetail(run())), {
    evidence: 'Send invoice reminders',
  }),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const cancel = within(dialog).getByRole('button', { name: 'Cancel run' })
    // A destructive button: filled red, not a bare text control.
    const [red, green, blue] = getComputedStyle(cancel).backgroundColor.match(/\d+/g)!.map(Number)
    await expect(red).toBeGreaterThan(green + 60)
    await expect(red).toBeGreaterThan(blue + 60)
    // It ends at the panel's end edge, as the timing list does.
    const list = within(dialog).getByText('Started', { selector: 'dt' }).closest('dl')!
    const end = list.getBoundingClientRect().right
    await expect(Math.abs(cancel.getBoundingClientRect().right - end)).toBeLessThanOrEqual(1)
    // Its duration reads in whole time, with no fraction of a second, and keeps counting.
    const duration = within(list).getByText('Duration', { selector: 'dt' }).nextElementSibling!
    await expect(duration.textContent).toMatch(/^(\d+s|\d+m \d+s|\d+h \d+m)$/)
  },
}
export const RunRunningReconnecting = panel(
  'Program run · running, reconnecting',
  runPage(runDetail(run())),
  { live: reconnecting, evidence: 'Send invoice reminders' },
)
/** A Run's current actions, one card each in the list a search result uses: what each is, its
 * review link, its kind and its deadline. The count is the Run's total; this page shows two. */
function actionsStory(
  story: Story,
  expected: { total: string; cards: { target: string; link: string; kind: string }[] },
  options: { fits?: boolean; reached?: number; missing?: string } = {},
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const nav = within(dialog).getByRole('navigation', { name: 'Actions in this run' })
      await expect(
        within(nav).getByText(
          `${expected.total} ${expected.total === '1' ? 'action needs' : 'actions need'} you.`,
        ),
      ).toBeVisible()
      const list = within(nav).getByRole('list')
      await expect(list).toHaveClass(listStyles.container)
      const items = within(list).getAllByRole('listitem')
      await expect(items).toHaveLength(expected.cards.length)
      for (const [index, card] of expected.cards.entries()) {
        const item = items[index]
        await expect(item).toHaveClass(listStyles.item)
        await expect(within(item).getByText(card.target)).toBeVisible()
        await expect(within(item).getByText(card.kind)).toBeVisible()
        const link = within(item).getByRole('link', { name: card.link })
        await expect(link).toBeVisible()
        await expect(link.getAttribute('href')).toContain('request=')
        const time = item.querySelector('time')!
        await expect(time).toBeVisible()
        await expect(time).toHaveTextContent(
          index === options.reached ? 'Deadline reached.' : 'Expires in 15 minutes',
        )
      }
      // Nothing links to another page of them until every loaded action shows.
      await expect(
        within(nav).queryByRole('link', { name: /actions in this run|First actions/ }),
      ).toBeNull()
      // A list a failed page left short says so, and offers to read it again.
      if (options.missing) {
        await expect(within(nav).getByRole('status')).toHaveTextContent(options.missing)
        await expect(within(nav).getByRole('button', { name: 'Try again' })).toBeVisible()
      } else await expect(within(nav).queryByRole('status')).toBeNull()
      // The Run's own cancel control stays below, at the panel's end.
      await expect(within(dialog).getByRole('button', { name: 'Cancel run' })).toBeVisible()
      if (options.fits) {
        await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
        for (const item of items) {
          const edge = item.getBoundingClientRect()
          for (const part of item.querySelectorAll('code, a, time')) {
            const box = part.getBoundingClientRect()
            await expect(box.left >= edge.left - 1 && box.right <= edge.right + 1).toBe(true)
          }
        }
      }
    },
  }
}
export const RunWaitingOnActions = actionsStory(
  panel(
    'Program run · waiting on several actions',
    runPage(
      runDetail(
        run({
          requiresAction: true,
          actionPage: {
            total: '3',
            next: '',
            actions: [
              approvalAction,
              {
                ...approvalAction,
                name: `${workspace}/approvals/update`,
                callPath: 'coral.providers.billing.updateInvoice',
                providerLabel: 'Billing',
              },
              connectionAction,
            ],
          },
        }),
      ),
    ),
    { evidence: 'coral.providers.mail.send' },
  ),
  {
    total: '3',
    cards: [
      {
        target: 'coral.providers.mail.send',
        link: 'Review coral.providers.mail.send',
        kind: 'approval',
      },
      {
        target: 'coral.providers.billing.updateInvoice',
        link: 'Review coral.providers.billing.updateInvoice',
        kind: 'approval',
      },
      {
        target: 'Connect your Linear account',
        link: 'Connect your Linear account',
        kind: 'account connection',
      },
    ],
  },
)
/** Twelve actions on a phone: the first five show, with the count of all twelve. Show more
 * lists the rest in order, the list scrolls with the panel, and Tab reaches each review. */
const manyActions = Array.from({ length: 12 }, (_, index) =>
  index % 4 === 3
    ? {
        ...connectionAction,
        name: `${workspace}/accountConnections/provider-${index}`,
        providerLabel: `Provider ${index}`,
      }
    : {
        ...approvalAction,
        name: `${workspace}/approvals/action-${index}`,
        callPath: `coral.providers.billing.invoices.sendOverdueReminder${index}`,
      },
)
const manyCards = manyActions.map((item) =>
  item.kind === 'approval'
    ? { target: item.callPath, link: `Review ${item.callPath}`, kind: 'approval' }
    : {
        target: `Connect your ${item.providerLabel} account`,
        link: `Connect your ${item.providerLabel} account`,
        kind: 'account connection',
      },
)
const manyStory = actionsStory(
  panel(
    'Program run · waiting on twelve actions, narrow screen',
    runPage(
      runDetail(
        run({ requiresAction: true, actionPage: { total: '12', next: '', actions: manyActions } }),
      ),
    ),
    { evidence: manyCards[4].target, narrow: true },
  ),
  { total: '12', cards: manyCards.slice(0, 5) },
  { fits: true },
)
export const RunWaitingOnManyActionsNarrow: Story = {
  ...manyStory,
  play: async (context) => {
    await manyStory.play!(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const nav = within(dialog).getByRole('navigation', { name: 'Actions in this run' })
    await expect(within(nav).getByText('Showing 5 of 12 actions.')).toBeVisible()
    await context.userEvent.click(within(nav).getByRole('button', { name: 'Show more actions' }))
    await expect(within(nav).queryByText(/^Showing/)).toBeNull()
    await expect(within(nav).getByRole('button', { name: 'Show fewer actions' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    const links = within(within(nav).getByRole('list')).getAllByRole('link')
    // Each link opens its own action's exact review, in the Run's order.
    const requests = links.map((link) =>
      new URL(link.getAttribute('href')!, 'http://reef').searchParams.get('request'),
    )
    await expect(requests).toEqual(manyActions.map((item) => item.name))
    // Tab moves through the reviews in list order.
    links[0].focus()
    for (const link of links.slice(1)) {
      await context.userEvent.tab()
      await expect(link).toHaveFocus()
    }
    // The last card scrolls into view inside the panel.
    await expect(links.at(-1)).toBeVisible()
  },
}
/** More actions than one page of Coral's: the first five show, then the rest of the page, then a
 * link to the next page. */
const pagedActions = Array.from({ length: 100 }, (_, index) => ({
  ...approvalAction,
  name: `${workspace}/approvals/action-${index}`,
  callPath: `coral.providers.billing.invoices.send${index}`,
}))
const pagedStory = actionsStory(
  panel(
    'Program run · waiting on more actions than one page',
    runPage(
      runDetail(
        run({
          requiresAction: true,
          actionPage: { total: '150', next: 'cursor-1', actions: pagedActions },
        }),
      ),
    ),
    { evidence: 'Showing 5 of 150 actions.' },
  ),
  {
    total: '150',
    cards: pagedActions.slice(0, 5).map((item) => ({
      target: item.callPath,
      link: `Review ${item.callPath}`,
      kind: 'approval',
    })),
  },
)
export const RunWaitingOnMoreActionsThanOnePage: Story = {
  ...pagedStory,
  play: async (context) => {
    await pagedStory.play!(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const nav = within(dialog).getByRole('navigation', { name: 'Actions in this run' })
    await context.userEvent.click(within(nav).getByRole('button', { name: 'Show more actions' }))
    await expect(within(within(nav).getByRole('list')).getAllByRole('listitem')).toHaveLength(100)
    await expect(within(nav).getByText('Showing 100 of 150 actions.')).toBeVisible()
    const next = within(nav).getByRole('link', { name: 'More actions in this run' })
    await expect(next).toBeVisible()
    const query = new URL(next.getAttribute('href')!, 'http://reef').searchParams
    await expect(query.get('actionsAfter')).toBe('cursor-1')
    await expect(query.has('request')).toBe(false)
  },
}
/** A later page of actions failed: the list shows what loaded and says how many are missing. */
export const RunWaitingOnActionsIncomplete = actionsStory(
  panel(
    'Program run · some actions did not load',
    runPage(
      runDetail(
        run({
          requiresAction: true,
          actionPage: {
            total: '150',
            next: '',
            incomplete: true,
            actions: [approvalAction, connectionAction],
          },
        }),
      ),
    ),
    { evidence: 'The rest did not load.' },
  ),
  {
    total: '150',
    cards: [
      {
        target: 'coral.providers.mail.send',
        link: 'Review coral.providers.mail.send',
        kind: 'approval',
      },
      {
        target: 'Connect your Linear account',
        link: 'Connect your Linear account',
        kind: 'account connection',
      },
    ],
  },
  { missing: '2 of 150 actions loaded. The rest did not load.' },
)
const longActionPath =
  'coral.providers.enterpriseResourcePlanning.accountsReceivable.invoices.sendOverdueReminderToPrimaryBillingContact'
/** On a phone, a long call path wraps inside its card; an approval and a connection read apart,
 * and a deadline that has passed says so. */
export const RunWaitingOnActionsNarrow = actionsStory(
  panel(
    'Program run · waiting on an approval and a connection, narrow screen',
    runPage(
      runDetail(
        run({
          requiresAction: true,
          actionPage: {
            total: '2',
            next: '',
            actions: [
              { ...approvalAction, callPath: longActionPath },
              { ...connectionAction, deadline: later },
            ],
          },
        }),
      ),
    ),
    { evidence: 'Connect your Linear account', narrow: true },
  ),
  {
    total: '2',
    cards: [
      { target: longActionPath, link: `Review ${longActionPath}`, kind: 'approval' },
      {
        target: 'Connect your Linear account',
        link: 'Connect your Linear account',
        kind: 'account connection',
      },
    ],
  },
  { fits: true, reached: 1 },
)
export const RunRequestUnavailable = panel(
  'Program run · selected request unavailable',
  runPage(runDetail(run(), { requestUnavailable: true })),
  { evidence: 'Send invoice reminders' },
)
export const RunPersonalOutsideTask = panel(
  'Program run · personal, outside a task',
  runPage(
    runDetail(
      endedRun('succeeded', {
        name: `${workspace}/runs/personal`,
        agent: '',
        actor: 'you',
        actorLabel: 'You',
        task: '',
        intent: '',
        submission: undefined,
      }),
      { terminal: { kind: 'succeeded', finalValue: '[]' }, source: 'return []' },
    ),
  ),
)
export const RunLabelsUnavailable = panel(
  'Program run · agent and workspace labels unavailable',
  runPage(
    runDetail(endedRun('succeeded', { actorLabel: '', workspaceLabel: '' }), {
      terminal: { kind: 'succeeded', finalValue },
    }),
  ),
  { evidence: '"reminded"' },
)
export const RunSucceededWithFinalValue = panel(
  'Program run · succeeded, final value',
  runPage(runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue } })),
  { evidence: '"owners"' },
)
/** What the program returned, with a precise amount and a trailing zero Coral keeps as stored. */
const returned = '{"reminded":24,"owners":9,"totalOverdue":18450.70,"skipped":["INV-4180"]}'
/**
 * The panel a "final value" row opens. That row names `<run>/final`; the loader answers
 * with its succeeded Run, so the Run panel shows the value the program returned. That is
 * the program's result, not an intermediate `coral.emit` output.
 */
export const FinalValue: Story = {
  ...panel(
    'Final value',
    runPage(
      runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue: returned } }),
    ),
    { evidence: '"totalOverdue"' },
  ),
  // Literal, so Storybook's static index shows this name too.
  name: 'Final value',
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const header = dialog.querySelector('header')!
    await expect(
      within(header).getByRole('heading', { level: 2, name: 'Send invoice reminders' }),
    ).toBeVisible()
    await expect(header).toHaveTextContent('program run')
    await expect(within(header).getByText('succeeded', { exact: true })).toBeVisible()
    // The returned value shows directly in its own card, not behind a disclosure.
    const value = within(dialog).getByRole('region', { name: 'Final value' })
    await expect(value).toBeVisible()
    await expect(within(value).getByRole('group', { name: 'Structure' })).toBeVisible()
    await expect(within(dialog).queryByText('Final value', { selector: 'summary' })).toBeNull()
    for (const token of ['"reminded"', '24', '"owners"', '9', '18450.70', '"INV-4180"'])
      await expect(value).toHaveTextContent(token)
    // A returned value is not an emission: no emitted-output card or title. The Run's
    // accepted source below may call coral.emit; the selected record's own pane does not.
    await expect(within(dialog).queryByRole('region', { name: 'Emitted value' })).toBeNull()
    await expect(
      within(dialog).getByRole('region', { name: 'Selected record' }),
    ).not.toHaveTextContent('coral.emit')
  },
}
/** A Run that returned nothing shows no card and no sentence about it. */
export const RunSucceededWithoutFinalValue = lean({
  ...panel(
    'Program run · succeeded, no final value',
    runPage(
      runDetail(endedRun('succeeded'), { terminal: { kind: 'succeeded', finalValue: null } }),
    ),
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    await expect(within(dialog).queryByRole('region', { name: 'Final value' })).toBeNull()
  },
})
/** Text no record panel shows: boilerplate about absent evidence, outcome prose the badge
 * already states, and technical scaffolding. */
const scaffolding = [
  'Technical details',
  'Terminal outcome',
  'Terminal evidence is unavailable',
  'returned no final value',
  'No retained outcome',
  'record #',
  'Failure details',
  'Run ID',
  'Invocation ID',
  'Whole-run controls',
  'Retained request',
]
/** A record panel that states only what helps: no scaffolding, no heading over its list, and
 * its error, when it has one, in view in the error card. */
function lean(
  story: Story,
  options: { error?: string[]; card?: string; fits?: boolean } = {},
): Story {
  return {
    ...story,
    play: async (context) => {
      await story.play?.(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      for (const text of scaffolding) await expect(dialog).not.toHaveTextContent(text)
      for (const heading of ['Context', 'Call', 'Run evidence', 'Decision'])
        await expect(opened.queryByRole('heading', { name: heading })).toBeNull()
      if (options.error) {
        const card = opened.getByRole('region', { name: 'Error' })
        await expect(card).toHaveClass(listStyles.container)
        await expect(card.closest('details')).toBeNull()
        for (const text of options.error) await expect(card).toHaveTextContent(text)
      }
      if (options.card)
        await expect(opened.getByRole('region', { name: options.card })).toBeVisible()
      if (options.fits) await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    },
  }
}
const failed = (
  code: RunFailureCode,
  error: Extract<RunDetail['terminal'], { kind: 'failed' }>['error'] = null,
) =>
  lean(
    panel(
      `Program run · failed, ${code.replaceAll('_', ' ')}`,
      runPage(
        runDetail(endedRun('failed'), {
          terminal: { kind: 'failed', code, message: failureMessages[code], error },
        }),
      ),
      { evidence: failureMessages[code] },
    ),
    // A plain heading, then the exact message; Coral's code only as a diagnostic line.
    {
      error:
        code === 'unknown'
          ? ['The run failed', failureMessages[code]]
          : [`Code: ${code}`, failureMessages[code]],
    },
  )
const failures = {
  program_rejected: failed('program_rejected', {
    ...groupedError.errors[1],
    cause: null,
  }),
  invalid_program_value: failed('invalid_program_value'),
  resource_limit_exceeded: failed('resource_limit_exceeded'),
  memory_limit_exceeded: failed('memory_limit_exceeded'),
  program_execution_failed: failed('program_execution_failed', groupedError),
  run_authority_lost: failed('run_authority_lost'),
  unknown: failed('unknown'),
} satisfies Record<RunFailureCode, Story>
export const RunFailedProgramRejected = failures.program_rejected
export const RunFailedInvalidProgramValue = failures.invalid_program_value
export const RunFailedResourceLimit = failures.resource_limit_exceeded
export const RunFailedMemoryLimit = failures.memory_limit_exceeded
export const RunFailedGroupedProgramError = failures.program_execution_failed
export const RunFailedAuthorityLost = failures.run_authority_lost
export const RunFailedUnknownCode = failures.unknown
const runStates = {
  running: RunRunning,
  succeeded: RunSucceededWithFinalValue,
  failed: RunFailedGroupedProgramError,
  canceled: lean(
    panel(
      'Program run · canceled',
      runPage(runDetail(endedRun('canceled'), { terminal: { kind: 'canceled' } })),
    ),
  ),
  timed_out: lean(
    panel(
      'Program run · timed out',
      runPage(runDetail(endedRun('timed_out'), { terminal: { kind: 'timed_out' } })),
    ),
  ),
  interrupted: lean(
    panel(
      'Program run · interrupted',
      runPage(runDetail(endedRun('interrupted'), { terminal: { kind: 'interrupted' } })),
    ),
  ),
  unknown: lean(
    panel(
      'Program run · state unknown, terminal evidence unavailable',
      runPage(runDetail(endedRun('unknown'))),
    ),
  ),
} satisfies Record<RunState, Story>
/** A long failure on a phone wraps inside its card. */
export const RunFailedNarrow = lean(
  panel(
    'Program run · failed, long error, narrow screen',
    runPage(
      runDetail(endedRun('failed'), {
        terminal: {
          kind: 'failed',
          code: 'program_execution_failed',
          message: `TypeError: Cannot read properties of undefined (reading '${'invoiceOwnerPrimaryBillingContact'.repeat(4)}')\n    at remind (program.ts:12:41)`,
          error: null,
        },
      }),
    ),
    { evidence: 'program.ts:12:41', narrow: true },
  ),
  { error: ['program_execution_failed', 'program.ts:12:41'], fits: true },
)
export const RunCanceled = runStates.canceled
export const RunTimedOut = runStates.timed_out
export const RunInterrupted = runStates.interrupted
export const RunStateUnknown = runStates.unknown

// Tool Invocations --------------------------------------------------------

export const InvocationPendingOnApproval = panel(
  'Tool call · pending, waits on an approval (review not opened)',
  invocationPage(
    invocationDetail(invocation({ requiresAction: true }), {
      requests: [{ kind: 'approval', name: approvalAction.name }],
    }),
  ),
  { evidence: ['coral.providers.mail.send', 'owner@example.com'] },
)
export const InvocationPendingOnConnection = panel(
  'Tool call · pending, waits on an account connection',
  invocationPage(
    invocationDetail(
      invocation({
        requiresAction: true,
        provider: `${workspace}/providers/linear`,
        callPath: 'coral.providers.linear.listIssues',
        tool: 'listIssues',
      }),
      {
        arguments: '{"assignee":"me","limit":50}',
        requests: [{ kind: 'account_connection', name: connectionAction.name }],
        request: connection('pending'),
      },
    ),
  ),
  { evidence: 'coral.providers.linear.listIssues' },
)
/** A tool in a running Run ends at its own outcome, with no Run controls. On a narrow
 * screen it does not overflow. */
export const InvocationInRunningRunNarrow: Story = {
  ...panel(
    'Tool call · in a running Run, narrow screen',
    invocationPage(invocationDetail(invocation(), { context: runDetail(run()) })),
    { evidence: 'coral.providers.mail.send', narrow: true },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
  },
}
export const InvocationPendingQueued = panel(
  'Tool call · pending, not waiting on you',
  invocationPage(
    invocationDetail(invocation({ waitingNanos: '4000000000', durationNanos: '4000000000' })),
  ),
  { evidence: 'coral.providers.mail.send' },
)
/**
 * A tool that got a complete response names it in plain words: "failed" only when an HTTP
 * status outside 2xx proves it, otherwise "completed". Never Coral's internal outcome name, and
 * never the provider's resource name: the call path names the provider.
 */
function answeredStory(story: Story, label: 'completed' | 'failed'): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const header = dialog.querySelector('header')!
      await expect(within(header).getByText(label)).toBeVisible()
      await expect(dialog).not.toHaveTextContent(/upstream|succeeded|Provider:|providers\//)
      await expect(within(dialog).getByRole('region', { name: 'Provider response' })).toBeVisible()
      await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    },
  }
}
const invocationStates = {
  pending: InvocationPendingOnApproval,
  dispatching: panel(
    'Tool call · dispatching',
    invocationPage(
      invocationDetail(
        invocation({
          state: 'dispatching',
          dispatchedAt: '2026-09-18T14:02:01.000Z',
          waitingNanos: '1000000000',
        }),
      ),
    ),
    { evidence: 'coral.providers.mail.send' },
  ),
  ended: answeredStory(
    panel(
      'Tool call · ended, response received',
      invocationPage(
        invocationDetail(endedInvocation('upstream_outcome'), {
          result: invocationResults.upstream_outcome,
        }),
      ),
      { evidence: 'msg_01J8' },
    ),
    'completed',
  ),
  unknown: panel(
    'Tool call · state unknown',
    invocationPage(invocationDetail(invocation({ state: 'unknown' }))),
    { evidence: 'coral.providers.mail.send' },
  ),
} satisfies Record<InvocationState, Story>
export const InvocationDispatching = invocationStates.dispatching
export const InvocationStateUnknown = invocationStates.unknown
const ended = (outcome: InvocationOutcome, name: string, evidence?: string) =>
  panel(
    name,
    invocationPage(
      invocationDetail(endedInvocation(outcome), { result: invocationResults[outcome] }),
    ),
    { evidence },
  )
const invocationOutcomes = {
  upstream_outcome: invocationStates.ended,
  coral_failure: headerSays(
    lean(
      ended(
        'coral_failure',
        'Tool call · ended, Coral failure',
        invocationResults.coral_failure.message,
      ),
      { error: [invocationResults.coral_failure.message] },
    ),
    'failed',
  ),
  run_terminated: headerSays(
    lean(ended('run_terminated', 'Tool call · ended, run terminated it before dispatch')),
    'run ended',
  ),
  indeterminate: headerSays(
    lean(ended('indeterminate', 'Tool call · ended, result indeterminate')),
    'result unknown',
  ),
  unknown: headerSays(
    lean(ended('unknown', 'Tool call · ended, outcome unknown')),
    'result unknown',
  ),
} satisfies Record<InvocationOutcome, Story>
export const InvocationUpstreamOutcome = invocationOutcomes.upstream_outcome
export const InvocationCoralFailure = invocationOutcomes.coral_failure
export const InvocationRunTerminated = invocationOutcomes.run_terminated
export const InvocationIndeterminate = invocationOutcomes.indeterminate
export const InvocationOutcomeUnknown = invocationOutcomes.unknown
const httpNotFound = invocationPage(
  invocationDetail(endedInvocation('upstream_outcome'), {
    result: {
      kind: 'upstream_outcome',
      value: '{"status":404,"data":{"message":"Recipient not found"},"metadata":{"kind":"http"}}',
    },
  }),
)
/** An HTTP status outside 2xx is the provider's own error, so the call reads as failed. */
export const InvocationUpstreamHttpError = answeredStory(
  panel('Tool call · provider answered HTTP 404', httpNotFound, {
    evidence: 'Recipient not found',
  }),
  'failed',
)
export const InvocationUpstreamHttpErrorNarrow = answeredStory(
  panel('Tool call · provider answered HTTP 404, narrow screen', httpNotFound, {
    evidence: 'Recipient not found',
    narrow: true,
  }),
  'failed',
)
/** An MCP `isError` errs only for a tool call, which the response does not record, so the call
 * reads only as completed; the result shows exactly as retained. */
export const InvocationUpstreamMcpError = answeredStory(
  panel(
    'Tool call · MCP result with isError',
    invocationPage(
      invocationDetail(
        endedInvocation('upstream_outcome', {
          provider: `${workspace}/providers/linear`,
          callPath: 'coral.providers.linear.createIssue',
          tool: 'createIssue',
        }),
        {
          arguments: '{"title":"Follow up on INV-4171","teamId":"FIN"}',
          result: {
            kind: 'upstream_outcome',
            value:
              '{"data":{"content":[{"type":"text","text":"Team FIN is archived."}],"isError":true},"metadata":{"kind":"mcp"}}',
          },
        },
      ),
    ),
    { evidence: 'Team FIN is archived.' },
  ),
  'completed',
)
export const InvocationConnectionDeclined = panel(
  'Tool call · failed after its account connection was declined',
  invocationPage(
    invocationDetail(
      endedInvocation('coral_failure', {
        dispatchedAt: null,
        provider: `${workspace}/providers/linear`,
        callPath: 'coral.providers.linear.listIssues',
        tool: 'listIssues',
      }),
      {
        arguments: '{"assignee":"me","limit":50}',
        result: {
          kind: 'coral_failure',
          code: 'account_connection_declined',
          message: 'You declined the shared account connection request.',
        },
        requests: [{ kind: 'account_connection', name: connectionAction.name }],
        request: connection('declined'),
      },
    ),
  ),
  { evidence: 'account_connection_declined' },
)
export const InvocationRequestUnavailable = panel(
  'Tool call · selected request unavailable',
  invocationPage(
    invocationDetail(invocation({ requiresAction: true }), {
      requests: [{ kind: 'approval', name: approvalAction.name }],
      requestUnavailable: true,
    }),
  ),
  { evidence: 'coral.providers.mail.send' },
)
export const InvocationUnknownRequestReference = panel(
  'Tool call · request reference unavailable',
  invocationPage(
    invocationDetail(endedInvocation('upstream_outcome'), {
      result: invocationResults.upstream_outcome,
      requests: [{ kind: 'unknown' }],
    }),
  ),
  { evidence: 'msg_01J8' },
)
export const InvocationExactLargeInteger = panel(
  'Tool call · exact integers beyond JavaScript precision',
  invocationPage(
    invocationDetail(endedInvocation('upstream_outcome'), {
      arguments: '{"invoice":9007199254740993,"ledger":-9223372036854775808}',
      result: {
        kind: 'upstream_outcome',
        value: '{"data":{"id":18446744073709551615},"metadata":{"status":200}}',
      },
    }),
  ),
  { evidence: ['9007199254740993', '-9223372036854775808', '18446744073709551615'] },
)

// Approvals -----------------------------------------------------------------

const approvalPage = (
  request: ReturnType<typeof approval>,
  context?: Parameters<typeof approvalDetail>[1],
) => invocationPage(approvalDetail(request, context))
/** An approval states its own status in its header, and no badge for its Run under it. A
 * decided one keeps who decided, when, and the comment. */
function approvalStory(story: Story, label: string, comment?: string): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const review = within(dialog).getByRole('region', { name: /^Review / })
      await expect(within(review.querySelector('header')!).getByText(label)).toBeVisible()
      await expect(dialog).not.toHaveTextContent('program run:')
      if (comment) {
        const decision = within(review).getByText('Reviewed by', { selector: 'dt' }).closest('dl')!
        await expect(decision).toHaveTextContent('Decided')
        await expect(decision).toHaveTextContent(comment)
      }
    },
  }
}
export const ApprovalPending = approvalStory(
  panel('Approval · pending, you can decide', approvalPage(approval()), {
    evidence: ['coral.providers.mail.send', 'owner@example.com'],
  }),
  'Approval required',
)
/**
 * Synthetic retained arguments for addProvider, written by hand to keep what a Program's
 * `JSON.stringify` cannot emit: an integer above 2^53 and a duplicate key. The review shows them
 * as sent, in one JSON block, and the inline document apart, collapsed, as readable text.
 */
const inlineDocument = [
  'openapi: 3.1.0',
  'info: { title: Weather (synthetic), version: "1" }',
  'paths:',
  ...Array.from(
    { length: 60 },
    (_, i) =>
      `  /forecast/${i}:\n    get: { operationId: getForecast${i}, summary: Get forecast ${i} }`,
  ),
].join('\n')
const exactAddProviderArguments =
  '{"namespace":"weather",' +
  `"source":{"kind":"openapi_document","document":${JSON.stringify(inlineDocument)}},` +
  '"credentialRoute":"fixed","maxForecasts":9007199254740993,' +
  '"label":"first as sent","label":"second as sent"}'
/** The program that makes this exact call, its arguments written out as sent. */
const exactAddProviderProgram = `return await coral.workspace.addProvider(${exactAddProviderArguments})`
/** Edge case: addProvider JSON that rows would change, a large integer and a duplicate key,
 * shows whole, exactly as sent, in one block. The ordinary arguments renderer does this for any
 * such JSON; nothing here is special to OpenAPI: no copy apart, no placeholder. */
export const ApprovalInlineOpenApiDocument: Story = {
  ...panel(
    'Approval · addProvider with an inline OpenAPI document, exact-JSON fallback',
    approvalPage(
      approval({
        callPath: 'coral.workspace.addProvider',
        provider: `${workspace}/providers/coral`,
        providerLabel: 'Coral workspace',
        arguments: exactAddProviderArguments,
      }),
      { source: exactAddProviderProgram },
    ),
    { evidence: ['9007199254740993', '"first as sent"', '"second as sent"'] },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const review = within(dialog).getByRole('region', {
      name: 'Review coral.workspace.addProvider',
    })
    await expect(review).not.toHaveTextContent('shown below')
    await expect(within(review).queryByText('OpenAPI document', { selector: 'summary' })).toBeNull()
    // Rows would change this JSON, so it shows whole, as sent, document included.
    const sent = within(review)
      .getByText(/^Arguments/, { selector: 'summary' })
      .closest('details')!
    await expect(sent).toHaveAttribute('open')
    await expect(sent.querySelectorAll('pre')).toHaveLength(1)
    await expect(sent).toHaveTextContent('getForecast59')
    await expect(within(review).queryByRole('button', { name: /^Show / })).toBeNull()
    // Nothing crosses the drawer's edge, and the decision stays in reach.
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    await expect(within(review).getByRole('button', { name: 'Approve this call' })).toBeVisible()
  },
}
/** A typical addProvider call: plain JSON that round-trips exactly. */
const openApiDocumentText = JSON.stringify({
  openapi: '3.1.0',
  info: { title: 'Weather', version: '1' },
  servers: [{ url: 'https://weather.example.com' }],
  paths: {
    '/forecast': { get: { operationId: 'getForecast', summary: 'Get the forecast' } },
  },
})
const addProviderCall = {
  namespace: 'weather',
  source: { kind: 'openapi_document', document: openApiDocumentText },
  credentialRoute: 'fixed',
  authentication: {
    method: 'oauth_authorization_code',
    authorization_url: 'https://weather.example.com/authorize',
    token_url: 'https://weather.example.com/token',
    scopes: ['read'],
    client: { client_id: 'weather-client', authentication: 'client_secret_post' },
  },
}
/** A typical program that adds the provider with this call. */
const addProviderProgram = `const call = ${JSON.stringify(addProviderCall, null, 2)}
return await coral.workspace.addProvider(call)`
/** A typical addProvider call through the ordinary arguments: one row per field, and the
 * inline document whole inside `source`, as sent. The Arguments disclosure is the one way to
 * fold it; nothing is special to OpenAPI. */
export const ApprovalAddProviderOpenApi: Story = {
  ...panel(
    'Approval · addProvider with an inline OpenAPI document',
    approvalPage(
      approval({
        callPath: 'coral.workspace.addProvider',
        provider: `${workspace}/providers/coral`,
        providerLabel: 'Coral workspace',
        arguments: JSON.stringify(addProviderCall),
      }),
      { source: addProviderProgram },
    ),
    { evidence: 'coral.workspace.addProvider' },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const review = within(dialog).getByRole('region', {
      name: 'Review coral.workspace.addProvider',
    })
    const sent = within(review)
      .getByText('Arguments (4)', { selector: 'summary' })
      .closest('details')!
    await expect(sent).toHaveAttribute('open')
    const value = (field: string) =>
      within(sent as HTMLElement).getByText(field, { selector: 'dt' }).nextElementSibling!
    await expect(
      within(sent as HTMLElement)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual(['namespace', 'source', 'credentialRoute', 'authentication'])
    await expect(value('namespace').textContent).toBe('"weather"')
    await expect(value('credentialRoute').textContent).toBe('"fixed"')
    // The whole document, as its exact JSON string, inside source.
    await expect(value('source')).toBeVisible()
    await expect(value('source').textContent).toContain('"openapi_document"')
    await expect(value('source').textContent).toContain(JSON.stringify(openApiDocumentText))
    await expect(value('authentication').textContent).toContain('"weather-client"')
    // Nothing apart from the arguments, and nothing that shortens them.
    await expect(within(review).queryByText('OpenAPI document', { selector: 'summary' })).toBeNull()
    await expect(review).not.toHaveTextContent('shown below')
    await expect(within(review).queryByRole('button', { name: /^Show / })).toBeNull()
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    for (const name of ['Deny this call', 'Approve this call'])
      await expect(within(review).getByRole('button', { name })).toBeVisible()
  },
}
/** An approved call whose Run later failed on another call: the review shows the call's own
 * result and decision, and leaves the Run's failure to the Run's panel. A visual state; the
 * absence is detail.test.tsx's to prove. */
export const ApprovalApprovedRunFailedElsewhere = panel(
  'Approval · approved, its Run failed later on another call',
  (() => {
    const page = approvalPage(approval(settle('approved')))
    const detail = page.detail as InvocationDetail
    return {
      ...page,
      detail: {
        ...detail,
        context: runDetail(endedRun('failed'), {
          terminal: {
            kind: 'failed',
            code: 'program_rejected',
            message: 'coral.providers.billing.listInvoices failed after this send.',
            error: null,
          },
        }),
      },
    }
  })(),
  { evidence: ['Checked the exact invoice recipient.', 'msg_01J8'] },
)
const approvals = {
  pending: ApprovalPending,
  approved: approvalStory(
    panel('Approval · approved, with comment', approvalPage(approval(settle('approved'))), {
      evidence: 'Checked the exact invoice recipient.',
    }),
    'Approved',
    'Checked the exact invoice recipient.',
  ),
  declined: approvalStory(
    panel('Approval · denied, with comment', approvalPage(approval(settle('declined'))), {
      evidence: 'This owner left the company.',
    }),
    'Denied',
    'This owner left the company.',
  ),
  expired: approvalStory(
    panel('Approval · expired', approvalPage(approval(settle('expired'))), {
      evidence: 'owner@example.com',
    }),
    'Expired',
  ),
  closed: approvalStory(
    panel('Approval · closed when the run ended', approvalPage(approval(settle('closed'))), {
      evidence: 'owner@example.com',
    }),
    'Closed',
  ),
} satisfies Record<ApprovalState, Story>
export const ApprovalApproved = approvals.approved
export const ApprovalDenied = approvals.declined
export const ApprovalExpired = approvals.expired
export const ApprovalClosed = approvals.closed
export const ApprovalPendingReconnecting = panel(
  'Approval · pending, reconnecting',
  approvalPage(approval()),
  { live: reconnecting, evidence: 'owner@example.com' },
)
export const ApprovalPendingNotActionable = panel(
  'Approval · pending, you cannot decide it',
  approvalPage(approval({ actionable: false })),
  { evidence: 'owner@example.com' },
)
export const ApprovalProviderUnavailable = panel(
  'Approval · pending, provider label unavailable',
  approvalPage(approval({ providerLabel: undefined })),
  { evidence: 'coral.providers.mail.send' },
)
/** Coral sent no names for the agent, its workspace or the provider. The review still says
 * an agent asked, and leaves the rest out: no placeholder, no ID. */
export const ApprovalWithoutNames: Story = {
  ...panel(
    'Approval · agent, workspace and provider names missing, narrow screen',
    (() => {
      const page = approvalPage(approval({ providerLabel: undefined }))
      const detail = page.detail as InvocationDetail
      const context = detail.context!
      return {
        ...page,
        detail: {
          ...detail,
          context: {
            ...context,
            run: { ...context.run, actorLabel: undefined, workspaceLabel: undefined },
          },
        },
      }
    })(),
    { evidence: 'coral.providers.mail.send', narrow: true },
  ),
  play: async (context) => {
    await meta.play(context)
    const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
    const requested = within(dialog).getByText('Requested by', { selector: 'dt' })
    await expect(requested.nextElementSibling!.textContent).toBe('Unknown agent')
    await expect(dialog).not.toHaveTextContent(/unavailable|Provider:|agents\//)
    for (const name of ['Deny this call', 'Approve this call'])
      await expect(within(dialog).getByRole('button', { name })).toBeVisible()
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
  },
}
export const ApprovalReviewerUnavailable = panel(
  'Approval · approved by a reviewer you cannot see, no comment',
  approvalPage(approval({ ...settle('approved'), approverLabel: undefined, comment: null })),
  { evidence: 'owner@example.com' },
)
export const ApprovalApprovedRunWaitsOnOthers = panel(
  'Approval · approved, the run still waits on other actions',
  approvalPage(approval(settle('approved')), {
    context: run({
      requiresAction: true,
      actionPage: {
        total: '2',
        next: '',
        actions: [
          {
            ...approvalAction,
            name: `${workspace}/approvals/update`,
            callPath: 'coral.providers.billing.updateInvoice',
            providerLabel: 'Billing',
          },
          connectionAction,
        ],
      },
    }),
  }),
  { evidence: 'Checked the exact invoice recipient.' },
)
const longArguments = JSON.stringify({
  to: 'owner@example.com',
  lines: Array.from({ length: 80 }, (_, i) => `Invoice INV-${4100 + i} · overdue 14 days`),
})
export const ApprovalLongArgumentsNarrow = panel(
  'Approval · pending, long arguments, narrow screen',
  approvalPage(approval({ arguments: longArguments })),
  { narrow: true, evidence: 'INV-4179' },
)
const longCallPath =
  'coral.providers.enterpriseResourcePlanning.accountsReceivable.invoices.sendOverdueReminderToPrimaryBillingContact'
export const ApprovalLongCallPathNarrow = panel(
  'Approval · pending, long call path and values, narrow screen',
  approvalPage(
    approval({
      callPath: longCallPath,
      arguments: JSON.stringify({
        contact: 'accounts-payable-escalations-for-the-northern-europe-region@example.com',
        reference: 'a'.repeat(160),
      }),
    }),
    { source: `await ${longCallPath}({ contact, reference })` },
  ),
  { narrow: true, evidence: longCallPath },
)

/** Exact logical arguments as Coral's JSON serializer can write them. */
const argumentCases = {
  'nested objects': [
    '{"invoice":{"id":"INV-4171","amount":{"currency":"EUR","value":"1250.00"}},"notify":true,"cc":null}',
    'INV-4171',
  ],
  'array at the top level': ['["owner@example.com","finance@example.com"]', 'finance@example.com'],
  'string at the top level': ['"owner@example.com"', 'owner@example.com'],
  'empty object': ['{}', 'coral.providers.mail.send'],
  'integers beyond JavaScript precision': [
    '{"invoice":9007199254740993,"amount":0.30000000000000004}',
    '9007199254740993',
  ],
  'unicode and escapes': [
    '{"subject":"Rechnung fällig · 請求書 🧾","body":"Line one\\nLine \\"two\\""}',
    'Rechnung fällig',
  ],
} as const
const argumentStory = (label: keyof typeof argumentCases) =>
  panel(
    `Approval · arguments: ${label}`,
    approvalPage(approval({ arguments: argumentCases[label][0] })),
    { evidence: argumentCases[label][1] },
  )
export const ApprovalArgumentsNested = argumentStory('nested objects')
export const ApprovalArgumentsArray = argumentStory('array at the top level')
export const ApprovalArgumentsString = argumentStory('string at the top level')
export const ApprovalArgumentsEmpty = argumentStory('empty object')
export const ApprovalArgumentsLargeIntegers = argumentStory('integers beyond JavaScript precision')
export const ApprovalArgumentsUnicode = argumentStory('unicode and escapes')

// Account connections --------------------------------------------------------

const connectionPage = (state: ConnectionState) => runPage(connectionDetail(connection(state)))
/**
 * A selected connection request leads the panel as an approval does: one header (the
 * request's, with its kind, live deadline and state, and the one close control), then the Run
 * as context, then Connect or Decline pinned to the drawer's bottom. No Run header, no Run
 * cancel, and no sentence about other calls.
 */
function connectionStory(
  story: Story,
  state: ConnectionState,
  options: { reconnecting?: boolean; fits?: boolean } = {},
): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const opened = within(dialog)
      const headers = dialog.querySelectorAll('header')
      await expect(headers).toHaveLength(1)
      const header = within(headers[0])
      await expect(header.getByRole('heading', { level: 3 }).textContent).toBe(
        state === 'pending' ? 'Connect your Linear account' : 'Connect your account',
      )
      await expect(header.getByText('account connection request')).toBeVisible()
      await expect(header.getByText(state.replaceAll('_', ' '))).toBeVisible()
      await expect(
        opened.queryAllByRole('button', { name: 'Close details' }).length,
      ).toBeLessThanOrEqual(1)
      for (const close of opened.queryAllByRole('button', { name: 'Close details' }))
        await expect(headers[0]).toContainElement(close)
      // No Run heading: the dialog's own accessible title is its only h2.
      await expect(dialog.querySelectorAll('header h2')).toHaveLength(0)
      // The header is the request's; the Run shows only as context below it.
      await expect(headers[0]).not.toHaveTextContent('program run')
      await expect(dialog).not.toHaveTextContent('One connection can unblock')
      await expect(opened.queryByRole('button', { name: 'Cancel run' })).toBeNull()
      // The Run follows as context, its times named as the Run's.
      const list = dialog.querySelector('dl')!
      await expect(
        within(list)
          .getAllByRole('term')
          .map((term) => term.textContent),
      ).toEqual(['Run intent', 'Requested by', 'Run duration', 'Run started'])
      if (state === 'pending') {
        await expect(header.getByText('Expires in 15 minutes')).toBeVisible()
        await expect(opened.getByText('Connect your Linear account to continue.')).toBeVisible()
        const decline = opened.getByRole('button', { name: 'Decline' })
        const connect = opened.getByRole('button', { name: 'Connect Linear' })
        const footer = decline.closest('footer')!
        await expect(footer).toContainElement(connect)
        await expect(footer.querySelector('time')).toBeNull()
        // The record's own footer, pinned as an approval's is: below the pane's scrolling
        // evidence beside the Run's source, or to the drawer's bottom without it.
        const pane = opened.queryByRole('region', { name: 'Selected record' })
        if (pane) {
          await expect(pane).toContainElement(footer)
          await expect(footer.previousElementSibling).toContainElement(headers[0]!)
        } else {
          await expect(getComputedStyle(footer).position).toBe('sticky')
          await expect(footer.closest('section')).toContainElement(headers[0]!)
        }
        // While the live view reconnects, neither choice is offered.
        await expect(decline).toHaveProperty('disabled', Boolean(options.reconnecting))
        await expect(connect).toHaveProperty('disabled', Boolean(options.reconnecting))
      } else {
        // The Run's source pane keeps its own status; the record states one outcome.
        const record = opened.queryByRole('region', { name: 'Selected record' }) ?? dialog
        await expect(within(record).getByRole('status')).toBeVisible()
        await expect(opened.queryByRole('button', { name: /Decline|Connect/ })).toBeNull()
      }
      if (options.fits) await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    },
  }
}
const connections = {
  pending: connectionStory(
    panel('Account connection · pending', connectionPage('pending'), {
      evidence: 'Read my open Linear issues',
    }),
    'pending',
  ),
  connected: connectionStory(
    panel('Account connection · connected', connectionPage('connected')),
    'connected',
  ),
  declined: connectionStory(
    panel('Account connection · declined', connectionPage('declined')),
    'declined',
  ),
  expired: connectionStory(
    panel('Account connection · expired', connectionPage('expired')),
    'expired',
  ),
  route_changed: connectionStory(
    panel('Account connection · account setting changed', connectionPage('route_changed')),
    'route_changed',
  ),
  authorization_changed: connectionStory(
    panel(
      'Account connection · account authorization changed',
      connectionPage('authorization_changed'),
    ),
    'authorization_changed',
  ),
} satisfies Record<ConnectionState, Story>
export const ConnectionPending = connections.pending
export const ConnectionConnected = connections.connected
export const ConnectionDeclined = connections.declined
export const ConnectionExpired = connections.expired
export const ConnectionRouteChanged = connections.route_changed
export const ConnectionAuthorizationChanged = connections.authorization_changed
export const ConnectionPendingReconnecting = connectionStory(
  panel('Account connection · pending, reconnecting', connectionPage('pending'), {
    live: reconnecting,
  }),
  'pending',
  { reconnecting: true },
)
export const ConnectionPendingNarrow = connectionStory(
  panel('Account connection · pending, narrow screen', connectionPage('pending'), {
    narrow: true,
  }),
  'pending',
  { fits: true },
)

// Run output and events --------------------------------------------------------

/** A run event's own fields, without the position every event shares. */
type EventBody = RunEvent extends infer E
  ? E extends unknown
    ? Omit<E, 'sequence' | 'occurredAt'>
    : never
  : never
const event = (sequence: number, value: EventBody) =>
  ({ ...value, sequence: String(sequence), occurredAt: at }) as RunEvent
/**
 * A log or an emission: a title naming its call and record, its time as the one fact, and
 * only its exact content in a card that stays inside the drawer, however long or narrow.
 */
function payloadCard(story: Story, card: string, title: string): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const dialog = await within(context.canvasElement.ownerDocument.body).findByRole('dialog')
      const region = within(dialog).getByRole('region', { name: card })
      await expect(region).toBeVisible()
      await expect(region.getBoundingClientRect().right).toBeLessThanOrEqual(
        dialog.getBoundingClientRect().right,
      )
      await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
      // The title names the call and the record; the facts list keeps only the time.
      await expect(within(dialog).getByRole('heading', { level: 2, name: title })).toBeVisible()
      // The icon and close control stay; no badge repeats what the title says.
      const header = dialog.querySelector('header')!
      await expect(header.querySelector('svg')).toBeVisible()
      await expect(within(header).getByRole('button', { name: 'Close details' })).toBeVisible()
      await expect(within(header).queryByText(/^(diagnostic|retained)$/)).toBeNull()
      const terms = within(dialog.querySelector('dl')!).getAllByRole('term')
      await expect(terms.map((term) => term.textContent)).toEqual(['Started'])
      // The payload shows in its viewer; the facts above carry no heading.
      await expect(
        within(region).getByRole('group', { name: /^(Structure|Lines|Raw)$/ }),
      ).toBeVisible()
      await expect(within(dialog).queryByRole('heading', { name: 'Context' })).toBeNull()
      await expect(dialog).not.toHaveTextContent('Technical details')
      await expect(dialog).not.toHaveTextContent('This log is not a user-facing result')
      // The record ends at its payload: nothing, such as a link or a separator, shows below
      // it. A detail leaving the split is not part of the record.
      const selected = within(dialog).getByRole('region', { name: 'Selected record' })
      const record = [...selected.children].find((child) => child.contains(region))!
      const end = region.getBoundingClientRect().bottom
      for (const element of record.querySelectorAll('*'))
        if (!element.contains(region) && !region.contains(element))
          await expect(element.getBoundingClientRect().bottom).toBeLessThanOrEqual(end + 0.5)
    },
  }
}
const emission = (value: string) => event(7, { kind: 'program.emitted', value })
export const EmittedObject = payloadCard(
  panel(
    'program.emitted · object, separate from the final value',
    eventPage(emission('{"invoice":"INV-4171","owner":"owner@example.com"}'), 'emission #7'),
    { evidence: 'INV-4171' },
  ),
  'Emitted value',
  'coral.emit #7',
)
export const EmittedString = payloadCard(
  panel(
    'program.emitted · string',
    eventPage(emission('"Halfway through the reminders"'), 'emission #7'),
    { evidence: 'Halfway through the reminders' },
  ),
  'Emitted value',
  'coral.emit #7',
)
export const EmittedLongNarrow = panel(
  'program.emitted · long value, narrow screen',
  eventPage(
    emission(
      JSON.stringify({
        owners: Array.from({ length: 40 }, (_, i) => ({
          owner: `owner-${i + 1}@example.com`,
          invoices: [`INV-${4100 + i}`],
        })),
      }),
    ),
    'emission #7',
  ),
  // A large value opens shallow: its list folds and states its size.
  { narrow: true, evidence: '40 items' },
)
EmittedLongNarrow.play = payloadCard(EmittedLongNarrow, 'Emitted value', 'coral.emit #7').play
const logged = (level: LogLevel, message: string) =>
  payloadCard(
    panel(
      `program.logged · ${level}`,
      eventPage(event(8, { kind: 'program.logged', level, message }), 'log #8'),
      { evidence: message },
    ),
    'Log message',
    `console.${level} #8`,
  )
const logLevels = {
  debug: logged('debug', 'Fetched page 2 of overdue invoices.'),
  info: logged('info', 'Sending 24 reminders.'),
  warn: logged('warn', 'Owner of INV-4180 has no email address. Skipping.'),
  error: logged('error', 'Mail rejected owner@example.com: mailbox full.'),
  unknown: logged('unknown', 'Retained at a level this version of Reef does not know.'),
} satisfies Record<LogLevel, Story>
export const LoggedDebug = logLevels.debug
export const LoggedInfo = logLevels.info
export const LoggedWarn = logLevels.warn
export const LoggedError = logLevels.error
export const LoggedUnknownLevel = logLevels.unknown
export const LoggedLongNarrow = panel(
  'program.logged · long message, narrow screen',
  eventPage(
    event(8, {
      kind: 'program.logged',
      level: 'warn',
      message: `Skipped owners without an address: ${Array.from({ length: 30 }, (_, i) => `owner-${i + 1}`).join(', ')}`,
    }),
    'log #8',
  ),
  { narrow: true, evidence: 'owner-30' },
)
LoggedLongNarrow.play = payloadCard(LoggedLongNarrow, 'Log message', 'console.warn #8').play
/** How a tool call's end reads in its run event title. */
const endedWords = {
  upstream_outcome: 'completed',
  coral_failure: 'failed',
  run_terminated: 'run ended',
  indeterminate: 'result unknown',
  unknown: 'result unknown',
} satisfies Record<InvocationOutcome, string>
const invocationEnded = (outcome: InvocationOutcome) =>
  headerSays(
    panel(
      `invocation.ended · ${outcome.replaceAll('_', ' ')}`,
      eventPage(
        event(5, { kind: 'invocation.ended', invocation: invocation().name, outcome }),
        'run event',
      ),
      { evidence: `Tool call ended: ${endedWords[outcome]}` },
    ),
    null,
  )
const endedCalls = {
  upstream_outcome: invocationEnded('upstream_outcome'),
  coral_failure: invocationEnded('coral_failure'),
  run_terminated: invocationEnded('run_terminated'),
  indeterminate: invocationEnded('indeterminate'),
  unknown: invocationEnded('unknown'),
} satisfies Record<InvocationOutcome, Story>
export const CallEndedUpstreamOutcome = endedCalls.upstream_outcome
export const CallEndedCoralFailure = endedCalls.coral_failure
export const CallEndedRunTerminated = endedCalls.run_terminated
export const CallEndedIndeterminate = endedCalls.indeterminate
export const CallEndedOutcomeUnknown = endedCalls.unknown
const cancellation = (actorKind: CancellationActor, actor: string) =>
  panel(
    `run.cancellation_requested · by ${{ user: 'a user', agent: 'an agent', unknown: 'an unknown principal' }[actorKind]}`,
    eventPage(
      event(9, { kind: 'run.cancellation_requested', actor, actorKind }),
      'cancellation requested',
    ),
  )
const cancellations = {
  user: cancellation('user', 'users/owner'),
  agent: cancellation('agent', `${workspace}/agents/collections`),
  unknown: cancellation('unknown', ''),
} satisfies Record<CancellationActor, Story>
export const CancellationByUser = cancellations.user
export const CancellationByAgent = cancellations.agent
export const CancellationByUnknown = cancellations.unknown
const runEnded = (state: Exclude<RunState, 'running'>) =>
  panel(
    `run.ended · ${state.replaceAll('_', ' ')}`,
    eventPage(event(10, { kind: 'run.ended', state }), 'execution ended'),
  )
const endings = {
  succeeded: runEnded('succeeded'),
  failed: runEnded('failed'),
  canceled: runEnded('canceled'),
  timed_out: runEnded('timed_out'),
  interrupted: runEnded('interrupted'),
  unknown: runEnded('unknown'),
} satisfies Record<Exclude<RunState, 'running'>, Story>
export const RunEndedSucceeded = endings.succeeded
export const RunEndedFailed = endings.failed
export const RunEndedCanceled = endings.canceled
export const RunEndedTimedOut = endings.timed_out
export const RunEndedInterrupted = endings.interrupted
export const RunEndedUnknown = endings.unknown
const events = {
  'run.started': headerSays(
    lean(panel('run.started', eventPage(event(1, { kind: 'run.started' }), 'execution accepted'))),
    null,
  ),
  'invocation.started': lean(
    panel(
      'invocation.started',
      eventPage(
        event(4, { kind: 'invocation.started', invocation: invocation().name }),
        'run event',
      ),
    ),
  ),
  'invocation.ended': CallEndedUpstreamOutcome,
  'program.emitted': EmittedObject,
  'program.logged': LoggedInfo,
  'run.cancellation_requested': CancellationByUser,
  'run.ended': RunEndedFailed,
  unknown: lean(
    panel(
      'Run event · kind unknown to this version',
      eventPage(event(11, { kind: 'unknown' }), 'run event'),
    ),
  ),
} satisfies Record<RunEvent['kind'], Story>
export const RunStarted = events['run.started']
export const CallStarted = events['invocation.started']
export const UnknownEvent = events.unknown

// Unavailable detail ------------------------------------------------------------

export const DetailUnavailable = panel(
  'Unavailable · record removed or concealed',
  panelPage(invocation(), null, { unavailable: true, selectedRow: null }),
)

// Large payloads ------------------------------------------------------------
const largeResponse = JSON.stringify({
  status: 200,
  data: Array.from({ length: 150 }, (_, index) => ({
    id: index,
    title: `Release ${index}`,
    body: `Notes for release ${index}. `.repeat(12),
  })),
  metadata: { kind: 'http' },
  // An integer no JavaScript number holds: it must show exactly as retained.
}).replace(/"id":\d+,/, '"id":9007199254740993123,')
const largeResponsePage = invocationPage(
  invocationDetail(endedInvocation('upstream_outcome'), {
    // Enough arguments that the detail pane scrolls around the response too.
    arguments: JSON.stringify(
      Object.fromEntries(Array.from({ length: 30 }, (_, index) => [`field${index}`, index])),
    ),
    result: { kind: 'upstream_outcome', value: largeResponse },
  }),
)
/** The scrolling element around `element`: the pane that owns its vertical scroll. */
function scroller(element: HTMLElement) {
  for (let node = element.parentElement; node; node = node.parentElement)
    if (
      /(auto|scroll)/.test(getComputedStyle(node).overflowY) &&
      node.scrollHeight > node.clientHeight
    )
      return node
  return null
}
/**
 * A large response in the detail pane: it scrolls inside its card, not the pane, and Expand
 * opens it from the keyboard. Closing the viewer returns focus, the record and both scroll
 * positions as they were. Wax's PayloadViewer stories own what the viewer shows and finds.
 */
function largePayloadStory(story: Story): Story {
  return {
    ...story,
    play: async (context) => {
      await meta.play(context)
      const { userEvent } = context
      const body = within(context.canvasElement.ownerDocument.body)
      const opened = await body.findByRole('dialog')
      const response = within(opened).getByRole('region', { name: 'Provider response' })
      const tree = within(response).getByRole('group', { name: 'Structure' })
      // Opened, its list makes the card scroll.
      await userEvent.click(within(tree).getByRole('button', { name: 'data: 150 items' }))
      const content = within(response).getByRole('group', { name: 'Payload content' })
      const actions = within(response).getByRole('group', { name: 'Payload actions' })
      await expect(content.scrollHeight).toBeGreaterThan(content.clientHeight)
      const pane = scroller(response)!
      await expect(pane).not.toBeNull()
      await expect(response.contains(pane)).toBe(false)
      const paneBefore = pane.scrollTop
      content.scrollTop = 200
      await waitFor(() => expect(content.scrollTop).toBe(200))
      await expect(pane.scrollTop).toBe(paneBefore)
      await expect(opened.scrollWidth).toBeLessThanOrEqual(opened.clientWidth)
      const heading = within(opened).getAllByRole('heading', { level: 2 })[0]!.textContent
      // Scroll the pane, as you would to reach Expand under content above it.
      const expand = within(actions).getByRole('button', { name: 'Expand Provider response' })
      expand.scrollIntoView({ block: 'center' })
      const position = pane.scrollTop
      await expect(position).toBeGreaterThan(0)
      // Keyboard: Enter on the focused Expand opens the viewer.
      expand.focus()
      await userEvent.keyboard('{Enter}')
      const viewer = await body.findByRole('dialog', { name: 'Provider response' })
      await waitFor(() =>
        expect(within(viewer).getByRole('group', { name: 'Structure' })).toBeVisible(),
      )
      await userEvent.keyboard('{Escape}')
      await waitFor(() =>
        expect(body.queryByRole('dialog', { name: 'Provider response' })).toBeNull(),
      )
      await waitFor(() => expect(expand).toHaveFocus())
      await expect(pane.scrollTop).toBe(position)
      await expect(content.scrollTop).toBe(200)
      await expect(within(opened).getAllByRole('heading', { level: 2 })[0]!).toHaveTextContent(
        heading!,
      )
    },
  }
}
export const InvocationLargeResponse = largePayloadStory(
  panel('Operation call · large response', largeResponsePage, { evidence: '150 items' }),
)
export const InvocationLargeResponseNarrow = largePayloadStory(
  panel('Operation call · large response, narrow screen', largeResponsePage, {
    evidence: '150 items',
    narrow: true,
  }),
)
