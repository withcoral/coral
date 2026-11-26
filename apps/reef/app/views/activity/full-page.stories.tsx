import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, Outlet, useLoaderData, useLocation, useNavigate } from 'react-router'
import { expect, spyOn, waitFor, within } from 'storybook/test'
import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { NavigationRoot } from '@/wax/components/navigation-frame/navigation-frame'
import { BreadcrumbRoot } from '@/wax/components/breadcrumbs/breadcrumbs'
import * as shellStyles from '@/routes/app-shell.css'
import { LiveActivity } from '@/routes/activity-live'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { coral } from '@/lib/activity-trace.test-support'
import { inTestBrowser, resizeViewport } from '../../../.storybook/browser'
import { workspace } from './fixtures'
import { instant, traceCall, traceEvent, traceRun, traceTask } from './trace-fixtures'
import { createContext, useContext, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import * as storyStyles from './comparison.stories.css'
import * as activityStyles from './task-activity.css'
import { traceStore } from './trace-fixtures'
import { openRoot, waterfall } from './trace'
import { Waterfall as HistoricalWaterfall } from './historical-waterfall'
import { Waterfall as OneLineWaterfall } from './one-line-waterfall'
import { programAt } from './side-panel-fixtures'
import type { ActivityPage, ActivityRow, RunDetail } from './types'

// Fictional data. Add a Task and its Runs here; each parent key owns its child window.
const completed = traceTask({
  intent: 'Summarize the open support queue',
  endedAt: instant(12_000),
})
const ongoing = traceTask({
  name: `${workspace}/tasks/research`,
  intent: 'Investigate recurring support themes',
  outcome: 'open',
  startedAt: instant(20_000),
  endedAt: null,
})
const source = [
  'const issues = await coral.private.collectIssues({ limit: 20 })',
  "console.log('Collected issues', issues.length)",
  'coral.emit({ count: issues.length })',
  'return { count: issues.length }',
].join('\n')
const children: Record<string, ActivityRow[]> = {}
const runs = (task: typeof completed, count: number, start: number, running = false) =>
  Array.from({ length: count }, (_, i) => {
    const from = start + i * 4_000
    const run = traceRun(
      `${task.name.split('/').at(-1)}-${i + 1}`,
      from,
      running && i === count - 1 ? null : from + 2_700,
      {
        task: task.name,
        actor: `Inspect support batch ${i + 1}`,
        intent: `Inspect support batch ${i + 1}`,
        contextImpactBytes: String(-4200 - i * 240),
      },
    )
    const call = traceCall(
      run,
      `${i}-${task.name.split('/').at(-1)}-collect`,
      [from + 100, from + 120, from + 1_700],
      {
        toolKind: 'private',
        provider: '',
        tool: 'collectIssues',
        callPath: 'coral.private.collectIssues',
        sourceLocation: programAt('coral.private.collectIssues', source),
      },
    )
    const nested = traceCall(
      run,
      `${i}-${task.name.split('/').at(-1)}-list`,
      [from + 300, from + 320, from + 1_400],
      {
        callPath: 'coral.providers.linear.listIssues',
        parent: { invocation: call.name, callPath: call.callPath, toolKind: 'private' },
      },
    )
    children[run.name] = [
      call,
      { ...traceEvent(run, 1, from + 1_800), sourceLocation: programAt('console.log', source) },
      {
        ...traceEvent(run, 2, from + 2_000, 'program.emitted'),
        sourceLocation: programAt('coral.emit', source),
      },
    ]
    children[call.name] = [nested]
    return run
  })
children[completed.name] = runs(completed, 2, 1_000)
children[ongoing.name] = runs(ongoing, 5, 21_000, true)
const roots = [ongoing, completed]
// Move the entire recorded chronology together: the ongoing Run starts about a minute
// before this fixture loads, while every child retains its own offset and ordering.
const shift = Date.now() - Date.parse(instant(97_000))
const recent = (at: string) => new Date(Date.parse(at) + shift).toISOString()
for (const row of [...roots, ...Object.values(children).flat()]) {
  row.startedAt = recent(row.startedAt)
  if ('endedAt' in row && row.endedAt) row.endedAt = recent(row.endedAt)
  if (row.kind === 'run' && row.state === 'running') row.durationNanos = '60000000000'
  if (row.kind === 'invocation' && row.dispatchedAt) row.dispatchedAt = recent(row.dispatchedAt)
  if (row.kind === 'tool' && row.completedAt) row.completedAt = recent(row.completedAt)
}
const fake = coral(children, roots)
const allRows = [...roots, ...Object.values(children).flat()]
const runContext = (run: Extract<ActivityRow, { kind: 'run' }>): RunDetail => ({
  kind: 'run',
  run,
  source,
  linkedPrivateTools: [],
  linkedSharedTools: [],
  terminal:
    run.state === 'running'
      ? { kind: 'unavailable' }
      : { kind: 'succeeded', finalValue: '{"count":20}' },
})
/** The same loader serves route navigation and the trace reader's fetcher. */
function load(request: Request): ActivityPage {
  const query = new URL(request.url).searchParams
  const page = fake.load(query)
  const row = allRows.find((item) => item.name === query.get('record'))
  if (!row) return page
  const parent =
    row.kind === 'run'
      ? row
      : 'run' in row
        ? allRows.find((item) => item.kind === 'run' && item.name === row.run)
        : undefined
  const context = parent?.kind === 'run' ? runContext(parent) : undefined
  let detail: ActivityPage['detail'] = null
  if (row.kind === 'task') detail = { kind: 'task', task: row }
  if (row.kind === 'run') detail = runContext(row)
  if (row.kind === 'invocation')
    detail = {
      kind: 'invocation',
      invocation: row,
      context,
      arguments: '{"limit":20}',
      result: { kind: 'upstream_outcome', value: '{"count":20}' },
      requests: [],
      request: null,
      requestUnavailable: false,
    }
  if (row.kind === 'output')
    detail = {
      kind: 'output',
      run: row.run,
      context,
      event:
        row.eventKind === 'program.emitted'
          ? {
              kind: 'program.emitted',
              sequence: '2',
              occurredAt: row.startedAt,
              value: '{"count":20}',
            }
          : {
              kind: 'program.logged',
              sequence: '1',
              occurredAt: row.startedAt,
              level: 'info',
              message: 'Collected issues 20',
            },
    }
  return { ...page, selected: row.name, selectedRow: row, detail }
}
const fixtureWorkspace = {
  path: '/workspaces/finance',
  displayName: 'Finance',
  effectiveRole: 'owner',
  state: 'active',
  mcpEndpoint: 'https://coral.example.com/mcp',
  connectEndpoint: 'https://coral.example.com/mcp',
  etag: 'fixture',
} as const
// Storybook cannot import the route modules' server-only dependencies. Reuse the shell's
// actual layout and navigation components around the real Activity route content instead.
function Shell() {
  return (
    <NavigationRoot>
      <div className={shellStyles.layout}>
        <Sidebar
          initialIsMinimized={false}
          workspace={fixtureWorkspace}
          workspaces={[fixtureWorkspace]}
        />
        <ContentContainer>
          <BreadcrumbRoot
            value={{
              id: 'workspace',
              type: 'link',
              text: fixtureWorkspace.displayName,
              to: fixtureWorkspace.path,
            }}
          >
            <Outlet />
          </BreadcrumbRoot>
        </ContentContainer>
      </div>
    </NavigationRoot>
  )
}
function Route() {
  const page = useLoaderData<ActivityPage>()
  return (
    <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
      <LiveActivity initial={page} />
    </PageLayout>
  )
}
const Stub = createRoutesStub([
  {
    path: '/',
    Component: Shell,
    children: [
      {
        path: 'workspaces/finance/activity',
        loader: ({ request }) => ({
          ...load(request),
          workspace: fixtureWorkspace,
          providerMarks: [],
        }),
        Component: Route,
      },
    ],
  },
])
// Story-only substitution: Activity keeps its real root list, router and detail rail. The
// historical snapshot is mounted in the trace pane with fully retained fixture windows.
const LayoutContext = createContext<'current' | 'previous' | 'one-line'>('current')
function ComparisonView() {
  const layout = useContext(LayoutContext)
  const page = useLoaderData<ActivityPage>()
  const location = useLocation()
  const navigate = useNavigate()
  const container = useRef<HTMLDivElement>(null)
  const [target, setTarget] = useState<HTMLElement | null>(null)
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const tableRef = useRef<HTMLDivElement>(null)
  const root = roots.find((item) => item.name === openRoot(page)) ?? ongoing
  const store = useMemo(() => traceStore(root, children), [root])
  const rows = useMemo(() => waterfall(store, collapsed, 0).rows, [store, collapsed])
  useLayoutEffect(() => {
    setTarget(container.current?.querySelector<HTMLElement>(`.${activityStyles.tracePane}`) ?? null)
  }, [])
  const toggle = (name: string) =>
    setCollapsed((old) => {
      const next = new Set(old)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  const href = (row: ActivityRow) => {
    const query = new URLSearchParams(location.search)
    query.set('record', row.name)
    return `${location.pathname}?${query}`
  }
  const Component = layout === 'one-line' ? OneLineWaterfall : HistoricalWaterfall
  return (
    <PageLayout
      variant="split"
      breadcrumbs={trail([], 'Activity')}
      header={
        layout !== 'current' && (
          <p>
            Story-only historical waterfall:{' '}
            {layout === 'one-line'
              ? '78baa173 (one-line; original overview alignment)'
              : 'bb9d070 (two-line)'}
            . Synthetic retained data; historical keyboard navigation and live trace loading are not
            reproduced.
          </p>
        )
      }
    >
      <div
        className={layout === 'current' ? undefined : storyStyles.historical}
        ref={container}
        style={{ height: '100%' }}
      >
        <LiveActivity initial={page} />
        {layout !== 'current' &&
          target &&
          createPortal(
            <div className={storyStyles.snapshot}>
              <Component
                store={store}
                rows={rows}
                selected={page.selected}
                holder={page.selectedRow?.kind === 'output' ? page.selectedRow.run : null}
                active={null}
                open={{ href, select: (row) => void navigate(href(row)) }}
                toggle={toggle}
                tableRef={tableRef}
                listHref={page.listPath}
                header={<h2>{root.intent}</h2>}
              />
            </div>,
            target,
          )}
      </div>
    </PageLayout>
  )
}
const ComparisonRoutes = createRoutesStub([
  {
    path: '/',
    Component: Shell,
    children: [
      {
        path: 'workspaces/finance/activity',
        loader: ({ request }) => load(request),
        Component: ComparisonView,
      },
    ],
  },
])
function Comparison({ layout }: { layout: 'current' | 'previous' | 'one-line' }) {
  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <LayoutContext value={layout}>
        <ComparisonRoutes
          initialEntries={[
            `/workspaces/finance/activity?tree=1&record=${encodeURIComponent(ongoing.name)}&expand=${encodeURIComponent(ongoing.name)}`,
          ]}
        />
      </LayoutContext>
    </div>
  )
}
const meta = {
  title: 'Activity/Full page',
  component: Comparison,
  args: { layout: 'current' },
  argTypes: {
    layout: {
      control: 'radio',
      options: ['current', 'previous', 'one-line'],
      description: 'Current dense, historical two-line, or historical one-line waterfall.',
    },
  },
  parameters: { layout: 'fullscreen' },
  beforeEach: () => {
    // No backend: keep observation open; route and fetcher reads use the stub loader.
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async (_input, init) => {
      const stream = new ReadableStream({
        start(controller) {
          const heartbeat = () =>
            controller.enqueue(new TextEncoder().encode('data: {"kind":"heartbeat"}\n\n'))
          heartbeat()
          const timer = setInterval(heartbeat, 5000)
          init?.signal?.addEventListener(
            'abort',
            () => {
              clearInterval(timer)
              controller.close()
            },
            { once: true },
          )
        },
      })
      return new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } })
    })
    return () => fetch.mockRestore()
  },
} satisfies Meta<typeof Comparison>
export default meta
type Story = StoryObj<typeof meta>
/** The current page and dense waterfall, with the same fixture as the historical views. */
export const CurrentDense: Story = { args: { layout: 'current' } }
/** The bb9d070 two-line waterfall in the current page's story-only seam. */
export const PreviousTwoLine: Story = { args: { layout: 'previous' } }
/** The earlier 78baa173 one-line table and its original overview alignment. */
export const PreviousOneLine: Story = { args: { layout: 'one-line' } }
/** Browser checks live separately so the named preview opens untouched. */
export const PreviousTableChecks: Story = {
  args: { layout: 'previous' },
  play: async ({ canvasElement, userEvent }) => {
    if (!(await inTestBrowser())) return
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    const tasks = await canvas.findByRole('table', { name: 'Tasks and runs' })
    const table = await canvas.findByRole('table', { name: 'Activity records' })
    await userEvent.click(
      within(tasks).getByRole('link', { name: 'Summarize the open support queue' }),
    )
    await waitFor(() =>
      expect(
        within(table).queryByRole('link', { name: 'Inspect support batch 5' }),
      ).not.toBeInTheDocument(),
    )
    await userEvent.click(
      within(tasks).getByRole('link', { name: 'Investigate recurring support themes' }),
    )
    await within(table).findByRole('link', { name: 'Inspect support batch 5' })
    for (const label of ['RECORD', 'DUR', 'CONTEXT'])
      await expect(within(table).getByText(label)).toBeVisible()
    const run = within(table).getByRole('link', { name: 'Inspect support batch 1' })
    await expect(run).toBeVisible()
    const row = run.closest('[role="row"]')!
    const bar = row.querySelector('[data-timeline-bar]')!
    await expect(bar.getBoundingClientRect().top).toBeGreaterThan(
      run.getBoundingClientRect().bottom,
    )
    await expect(within(row as HTMLElement).getByText('2.7s')).toBeVisible()
    await userEvent.click(run)
    await expect(await within(canvasElement.ownerDocument.body).findByRole('dialog')).toBeVisible()
  },
}
export const OneLineTableChecks: Story = {
  args: { layout: 'one-line' },
  play: async ({ canvasElement, userEvent }) => {
    if (!(await inTestBrowser())) return
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    const tasks = await canvas.findByRole('table', { name: 'Tasks and runs' })
    const table = await canvas.findByRole('table', { name: 'Activity records' })
    await userEvent.click(
      within(tasks).getByRole('link', { name: 'Summarize the open support queue' }),
    )
    await waitFor(() =>
      expect(
        within(table).queryByRole('link', { name: 'Inspect support batch 5' }),
      ).not.toBeInTheDocument(),
    )
    await userEvent.click(
      within(tasks).getByRole('link', { name: 'Investigate recurring support themes' }),
    )
    await within(table).findByRole('link', { name: 'Inspect support batch 5' })
    for (const label of ['RECORD', 'DUR', 'CONTEXT'])
      await expect(within(table).getByText(label)).toBeVisible()
    const run = within(table).getByRole('link', { name: 'Inspect support batch 1' })
    const row = run.closest('[role="row"]')!
    const bar = row.querySelector('[data-timeline-bar]')!
    await expect(bar.getBoundingClientRect().top).toBeLessThan(run.getBoundingClientRect().bottom)
    await expect(within(row as HTMLElement).getByText('2.7s')).toBeVisible()
    await userEvent.click(run)
    await expect(await within(canvasElement.ownerDocument.body).findByRole('dialog')).toBeVisible()
  },
}
export const Representative: Story = {
  parameters: { controls: { exclude: ['layout'] } },
  render: () => (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <Stub initialEntries={['/workspaces/finance/activity']} />
    </div>
  ),
}
/** Browser-only interaction coverage; the preview story above never drives itself. */
export const InteractiveChecks: Story = {
  ...Representative,
  play: async ({ canvasElement, userEvent }) => {
    if (!(await inTestBrowser())) return
    await resizeViewport(1440, 900)
    const canvas = within(canvasElement)
    await expect(await canvas.findByRole('link', { name: 'Tools' })).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'Activity' })).toHaveAttribute(
      'href',
      '/workspaces/finance/activity',
    )
    await expect(
      await canvas.findByRole('link', { name: 'Investigate recurring support themes' }),
    ).toBeVisible()
    await expect(
      canvas.getByRole('link', { name: 'Summarize the open support queue' }),
    ).toBeVisible()
    await userEvent.click(canvas.getByRole('link', { name: 'Summarize the open support queue' }))
    await waitFor(() =>
      expect(fake.calls.some((query) => query.getAll('expand').includes(completed.name))).toBe(
        true,
      ),
    )
    await expect(await canvas.findByRole('table', { name: 'Activity records' })).toBeVisible()
    await expect(await canvas.findByText('Inspect support batch 1')).toBeVisible()
    await waitFor(() => expect(fake.calls.some((query) => query.get('load') === 'page')).toBe(true))
    await userEvent.click(canvas.getByText('Inspect support batch 1'))
    const runRow = canvasElement
      .querySelector('[data-record-link][href*="invoices-1"]')!
      .closest('[role="row"]')!
    await waitFor(() => expect(runRow).toHaveAttribute('aria-selected', 'true'))
    for (const shape of ['circle', 'diamond']) {
      await waitFor(() => {
        const mark = runRow.querySelector<HTMLElement>(`[data-timeline-mark="${shape}"]`)!
        expect(mark).not.toBeNull()
        const box = mark.getBoundingClientRect()
        expect(document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2)).toBe(mark)
      })
    }
    const log = within(runRow as HTMLElement).getByRole('link', { name: 'log #1' })
    await userEvent.click(log)
    await expect(
      await within(canvasElement.ownerDocument.body).findByText('Collected issues 20'),
    ).toBeVisible()
    await userEvent.click(within(runRow as HTMLElement).getByText('Inspect support batch 1'))
    const runLink = within(runRow as HTMLElement).getByText('Inspect support batch 1')
    runLink.focus()
    await userEvent.keyboard('{ArrowRight}')
    await waitFor(() =>
      expect(within(runRow as HTMLElement).getByRole('link', { name: 'log #1' })).toHaveAttribute(
        'aria-current',
        'true',
      ),
    )
  },
}
