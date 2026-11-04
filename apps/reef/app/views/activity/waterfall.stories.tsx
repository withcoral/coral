import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, waitFor, within } from 'storybook/test'
import gmail from '../../../recipes/gmail/logo.svg'
import drive from '../../../recipes/google_drive_mcp/logo.svg'
import linearDark from '../../../recipes/linear/logo-dark.svg'
import linear from '../../../recipes/linear/logo.svg'
import stripe from '../../../recipes/stripe_mcp/logo.svg'
import { resizeViewport } from '../../../.storybook/browser'
import { ProviderMarksContext, type ProviderMark } from './call-tool'
import { freezeStoryClock, storyNow } from './story-clock'
import type { ActivityTrace } from './types'
import { ActivityWaterfall } from './waterfall'
import {
  exploreTrace,
  failedTrace,
  longTrace,
  manyTrace,
  runningTrace,
  shortTrace,
  standaloneTrace,
} from './waterfall-fixtures'

/** The fixture workspace's providers, so calls name their tool by logo. */
const providers: ProviderMark[] = [
  { callRoot: 'coral.providers.billing', displayName: 'Billing', icons: { light: stripe } },
  { callRoot: 'coral.providers.mail', displayName: 'Mail', icons: { light: gmail } },
  {
    callRoot: 'coral.providers.linear',
    displayName: 'Linear',
    icons: { light: linear, dark: linearDark },
  },
  { callRoot: 'coral.providers.drive', displayName: 'Drive', icons: { light: drive } },
  { callRoot: 'coral.providers.coral', displayName: 'Coral' },
  { callRoot: 'coral.providers.clickstack', displayName: 'ClickStack' },
]

/** Stories render inside a router, with the fixture workspace's providers. */
function Waterfall({ trace, selected = null }: { trace: ActivityTrace; selected?: string | null }) {
  return (
    <ProviderMarksContext value={providers}>
      <div style={{ height: '100vh', display: 'flex' }}>
        <ActivityWaterfall
          trace={trace}
          selected={selected}
          href={(record) => `/activity?record=${encodeURIComponent(record)}`}
          loadedAt={storyNow}
        />
      </div>
    </ProviderMarksContext>
  )
}

const meta = {
  title: 'Activity/Waterfall',
  component: Waterfall,
  beforeEach: freezeStoryClock,
  parameters: { layout: 'fullscreen', chromatic: { viewports: [1440] } },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub initialEntries={['/activity']} />
    },
  ],
} satisfies Meta<typeof Waterfall>
export default meta
type Story = StoryObj<typeof meta>

/** Minutes of work with idle gaps, a rejected program, two waits, a failure and running work. */
export const LongTask: Story = {
  args: { trace: longTrace() },
  // Catches: logs and output, dots on a bar, counted as records the rows do not show.
  play: async ({ canvasElement }) => {
    await expect(canvasElement.querySelector('header')).toHaveTextContent(
      / · 20 records · 2 logs · 1 program output · 2 failed calls$/,
    )
  },
}

/** Seconds of work, with calls in parallel. */
export const ShortTask: Story = {
  args: { trace: shortTrace() },
  // Catches: a count of logs or output a root without them does not need.
  play: async ({ canvasElement }) => {
    await expect(canvasElement.querySelector('header')).toHaveTextContent(
      / · 8 records · 0 failed calls$/,
    )
  },
}

/** Sixty calls in turn, cut at the read limit. */
export const ManyCalls: Story = {
  args: { trace: manyTrace() },
  play: async ({ canvasElement }) => {
    await expect(
      within(canvasElement).getByText('This timeline shows only the first records.'),
    ).toBeVisible()
  },
}

/** A Run with no Task is its own root. */
export const StandaloneRun: Story = {
  args: { trace: standaloneTrace() },
  // Catches: a header that does not say who did the work.
  play: async ({ canvasElement }) => {
    await expect(canvasElement.querySelector('header')).toHaveTextContent(/UTC · Collections agent/)
  },
}

/** A Run that still runs: its bars grow, and a call waits on approval. */
export const RunningRun: Story = {
  args: { trace: runningTrace() },
  // Catches: running work drawn as ended, a waiting call without its wait, or a lifecycle
  // Event missing from its Run.
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByRole('tab', { name: /^Time/ })).toHaveTextContent(/ so far/)
    // A call still waiting says so, and what it waits for, with its row.
    await expect(canvas.getByText(/Waited .* so far\./)).toBeInTheDocument()
    await expect(canvas.getByRole('button', { name: /^execution accepted at / })).toBeVisible()
  },
}

/** A double-click on a bar fits the axis to it; its exact timing shows on hover. */
export const DoubleClickFits: Story = {
  args: { trace: longTrace() },
  // Catches: no quick way to fit one record, or a bar whose exact times are out of reach.
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const row = within(canvas.getByRole('table', { name: 'Timeline records' }))
      .getByText('Send invoice reminders')
      .closest('[role="row"]') as HTMLElement
    const bar = within(row).getByText(/^Started /).parentElement!
    // Hover shows the bar's breakdown as a card.
    await userEvent.hover(bar)
    const page = within(canvasElement.ownerDocument.body)
    // The card fades in.
    await waitFor(() => expect(page.getByText('14:04:12.300 UTC')).toBeVisible())
    await userEvent.dblClick(bar)
    await waitFor(() =>
      expect(canvas.getByText(/^Showing /)).toHaveTextContent('Showing 20.6s to 4:50'),
    )
  },
}

/** Collapsing a Run hides its calls; zoom to selection frames the selected record. */
export const CollapseAndZoom: Story = {
  args: {
    trace: longTrace(),
    selected: 'workspaces/finance/runs/send-reminders',
  },
  play: async ({ canvasElement }) => {
    const page = within(canvasElement)
    const table = page.getByRole('table', { name: 'Timeline records' })
    const calls = () => within(table).queryAllByTitle('coral.providers.mail.send')
    await expect(calls()).toHaveLength(4)
    const row = within(table)
      .getByText('Send invoice reminders')
      .closest('[role="row"]') as HTMLElement
    await userEvent.click(within(row).getByRole('button', { name: 'Collapse record' }))
    await expect(calls()).toHaveLength(0)
    await userEvent.click(page.getByRole('button', { name: 'Zoom to selection' }))
    await waitFor(() =>
      expect(page.getByText(/^Showing /)).toHaveTextContent('Showing 20.6s to 4:50'),
    )
    await userEvent.click(page.getByRole('button', { name: 'Fit' }))
    await expect(page.getByText(/^Showing /)).toHaveTextContent('Showing 0s to 6:02')
  },
}

/** A Task that failed: its error leads, and the agent's time between calls is a row. */
export const FailedTask: Story = {
  args: { trace: failedTrace() },
  // Catches: a failure you must click through to read, waits drawn in a failure's red, a
  // program row that hides its failed call, or agent time hidden in the root's bar.
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const banner = canvas.getByRole('alert')
    await expect(within(banner).getByText('addProvider failed in Coral')).toBeVisible()
    await expect(
      within(banner).getByText('upstream_rejected: The MCP server refused the bearer token.'),
    ).toBeVisible()
    await expect(
      within(banner).getByText('The earlier call of this tool failed with the same error.'),
    ).toBeVisible()
    await expect(within(banner).getByText('It waited for approval before it ran.')).toBeVisible()
    await expect(
      within(banner).getByText(/^The agent then worked 37\.8s without another call\.$/),
    ).toBeVisible()
    // The agent's summary leads the header, once, not inside the failure.
    const summary = canvas.getByRole('region', { name: 'Agent summary' })
    await expect(within(summary).getByText(/^The ClickStack MCP server rejects/)).toBeVisible()
    await expect(within(banner).queryByText(/rejects the bearer token/)).toBeNull()
    await expect(within(banner).getByRole('link', { name: 'Show call' })).toHaveAttribute(
      'href',
      expect.stringContaining(encodeURIComponent('invocations/add-2')),
    )
    // The status reads as a pill, and the waits name what they waited for.
    await expect(canvas.getByText('failed')).toBeVisible()
    // The Time tab names what the run waited for.
    await expect(canvas.getByRole('tab', { name: /^Time/ })).toHaveTextContent(
      /9\.19s\s*Waiting for approval/,
    )
    // A call's wait and its reason are in its breakdown, which a screen reader reads with it.
    await expect(canvas.getByText(/Waited 6\.62s for approval\. Ran 31ms\./)).toBeInTheDocument()
    const timeline = canvas.getByRole('table', { name: 'Timeline records' })
    await expect(within(timeline).getByText('Agent · between calls')).toBeVisible()
    await expect(within(timeline).getByText('37.8s · no calls before failure')).toBeVisible()
    await expect(within(timeline).getAllByText('1 call failed')).toHaveLength(2)
    // Issues keeps the failed calls and what leads to them.
    await userEvent.click(canvas.getByRole('button', { name: 'Issues 2' }))
    await expect(within(timeline).queryByText('Find a tool that adds an MCP provider')).toBeNull()
    await expect(within(timeline).getAllByTitle('coral.providers.coral.addProvider')).toHaveLength(
      2,
    )
  },
}

export const AgentAtWork: Story = {
  args: { trace: exploreTrace() },
  // Catches: agent gaps drawn to scale by default, so a trace of mostly thinking shows its calls
  // as hairlines; a summary that needs a chart to read; saved functions hidden in the tree; or
  // context bytes with no record to blame.
  play: async ({ canvasElement }) => {
    // Gap labels need a desktop's width; Storybook's own canvas has no fixed size.
    if (!(await resizeViewport(1300, 900))) return
    const canvas = within(canvasElement)
    const timeline = canvas.getByRole('table', { name: 'Timeline records' })
    const tab = (name: RegExp) => canvas.getByRole('tab', { name })
    // The run heads with its measures: its time, mostly the agent model's, then what Coral kept
    // out of the agent's context and the functions it saved.
    await expect(tab(/^Time/)).toHaveTextContent(/1m 29s.*1m 26s.*Agent model.*2\.\d+s.*Coral/)
    await expect(tab(/^Context kept out/)).toHaveTextContent(
      /84\.3 kB.*106 kB read · 21\.4 kB sent/,
    )
    await expect(tab(/^Saved functions/)).toHaveTextContent(/3.*1 revised · 3 tested/)
    // The time lens is the timeline.
    await expect(within(timeline).getByText('5.00s · final answer')).toBeVisible()
    await expect(within(timeline).getByText('38.9s')).toBeVisible()
    await expect(within(timeline).getByLabelText("25.7 kB into the agent's context")).toBeVisible()
    // Call time folds the agent's gaps, each marked on the axis; wall time draws them to scale.
    const zigzags = () =>
      within(timeline).getByRole('columnheader', { name: 'Time' }).querySelectorAll('svg').length
    await expect(zigzags()).toBeGreaterThan(0)
    const wallTime = canvas.getByRole('radio', { name: 'Wall time' })
    await expect(wallTime).toHaveAttribute('aria-checked', 'false')
    await userEvent.click(wallTime)
    await expect(wallTime).toHaveAttribute('aria-checked', 'true')
    await expect(zigzags()).toBe(0)
    // The smoke test's eleven calls start folded under their program.
    await expect(within(timeline).getByText('11 calls')).toBeVisible()
    // The agent's report on a tool marks its calls, and Issues shows only them.
    await expect(
      canvas.getByText(/^Provider issue reported by the agent · .* · clickstackTraceWaterfall$/),
    ).toBeVisible()
    await userEvent.click(canvas.getByRole('button', { name: 'Issues 2' }))
    await expect(within(timeline).getAllByTitle(/clickstackTraceWaterfall$/)).toHaveLength(2)
    await expect(within(timeline).queryByText('Find ClickStack tools')).toBeNull()
    await userEvent.click(canvas.getByRole('button', { name: 'Issues 2' }))
    // The context lens: what each step read and sent, never a net.
    await userEvent.click(tab(/^Context kept out/))
    const steps = canvas.getByRole('table', { name: 'Context by step' })
    await expect(
      within(steps).getByRole('row', { name: /^Smoke-test the three saved functions/ }),
    ).toHaveTextContent(/55 kB.*1\.9 kB.*53\.1 kB/)
    // The functions lens: each function once, by its path, with its revisions and its test.
    await userEvent.click(tab(/^Saved functions/))
    const functions = canvas.getByRole('table', { name: 'Saved functions' })
    await expect(
      within(functions).getByRole('link', { name: 'clickstack.serviceHealth' }),
    ).toHaveAttribute('href', expect.stringContaining('/functions/clickstack.serviceHealth'))
    await expect(within(functions).getAllByText('Smoke-tested')).toHaveLength(3)
  },
}
