import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'
import { ActivitySummary, type ActivitySummaryProps, type SummaryProvider } from './summary'
import type { ActivitySummaryData, SummaryDays } from '@/lib/activity-summary'

/** A month of a team's use. Every stopped tool is also among the busiest. */
const busy: ActivitySummaryData = {
  calls: { answered: 3781, blocked: 7, notAllowed: 3, needsSetup: 2, failed: 14, canceled: 5 },
  stopped: [
    {
      callPath: 'coral.providers.github.mergePullRequest',
      blocked: 4,
      notAllowed: 1,
      needsSetup: 0,
    },
    { callPath: 'coral.providers.slack.postMessage', blocked: 3, notAllowed: 0, needsSetup: 0 },
    { callPath: 'coral.providers.gmail.sendMessage', blocked: 0, notAllowed: 2, needsSetup: 0 },
    {
      callPath: 'coral.providers.salesforce.getAccount',
      blocked: 0,
      notAllowed: 0,
      needsSetup: 2,
    },
  ],
  tools: [
    { callPath: 'coral.providers.github.getPullRequest', calls: 1204 },
    { callPath: 'coral.providers.linear.searchIssues', calls: 811 },
    { callPath: 'coral.providers.github.listPullRequestReviewComments', calls: 532 },
    { callPath: 'coral.providers.slack.listChannelMessages', calls: 418 },
    { callPath: 'coral.providers.datadog.queryMetrics', calls: 297 },
    { callPath: 'coral.providers.slack.postMessage', calls: 188 },
    { callPath: 'coral.providers.gmail.listMessages', calls: 143 },
    { callPath: 'coral.providers.github.mergePullRequest', calls: 96 },
    { callPath: 'coral.providers.notion.getPage', calls: 71 },
    { callPath: 'coral.providers.salesforce.getAccount', calls: 41 },
    { callPath: 'coral.providers.gmail.sendMessage', calls: 11 },
  ],
  approvals: { approved: 38, declined: 3, medianWaitSeconds: 96 },
  contextImpactBytes: '-187420311',
  contextActors: [
    { key: 'review', label: 'PR review agent', bytes: '-102881004' },
    { key: 'you', label: 'You', bytes: '-51230977' },
    { key: 'triage', label: 'Triage agent', bytes: '-28004110' },
    { key: 'digest', label: 'Daily digest agent', bytes: '-5304220' },
  ],
}

const quiet: ActivitySummaryData = {
  calls: { answered: 962, blocked: 0, notAllowed: 0, needsSetup: 0, failed: 0, canceled: 0 },
  stopped: [],
  tools: [
    { callPath: 'coral.providers.github.getPullRequest', calls: 512 },
    { callPath: 'coral.providers.linear.searchIssues', calls: 301 },
    { callPath: 'coral.providers.notion.getPage', calls: 149 },
  ],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
  contextImpactBytes: '-40210554',
  contextActors: [{ key: 'you', label: 'You', bytes: '-40210554' }],
}

const empty: ActivitySummaryData = {
  calls: { answered: 0, blocked: 0, notAllowed: 0, needsSetup: 0, failed: 0, canceled: 0 },
  stopped: [],
  tools: [],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
}

// Every provider the fixtures call, named after its namespace.
const providers: SummaryProvider[] = [
  'datadog',
  'github',
  'gmail',
  'linear',
  'notion',
  'salesforce',
  'slack',
].map((id) => ({
  callRoot: `coral.providers.${id}`,
  displayName: id[0].toUpperCase() + id.slice(1),
  path: `/workspaces/w/providers/${id}`,
}))

const meta = {
  component: ActivitySummary,
  title: 'Activity/Summary',
  args: { summary: busy, days: 30, providers },
  decorators: [
    (Story, { parameters }) => {
      const Stub = createRoutesStub([{ path: '*', Component: () => <Story /> }])
      return (
        <div style={{ maxWidth: 1080, padding: 16 }}>
          <Stub initialEntries={[`/${parameters.search ?? ''}`]} />
        </div>
      )
    },
  ],
} satisfies Meta<typeof ActivitySummary>
export default meta
type Story = StoryObj<typeof meta>

const tab = (canvas: ReturnType<typeof within>, name: RegExp) => canvas.getByRole('tab', { name })

export const Busy: Story = {
  // Catches: a metric that loses its split, counts calls a person declined as yours to fix, or
  // miscounts who moved the context.
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Calls/)).toHaveTextContent('99.2% answered')
    await expect(tab(canvas, /^Context/)).toHaveTextContent('Saved · You and 3 agents')
    await expect(tab(canvas, /^Needs you/)).toHaveTextContent('9 to fix7 blocked · 2 need setup')
    await expect(canvas.getByRole('group', { name: 'Failed' })).toHaveTextContent('140.4%')
  },
}

export const NeedsYou: Story = {
  parameters: { search: '?lens=needs' },
  // Catches: a stop without the page that fixes it, a person's no listed as a fix, calls
  // counted among the approval decisions, or ↓ that stops at the end of the first table.
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Needs you/)).toHaveAttribute('aria-selected', 'true')
    const toFix = within(canvas.getByRole('region', { name: 'To fix' }))
    await expect(toFix.getAllByRole('link')).toHaveLength(3)
    await expect(toFix.getByRole('link', { name: 'salesforce.getAccount' })).toHaveAttribute(
      'href',
      '/workspaces/w/providers/salesforce/settings',
    )
    await expect(toFix.queryByText('sendMessage')).toBeNull()
    const decisions = within(canvas.getByRole('region', { name: 'Approval decisions' }))
    await expect(decisions.getByRole('group', { name: 'Declined' })).toHaveTextContent('3')
    await expect(decisions.queryByText('Calls not allowed')).toBeNull()
    const notAllowed = within(canvas.getByRole('region', { name: 'Calls not allowed' }))
    toFix.getAllByRole('link').at(-1)!.focus()
    await userEvent.keyboard('{ArrowDown}')
    await expect(notAllowed.getAllByRole('link')[0]).toHaveFocus()
  },
}

export const Filtered: Story = {
  // Catches: a filter that ignores the provider name, or forgets how many it hid, or rows
  // that ↓ cannot reach from the filter or that do not open the busiest tool first.
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter tools' }), 'slack')
    await expect(await canvas.findByText('2 of 11')).toBeVisible()
    await userEvent.keyboard('{ArrowDown}')
    await expect(document.activeElement).toHaveAttribute(
      'href',
      '/workspaces/w/providers/slack/tools/listChannelMessages',
    )
  },
}

export const ContextByCaller: Story = {
  parameters: { search: '?lens=context' },
  // Catches: a caller's row that loses the exact byte count behind its rounded value.
  play: async ({ canvas }) => {
    const callers = within(canvas.getByRole('table', { name: 'Context by caller' }))
    const you = callers.getAllByRole('row').find((row) => within(row).queryByText('You'))!
    await expect(you.querySelector('[title^="51,230,977 bytes saved."]')).not.toBeNull()
  },
}

function WithPeriod(args: ActivitySummaryProps) {
  const [days, setDays] = useState<SummaryDays>(args.days)
  return <ActivitySummary {...args} days={days} onDaysChange={setDays} />
}
export const PeriodPicker: Story = {
  render: (args) => <WithPeriod {...args} />,
  // Catches: the picker not showing the period you chose.
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('radio', { name: '7 days' }))
    await expect(canvas.getByRole('radio', { name: '7 days' })).toBeChecked()
  },
}

// The numbers fade while the chosen period's numbers load.
export const Pending: Story = {
  args: { onDaysChange: () => {}, pending: true },
  // Catches: old numbers that only fade, with no status that says they are loading.
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('status')).toHaveTextContent('Loading…')
  },
}

export const ContextAdded: Story = {
  args: {
    summary: {
      ...busy,
      contextImpactBytes: '30500',
      contextActors: [{ key: 'you', label: 'You', bytes: '30500' }],
    },
  },
}

export const AllClear: Story = {
  args: { summary: quiet },
  // Catches: a quiet period that still reads as if something needs you.
  play: async ({ canvas }) => {
    const needs = tab(canvas, /^Needs you/)
    await expect(needs).toHaveTextContent('NothingNo calls to fix')
  },
}

export const NoCalls: Story = {
  args: { summary: empty },
  // Catches: an empty period showing zeros as if they were measured work.
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Calls/)).toHaveTextContent('No calls')
    await expect(canvas.getByText('No calls.')).toBeVisible()
    await expect(tab(canvas, /^Context/)).toHaveTextContent('No runs')
  },
}

export const Unavailable: Story = {
  args: { summary: null, onDaysChange: () => {} },
  // Catches: an unavailable summary hiding the period picker, or showing zeros.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('The summary is not available. Try again later.')).toBeVisible()
    await expect(canvas.getByRole('radio', { name: '30 days' })).toBeChecked()
    await expect(canvas.queryByRole('tablist')).toBeNull()
  },
}

export const Owner: Story = {
  args: { owner: true },
  // Catches: an owner reading their agents' calls as their own.
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('heading', { name: 'Workspace activity' })).toBeVisible()
  },
}

export const OneOfEach: Story = {
  args: {
    days: 1,
    summary: {
      ...empty,
      calls: { answered: 38, blocked: 0, notAllowed: 1, needsSetup: 1, failed: 0, canceled: 0 },
      stopped: [
        { callPath: 'coral.providers.gmail.sendMessage', blocked: 0, notAllowed: 1, needsSetup: 0 },
        {
          callPath: 'coral.providers.salesforce.getAccount',
          blocked: 0,
          notAllowed: 0,
          needsSetup: 1,
        },
      ],
      tools: [{ callPath: 'coral.providers.gmail.listMessages', calls: 38 }],
      approvals: { approved: 3, declined: 1, medianWaitSeconds: 38 },
    },
  },
  // Catches: a count of one read with the plural verb, as in "1 need setup".
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Needs you/)).toHaveTextContent('1 to fix1 needs setup')
  },
}

export const Narrow: Story = {
  decorators: [
    (Story) => (
      <div style={{ width: 360 }}>
        <Story />
      </div>
    ),
  ],
}
