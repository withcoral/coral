import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'
import {
  ActivitySummary,
  ApprovalSummary,
  CallOutcomeChart,
  CallsTile,
  ContextBreakdown,
  ContextSavedTile,
  StoppedOperations,
  StoppedTile,
  TopOperations,
  type ActivitySummaryProps,
} from './summary'
import type { ActivitySummaryData, SummaryDays } from '@/lib/activity-summary'
import { Card } from '@/wax/components'

const busy: ActivitySummaryData = {
  calls: { answered: 412, blocked: 23, notAllowed: 6, needsSetup: 5, failed: 9, canceled: 3 },
  stopped: [
    {
      callPath: 'coral.providers.github.deleteRepository',
      blocked: 14,
      notAllowed: 0,
      needsSetup: 0,
    },
    { callPath: 'coral.providers.gmail.sendMessage', blocked: 6, notAllowed: 2, needsSetup: 0 },
    {
      callPath: 'coral.providers.salesforce.getAccount',
      blocked: 0,
      notAllowed: 0,
      needsSetup: 5,
    },
    { callPath: 'coral.providers.linear.archiveIssue', blocked: 0, notAllowed: 4, needsSetup: 0 },
    { callPath: 'coral.providers.datadog.muteMonitor', blocked: 3, notAllowed: 0, needsSetup: 0 },
  ],
  operations: [
    { callPath: 'coral.providers.github.getIssue', calls: 188 },
    { callPath: 'coral.providers.linear.searchIssues', calls: 97 },
    { callPath: 'coral.providers.gmail.listMessages', calls: 64 },
    { callPath: 'coral.providers.github.listPullRequestReviewComments', calls: 41 },
    { callPath: 'coral.providers.datadog.queryMetrics', calls: 22 },
    { callPath: 'coral.providers.slack.postMessage', calls: 12 },
    { callPath: 'coral.providers.slack.listChannels', calls: 8 },
    { callPath: 'coral.providers.notion.getPage', calls: 4 },
  ],
  approvals: { approved: 17, declined: 6, medianWaitSeconds: 252 },
  contextImpactBytes: '-48213977',
  contextActors: [
    { key: 'collections', label: 'Collections agent', bytes: '-31004211' },
    { key: 'you', label: 'You', bytes: '-12880400' },
    { key: 'triage', label: 'Triage agent', bytes: '-4100250' },
    { key: 'digest', label: 'Digest agent', bytes: '-310016' },
    { key: 'old', label: 'Unavailable agent', bytes: '80900' },
  ],
}

const quiet: ActivitySummaryData = {
  calls: { answered: 0, blocked: 0, notAllowed: 0, needsSetup: 0, failed: 0, canceled: 0 },
  stopped: [],
  operations: [],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
}

const meta = {
  component: ActivitySummary,
  title: 'Activity/Summary',
  args: { summary: busy, days: 7 },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: () => <Story /> }])
      return (
        <div style={{ maxWidth: 1080, padding: 16 }}>
          <Stub />
        </div>
      )
    },
  ],
} satisfies Meta<typeof ActivitySummary>
export default meta
type Story = StoryObj<typeof meta>

/** The list item in `list` whose text includes `name`. */
const rowOf = (list: HTMLElement, name: string) =>
  within(list)
    .getAllByRole('listitem')
    .find((item) => within(item).queryByText(name))!

export const Busy: Story = {
  // Catches: stopped calls losing the split by who must act, or an outcome missing from the bar.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('23 blocked · 6 not allowed · 5 need setup')).toBeVisible()
    const outcomes = canvas.getByRole('list', { name: 'Call outcomes' })
    for (const [label, count] of [
      ['Answered', '412 · 90%'],
      ['Blocked by a rule', '23 · 5%'],
      ['Not allowed', '6 · 1%'],
      ['Needs setup', '5 · 1%'],
      ['Failed', '9 · 2%'],
      ['Canceled', '3 · 1%'],
    ])
      await expect(within(rowOf(outcomes, label)).getByText(count)).toBeVisible()
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
    await userEvent.click(canvas.getByRole('radio', { name: '30 days' }))
    await expect(canvas.getByRole('radio', { name: '30 days' })).toBeChecked()
  },
}

export const Others: Story = {
  // Catches: rows past the limit dropping out of the totals instead of adding up to one row.
  play: async ({ canvas }) => {
    const operations = canvas.getByRole('region', { name: 'Most called operations' })
    const others = rowOf(within(operations).getByRole('list'), '3 others')
    await expect(within(others).getByText('24 calls')).toBeVisible()
    const callers = canvas.getByRole('region', { name: 'Context by caller' })
    await expect(within(callers).getByText('2 others')).toBeVisible()
  },
}

export const StillRunning: Story = {
  args: { summary: { ...busy, contextImpactBytes: '-1820400', contextImpactPartial: true } },
  // Catches: a partial context total read as the whole window's.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('Saved · Incomplete')).toBeVisible()
    await expect(canvas.getByText(/so far$/)).toBeInTheDocument()
    const callers = canvas.getByRole('region', { name: 'Context by caller' })
    await expect(
      within(callers).getByText('Incomplete. Running and unmeasured runs are not counted.'),
    ).toBeVisible()
  },
}

export const NothingMeasuredYet: Story = {
  args: { summary: { ...quiet, contextImpactPartial: true } },
  // Catches: work that is still running shown as if no run happened.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('Context impact incomplete')).toBeInTheDocument()
  },
}

// The numbers fade while the chosen period's numbers load.
export const Pending: Story = {
  args: { days: 30, onDaysChange: () => {}, pending: true },
  // Catches: old numbers that only fade, with no status that says they are loading.
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('status')).toHaveTextContent('Loading…')
  },
}

export const ContextAdded: Story = { args: { summary: { ...busy, contextImpactBytes: '30500' } } }

export const Quiet: Story = {
  args: { summary: quiet },
  // Catches: an empty window showing zeros as if they were measured work.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('No calls')).toBeVisible()
    await expect(canvas.getByText('No stopped calls.')).toBeVisible()
    await expect(canvas.getByText('No context impact')).toBeInTheDocument()
    await expect(canvas.getByText('No measured runs.')).toBeVisible()
  },
}

export const Unavailable: Story = {
  args: { summary: null, onDaysChange: () => {} },
  // Catches: an unavailable summary hiding the period picker, or showing zeros.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('The summary is not available. Try again later.')).toBeVisible()
    await expect(canvas.getByRole('radio', { name: '7 days' })).toBeChecked()
    await expect(canvas.queryByRole('list', { name: 'Call outcomes' })).toBeNull()
  },
}

export const Owner: Story = {
  args: { owner: true },
  // Catches: an owner reading their agents' calls as their own.
  play: async ({ canvas }) => {
    await expect(
      canvas.getByRole('heading', { name: 'Activity by you and your agents' }),
    ).toBeVisible()
  },
}

export const OneDay: Story = {
  args: {
    days: 1,
    summary: {
      calls: { answered: 38, blocked: 0, notAllowed: 1, needsSetup: 1, failed: 0, canceled: 0 },
      stopped: [
        {
          callPath: 'coral.providers.gmail.sendMessage',
          blocked: 0,
          notAllowed: 1,
          needsSetup: 0,
        },
        {
          callPath: 'coral.providers.salesforce.getAccount',
          blocked: 0,
          notAllowed: 0,
          needsSetup: 1,
        },
      ],
      operations: [{ callPath: 'coral.providers.gmail.listMessages', calls: 40 }],
      approvals: { approved: 3, declined: 1, medianWaitSeconds: 38 },
      contextImpactBytes: '-204800',
    },
  },
  // Catches: a count of one read with the plural verb, as in "1 need setup".
  play: async ({ canvas }) => {
    await expect(canvas.getByText('1 not allowed · 1 needs setup')).toBeVisible()
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

// The Stopped tile beside its breakdown: a rule, a person or missing setup stopped each call.
export const Stopped: Story = {
  render: () => (
    <div
      style={{ display: 'grid', gap: 12, gridTemplateColumns: '240px 1fr', alignItems: 'start' }}
    >
      <StoppedTile calls={busy.calls} />
      <StoppedOperations operations={busy.stopped} />
    </div>
  ),
  // Catches: an operation's stopped calls losing who must act.
  play: async ({ canvas }) => {
    const list = within(canvas.getByRole('region', { name: 'Stopped operations' })).getByRole(
      'list',
    )
    const gmail = within(rowOf(list, 'coral.providers.gmail.sendMessage'))
    await expect(gmail.getByText('6 blocked')).toBeInTheDocument()
    await expect(gmail.getByText('2 not allowed')).toBeInTheDocument()
  },
}

// The parts on their own, to place them one at a time.
export const Parts: Story = {
  render: () => (
    <div style={{ display: 'grid', gap: 12 }}>
      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(3, 1fr)' }}>
        <CallsTile calls={busy.calls} />
        <StoppedTile calls={busy.calls} />
        <ContextSavedTile bytes={busy.contextImpactBytes} />
      </div>
      <Card.Container>
        <CallOutcomeChart calls={busy.calls} />
      </Card.Container>
      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(3, 1fr)' }}>
        <TopOperations operations={busy.operations} />
        <StoppedOperations operations={busy.stopped} />
        <ApprovalSummary approvals={busy.approvals} />
        <ContextBreakdown entries={busy.contextActors!} />
      </div>
    </div>
  ),
}
