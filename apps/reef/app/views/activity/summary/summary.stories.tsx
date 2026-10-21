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
import type { SummaryDays } from '@/lib/activity-summary'
import { Card } from '@/wax/components'

const busy: ActivitySummaryProps = {
  days: 7,
  calls: { answered: 412, blocked: 23, notAllowed: 6, needsSetup: 5, failed: 9, canceled: 3 },
  stopped: [
    { callPath: 'providers.github.deleteRepository', blocked: 14, notAllowed: 0, needsSetup: 0 },
    { callPath: 'providers.gmail.sendMessage', blocked: 6, notAllowed: 2, needsSetup: 0 },
    { callPath: 'providers.salesforce.getAccount', blocked: 0, notAllowed: 0, needsSetup: 5 },
    { callPath: 'providers.linear.archiveIssue', blocked: 0, notAllowed: 4, needsSetup: 0 },
    { callPath: 'providers.datadog.muteMonitor', blocked: 3, notAllowed: 0, needsSetup: 0 },
  ],
  operations: [
    { callPath: 'providers.github.getIssue', calls: 188 },
    { callPath: 'providers.linear.searchIssues', calls: 97 },
    { callPath: 'providers.gmail.listMessages', calls: 64 },
    { callPath: 'providers.github.listPullRequestReviewComments', calls: 41 },
    { callPath: 'providers.datadog.queryMetrics', calls: 22 },
    { callPath: 'providers.slack.postMessage', calls: 12 },
    { callPath: 'providers.slack.listChannels', calls: 8 },
    { callPath: 'providers.notion.getPage', calls: 4 },
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

const quiet: ActivitySummaryProps = {
  days: 7,
  calls: { answered: 0, blocked: 0, notAllowed: 0, needsSetup: 0, failed: 0, canceled: 0 },
  stopped: [],
  operations: [],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
}

const meta = {
  component: ActivitySummary,
  title: 'Activity/Summary',
  args: busy,
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

export const Busy: Story = {
  // Catches: stopped calls losing the split by who must act, or an outcome missing from the bar.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('23 blocked · 6 not allowed · 5 need setup')).toBeVisible()
    const outcomes = canvas.getByRole('list', { name: 'Call outcomes' })
    await expect(
      within(outcomes)
        .getAllByRole('listitem')
        .map((item) => item.textContent),
    ).toEqual([
      'Answered412 · 90%',
      'Blocked by a rule23 · 5%',
      'Not allowed6 · 1%',
      'Needs setup5 · 1%',
      'Failed9 · 2%',
      'Canceled3 · 1%',
    ])
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
    await expect(operations).toHaveTextContent('3 others24 calls')
    const actors = canvas.getByRole('list', { name: 'Context by caller' })
    await expect(actors).toHaveTextContent('2 others')
  },
}

export const StillRunning: Story = {
  args: { contextImpactBytes: '-1820400', contextImpactPartial: true },
}

// The numbers fade while the chosen period's numbers load.
export const Pending: Story = { args: { days: 30, onDaysChange: () => {}, pending: true } }

export const ContextAdded: Story = { args: { contextImpactBytes: '30500' } }

export const Quiet: Story = {
  args: quiet,
  // Catches: an empty window showing zeros as if they were measured work.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('No calls')).toBeVisible()
    await expect(canvas.getByText('No stopped calls.')).toBeVisible()
  },
}

export const OneDay: Story = {
  args: {
    days: 1,
    calls: { answered: 38, blocked: 0, notAllowed: 1, needsSetup: 0, failed: 0, canceled: 0 },
    stopped: [
      { callPath: 'providers.gmail.sendMessage', blocked: 0, notAllowed: 1, needsSetup: 0 },
    ],
    operations: [{ callPath: 'providers.gmail.listMessages', calls: 39 }],
    approvals: { approved: 3, declined: 1, medianWaitSeconds: 38 },
    contextImpactBytes: '-204800',
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
      <StoppedOperations operations={busy.stopped} limit={3} />
    </div>
  ),
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
        <TopOperations operations={busy.operations} limit={3} />
        <StoppedOperations operations={busy.stopped} limit={3} />
        <ApprovalSummary approvals={busy.approvals} />
        <ContextBreakdown title="Context by caller" entries={busy.contextActors!} />
      </div>
    </div>
  ),
}
