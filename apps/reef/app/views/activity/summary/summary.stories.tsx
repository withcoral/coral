import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'
import { ActivitySummary, type ActivitySummaryProps, type SummaryLinks } from './summary'
import type { ActivitySummaryData, SummaryDays } from '@/lib/activity-summary'

/** A month of a team's use. Every stopped operation is also among the busiest. */
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
  operations: [
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
  operations: [
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
  operations: [],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
}

const links: SummaryLinks = {
  activity: '/workspaces/w/activity',
  provider: (callPath) => {
    const id = callPath.split('.')[2]
    return { name: id[0].toUpperCase() + id.slice(1), path: `/workspaces/w/providers/${id}` }
  },
  operation: (callPath) => {
    const [, , id, ...rest] = callPath.split('.')
    const path = `/workspaces/w/providers/${id}`
    return {
      operation: `${path}/ops/${rest.join('.')}`,
      rules: `${path}/policy`,
      settings: `${path}/settings`,
    }
  },
}

const meta = {
  component: ActivitySummary,
  title: 'Activity/Summary',
  args: { summary: busy, days: 30, links },
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
  // Catches: a metric that loses its split, or approvals hidden behind a lens.
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Calls/)).toHaveTextContent('99.2% answered')
    const needs = tab(canvas, /^Needs you/)
    await expect(needs).toHaveTextContent('7 blocked · 2 need setup · 3 not allowed')
    await expect(needs).toHaveTextContent('38 approved · 3 declined · 2 min median wait')
    await expect(canvas.getByRole('list', { name: 'Calls by outcome' })).toHaveTextContent(
      'Failed14 · 0.4%',
    )
  },
}

export const NeedsYou: Story = {
  parameters: { search: '?lens=needs' },
  // Catches: a stop without the page that fixes it, or a person's no listed as a fix.
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Needs you/)).toHaveAttribute('aria-selected', 'true')
    const toFix = within(canvas.getByRole('region', { name: 'To fix' }))
    await expect(toFix.getAllByRole('link', { name: 'Review rules' })).toHaveLength(2)
    await expect(toFix.getByRole('link', { name: 'Fix setup' })).toHaveAttribute(
      'href',
      '/workspaces/w/providers/salesforce/settings',
    )
    await expect(toFix.queryByText('sendMessage')).toBeNull()
    const people = within(canvas.getByRole('region', { name: 'Decided by people' }))
    await expect(people.getByRole('group', { name: 'Declined' })).toHaveTextContent('3')
  },
}

export const GroupedByProvider: Story = {
  // Catches: a provider group whose total leaves out an operation's calls or stops.
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('radio', { name: 'Providers' }))
    const operations = within(canvas.getByRole('table', { name: 'Operations' }))
    const github = operations
      .getAllByRole('row')
      .find((row) => within(row).queryByRole('button', { name: 'Collapse Github' }))!
    await expect(github).toHaveTextContent('1,832')
    await expect(github).toHaveTextContent('4 blocked')
  },
}

export const Filtered: Story = {
  // Catches: a filter that ignores the provider name, or forgets how many it hid.
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter operations' }), 'slack')
    await expect(await canvas.findByText('2 of 11')).toBeVisible()
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
    await expect(needs).toHaveTextContent('Nothing')
    await expect(needs).toHaveTextContent('No approval decisions')
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
    await expect(
      canvas.getByRole('heading', { name: 'Activity by you and your agents' }),
    ).toBeVisible()
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
      operations: [{ callPath: 'coral.providers.gmail.listMessages', calls: 38 }],
      approvals: { approved: 3, declined: 1, medianWaitSeconds: 38 },
    },
  },
  // Catches: a count of one read with the plural verb, as in "1 need setup".
  play: async ({ canvas }) => {
    await expect(tab(canvas, /^Needs you/)).toHaveTextContent('1 needs setup · 1 not allowed')
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
