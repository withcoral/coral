import type { Decorator, Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, waitFor, within } from 'storybook/test'
import { AgentActivityPage } from './agent-activity-page'
import { AgentSettingsPage } from './agent-settings-page'
import { AgentsPage } from './agents-page'
import { ManagementError } from './management-error'
import { OrganizationMembersPage } from './organization-members-page'
import { OrganizationWorkspacesPage } from './organization-workspaces-page'
import { WorkspaceMembersPage, type WorkspaceMembersPageProps } from './workspace-members-page'
import { Activity } from '@/views/activity/activity'
import type { ActivityRow, ActivityViewProps } from '@/views/activity/types'
import { ActivitySummary } from '@/views/activity/summary/summary'
import { ACTIVITY_PAGE_SIZE } from '@/lib/activity-list'
import { resizeViewport } from '../../../.storybook/browser'
import { WorkspaceOverview, type WorkspaceOverviewProps } from './workspace-overview'

const form = { csrf: 'storybook-csrf', requestId: 'storybook-request' }
// The app shell bounds a page's height; a story that scrolls inside it needs the same bound.
const viewportHeight: Decorator = (Story) => (
  <div style={{ height: '100vh' }}>
    <Story />
  </div>
)
const jordan = {
  user: 'users/jordan',
  name: 'Jordan Lee',
  email: 'jordan@example.com',
  local: false,
}
const alice = { user: 'users/alice', name: 'Alice Chen', email: 'alice@example.com', local: false }
const workspace = {
  name: 'workspaces/demo',
  displayName: 'Operations team',
  state: 'active',
  etag: 'workspace-etag',
  effectiveRole: 'owner',
  currentUser: 'users/jordan',
  profile: jordan,
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  connectEndpoint: 'https://mcp.example.com/mcp',
  path: '/workspaces/demo',
}
const member = { ...workspace, effectiveRole: 'member' }
const archived = { ...workspace, state: 'archived' }
const agent = {
  name: 'workspaces/demo/agents/automation',
  displayName: 'Issue triage',
  state: 'enabled',
  etag: 'agent-etag',
  createdBy: 'users/jordan',
  creator: jordan,
  accessBlockers: [] as string[],
  path: '/workspaces/demo/agents/automation',
  settingsPath: '/workspaces/demo/agents/automation/settings',
}
const blockedAgent = {
  ...agent,
  name: 'workspaces/demo/agents/reconciler',
  displayName: 'Nightly reconciler',
  state: 'disabled',
  accessBlockers: ['Agent disabled', 'Access expired'],
  path: '/workspaces/demo/agents/reconciler',
  settingsPath: '/workspaces/demo/agents/reconciler/settings',
}
const registration = {
  name: `${agent.name}/registration`,
  uid: 'registration-id',
  enabled: true,
  etag: 'registration-etag',
  expiration: '',
}
const owners = [
  { user: 'users/jordan', profile: jordan, enabled: true },
  { user: 'users/alice', profile: alice, enabled: true },
]
const ownMembership = {
  name: 'workspaces/demo/memberships/jordan',
  user: 'users/jordan',
  profile: jordan,
  role: 'member',
  enabled: true,
  etag: 'self',
}
// The overview and agent routes pass no endpoint: their quick start cards show the setup.
const activityPage = (
  rows: ActivityRow[],
  owner: string | null = null,
  more?: ActivityViewProps['more'],
) => (
  <Activity
    page={{
      rows,
      branches: {},
      expanded: [],
      hasBefore: false,
      hasAfter: false,
      anchorUnavailable: false,
      listUnavailable: false,
      agent: owner,
      detail: null,
      unavailable: false,
      selected: null,
      selectedRow: null,
      listPath: workspace.path,
    }}
    more={more}
  />
)
const emptyAgentActivity = activityPage([], agent.name)
const emptyAgentGlimpse = activityPage([], agent.name, {
  to: `${agent.path}?view=activity`,
  label: 'View all activity',
  roots: ACTIVITY_PAGE_SIZE,
})
const overview: WorkspaceOverviewProps = {
  workspace,
  agents: [agent, blockedAgent],
  providers: [
    {
      id: 'github',
      displayName: 'GitHub',
      namespace: 'github',
      path: '/workspaces/demo/providers/github',
      blockers: [],
    },
    {
      id: 'linear',
      displayName: 'Linear',
      namespace: 'linear',
      path: '/workspaces/demo/providers/linear',
      blockers: ['Account required'],
    },
  ],
  form,
  recipes: [],
  owners,
  ownMembership: null,
}
const members: WorkspaceMembersPageProps = {
  workspace,
  form,
  memberships: [
    {
      name: 'workspaces/demo/memberships/jordan',
      user: 'users/jordan',
      profile: jordan,
      role: 'owner',
      enabled: true,
      etag: 'own-etag',
    },
    {
      name: 'workspaces/demo/memberships/alice',
      user: 'users/alice',
      profile: alice,
      role: 'owner',
      enabled: true,
      etag: 'member-etag',
    },
  ],
  owners,
  ownMembership: null,
  nextMembers: '',
  nextOwners: '',
  candidates: [
    { user: 'users/sam', name: 'Sam Rivera', email: 'sam@example.com', local: false },
    { user: 'users/priya', name: 'Priya Natarajan', email: 'priya@example.com', local: false },
    { user: 'users/noname', name: '', email: 'ops-bot@example.com', local: false },
  ],
}
const meta = {
  title: 'Management/Workspaces and Agents',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: Story,
          action: () =>
            context.parameters.dialogPending
              ? new Promise(() => {})
              : {
                  error: context.parameters.dialogError ?? null,
                  credential: null,
                  refused: context.parameters.dialogRefused,
                },
        },
      ])
      return <Stub initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo']} />
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

// Errors
export const ConcealedResource: Story = { render: () => <ManagementError status={404} /> }
export const PermissionDenied: Story = { render: () => <ManagementError status={403} /> }
export const Unavailable: Story = { render: () => <ManagementError status={503} /> }

// Overview
export const OwnerOverview: Story = { render: () => <WorkspaceOverview {...overview} /> }
// The last week's totals sit under the cards.
export const SummaryOverview: Story = {
  render: () => (
    <WorkspaceOverview
      {...overview}
      summary={
        <ActivitySummary
          days={7}
          onDaysChange={() => {}}
          owner
          summary={{
            calls: {
              answered: 128,
              blocked: 4,
              notAllowed: 2,
              needsSetup: 0,
              failed: 3,
              canceled: 0,
            },
            stopped: [
              {
                callPath: 'coral.providers.github.deleteRepository',
                blocked: 4,
                notAllowed: 0,
                needsSetup: 0,
              },
              {
                callPath: 'coral.providers.gmail.sendMessage',
                blocked: 0,
                notAllowed: 2,
                needsSetup: 0,
              },
            ],
            tools: [
              { callPath: 'coral.providers.github.getIssue', calls: 81, callers: [] },
              { callPath: 'coral.providers.linear.searchIssues', calls: 40, callers: [] },
              { callPath: 'coral.providers.gmail.sendMessage', calls: 16, callers: [] },
            ],
            approvals: { approved: 5, declined: 2, medianWaitSeconds: 95 },
            callers: [],
            contextImpactBytes: '-3204411',
            contextActors: [
              { key: 'you', label: 'You', bytes: '-2900000' },
              { key: 'triage', label: 'Triage agent', bytes: '-304411' },
            ],
          }}
        />
      }
    />
  ),
}
// Each card lists three rows, then links to the page that lists them all.
export const CrowdedOverview: Story = {
  render: () => (
    <WorkspaceOverview
      {...overview}
      providers={['GitHub', 'Linear', 'Slack', 'Stripe', 'Notion', 'Sentry', 'Datadog'].map(
        (name, i) => ({
          id: `p${i}`,
          displayName: name,
          namespace: name.toLowerCase(),
          path: `/workspaces/demo/providers/p${i}`,
          blockers: i === 2 ? ['Account required'] : [],
        }),
      )}
      agents={Array.from({ length: 6 }, (_, i) => ({
        ...agent,
        name: `${agent.name}-${i}`,
        displayName: `Automation ${i + 1}`,
        path: `${agent.path}-${i}`,
      }))}
      owners={['Jordan Lee', 'Alice Chen', 'Sam Ortiz', 'Priya Rao', 'Ken Ito'].map((name, i) => ({
        user: `users/u${i}`,
        profile: { user: `users/u${i}`, name, email: `u${i}@example.com`, local: false },
        enabled: true,
      }))}
    />
  ),
}
export const FreshOverview: Story = {
  render: () => <WorkspaceOverview {...overview} agents={[]} providers={[]} />,
}
export const MemberOverview: Story = {
  render: () => (
    <WorkspaceOverview {...overview} workspace={member} agents={[]} ownMembership={ownMembership} />
  ),
}
// The members card marks a disabled owner in words beside their name.
export const DisabledOwnerOverview: Story = {
  render: () => (
    <WorkspaceOverview
      {...overview}
      owners={[owners[0], { user: alice.user, profile: alice, enabled: false }]}
    />
  ),
  play: async ({ canvas }) => {
    const card = within(
      canvas.getByRole('heading', { name: 'Members' }).closest('div')!.parentElement!,
    )
    await expect(
      within(card.getByText('Alice Chen').closest('li')!).getByText('Disabled'),
    ).toBeVisible()
    await expect(card.getAllByText('Disabled')).toHaveLength(1)
  },
}
export const NoEndpointOverview: Story = {
  render: () => (
    <WorkspaceOverview
      {...overview}
      workspace={{ ...workspace, mcpEndpoint: '', connectEndpoint: '' }}
    />
  ),
}

// Agents
export const AgentListing: Story = {
  render: () => (
    <AgentsPage workspace={workspace} agents={[agent, blockedAgent]} nextPage="next" form={form} />
  ),
  parameters: { initialEntry: '/workspaces/demo/agents' },
  play: async ({ canvas }) => {
    await expect(
      canvas.getByRole('link', { name: `${agent.displayName} settings` }),
    ).toHaveAttribute('href', agent.settingsPath)
  },
}
export const FilterAgents: Story = {
  ...AgentListing,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const page = within(canvasElement.ownerDocument.body)
    await userEvent.click(
      canvas.getByRole('button', { name: 'Filter agents by status: All statuses' }),
    )
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'Blocked' }))
    // The filters apply through a deferred value, so the rows can lag a render.
    await waitFor(() => expect(canvas.queryByRole('link', { name: agent.displayName })).toBeNull())
    await expect(canvas.getByRole('link', { name: blockedAgent.displayName })).toBeVisible()

    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter agents' }), 'deploy')
    await expect(await canvas.findByText('No agents match “deploy”.')).toBeVisible()
  },
}
// The table scrolls its rows under its heading; the page stays the viewport's height.
export const ManyAgents: Story = {
  render: () => (
    <AgentsPage
      workspace={workspace}
      agents={Array.from({ length: 40 }, (_, i) => ({
        ...agent,
        name: `${agent.name}-${i}`,
        // One name is too long for its column, so it truncates.
        displayName:
          i === 0
            ? `${agent.displayName} for every payments, billing and reconciliation queue in each region`
            : `${agent.displayName} ${i + 1}`,
        settingsPath: `/${agent.name}-${i}/settings`,
      }))}
      nextPage="next"
      form={form}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/agents', chromatic: { viewports: [390, 1280] } },
  // A long name stays on one line and shows in full on hover.
  play: async ({ canvasElement }) => {
    const table = within(canvasElement).getByRole('table', { name: 'Agents' })
    const rows = within(table).getAllByRole('row').slice(1)
    await expect(rows[0].getBoundingClientRect().height).toBe(
      rows[1].getBoundingClientRect().height,
    )
    const [name] = within(rows[0]).getAllByRole('cell')
    await expect(name).toHaveAttribute('title', expect.stringContaining('reconciliation queue'))
  },
  decorators: [viewportHeight],
}
/** The table keeps its shape with no agents, and offers to create one. */
export const NoAgents: Story = {
  render: () => <AgentsPage workspace={workspace} agents={[]} nextPage="" form={form} />,
  parameters: { initialEntry: '/workspaces/demo/agents' },
  play: async ({ canvas }) => {
    const table = within(canvas.getByRole('table', { name: 'Agents' }))
    await expect(table.getByText('No agents yet')).toBeVisible()
    await expect(table.getByRole('button', { name: 'Create agent' })).toBeVisible()
  },
}
export const ArchivedAgents: Story = {
  render: () => <AgentsPage workspace={archived} agents={[agent]} nextPage="" form={form} />,
  parameters: { initialEntry: '/workspaces/demo/agents' },
}
export const AgentCreated: Story = {
  render: () => (
    <AgentsPage
      workspace={workspace}
      agents={[agent]}
      nextPage=""
      form={form}
      result={{
        error: null,
        credential: 'storybook-only-api-key',
        agentDisplayName: agent.displayName,
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/agents' },
}
export const CreateAgentDialog: Story = {
  ...NoAgents,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Create agent' }))
  },
}

// Agent
const agentProviders: import('./agent-providers').AgentProviderView[] = [
  {
    provider: 'missing',
    name: 'Separate account',
    namespace: 'separate',
    account: { kind: 'separate', state: 'disconnected', canConnect: true, path: '/accounts' },
    blockers: ['Connect an account for this agent'],
  },
  {
    provider: 'connected',
    name: 'Connected account',
    namespace: 'connected',
    account: { kind: 'separate', state: 'ready', canConnect: false, path: '/accounts' },
    blockers: [],
  },
  {
    provider: 'reconnect',
    name: 'Account needs reconnection',
    namespace: 'reconnect',
    account: { kind: 'separate', state: 'reconnect', canConnect: true, path: '/accounts' },
    blockers: ['Connect an account for this agent'],
  },
  {
    provider: 'shared',
    name: 'Shared account',
    namespace: 'shared',
    account: { kind: 'shared', path: '/accounts' },
    blockers: [],
  },
  {
    provider: 'public',
    name: 'Public operations',
    namespace: 'public',
    account: { kind: 'none' },
    blockers: [],
  },
  {
    provider: 'disabled',
    name: 'Disabled provider',
    namespace: 'disabled',
    account: { kind: 'separate', state: 'disconnected', canConnect: false, path: '/accounts' },
    blockers: ['Provider disabled', 'Connect an account for this agent'],
  },
]
const activityIntents = [
  'Reconcile nightly invoices',
  'Triage incoming support requests',
  'Update the release readiness report',
  'Check overdue customer follow-ups',
  'Summarize provider health signals',
  'Review unresolved approval requests',
]
const busyIntent = (index: number) =>
  `${activityIntents[index % activityIntents.length]} · batch ${index + 1}`
const busyAgentActivityRows: ActivityRow[] = Array.from({ length: 36 }, (_, index) => {
  const ended = Date.UTC(2026, 8, 22, 9) - index * 3_600_000
  return {
    kind: 'task' as const,
    name: `${workspace.name}/tasks/review-${index + 1}`,
    workspace: workspace.name,
    agent: agent.name,
    actor: agent.displayName,
    intent: busyIntent(index),
    outcome: index % 7 === 0 ? 'failure' : 'success',
    startedAt: new Date(ended - 90_000).toISOString(),
    endedAt: new Date(ended).toISOString(),
  }
})
const busyAgentActivity = activityPage(busyAgentActivityRows, agent.name)
const agentActivityGlimpse = activityPage(busyAgentActivityRows, agent.name, {
  to: `${agent.path}?view=activity`,
  label: 'View all activity',
  roots: ACTIVITY_PAGE_SIZE,
})
export const AgentActivity: Story = {
  render: () => (
    <AgentActivityPage
      workspace={workspace}
      agent={agent}
      providers={agentProviders}
      form={form}
      activity={busyAgentActivity}
      recentActivity={agentActivityGlimpse}
    />
  ),
  parameters: { initialEntry: agent.path },
  // The app shell bounds the page; the cards fill its height and scroll their rows.
  decorators: [viewportHeight],
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('link', { name: 'Settings' })).toHaveAttribute(
      'href',
      agent.settingsPath,
    )
    // Activity only reads; every change waits behind Settings.
    for (const name of ['Replace API key', 'Disable agent', 'Edit expiration', 'Delete agent'])
      await expect(canvas.queryByRole('button', { name })).toBeNull()
    // Catches: a long list stretching its card past the page instead of scrolling its rows.
    if (!(await resizeViewport(1440, 800))) return
    const accounts = canvas.getByRole('region', { name: 'Accounts' })
    const recent = canvas.getByRole('region', { name: 'Recent activity' })
    await waitFor(() => {
      for (const card of [accounts, recent])
        expect(card.getBoundingClientRect().bottom).toBeLessThanOrEqual(window.innerHeight)
      expect(accounts.getBoundingClientRect().top).toBe(recent.getBoundingClientRect().top)
    })
    const last = within(recent).getByRole('link', {
      name: busyIntent(ACTIVITY_PAGE_SIZE - 1),
    })
    expect(last.getBoundingClientRect().top).toBeGreaterThan(recent.getBoundingClientRect().bottom)
    expect(
      within(recent).queryByRole('link', {
        name: busyIntent(ACTIVITY_PAGE_SIZE),
      }),
    ).toBeNull()
  },
}
// Catches: a short viewport trapping Recent activity below the visible page.
export const AgentActivityShortViewport: Story = {
  ...AgentActivity,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  play: async ({ canvas }) => {
    const activity = canvas.getByRole('table', { name: 'Activity records' })
    activity.scrollIntoView({ block: 'end' })
    await waitFor(() =>
      expect(activity.getBoundingClientRect().bottom).toBeLessThanOrEqual(window.innerHeight + 1),
    )
  },
}
export const AgentFullActivity: Story = {
  ...AgentActivity,
  parameters: { initialEntry: `${agent.path}?view=activity` },
  // The app shell bounds the split page; the story needs the same height so
  // Activity scrolls its records instead of growing with them.
  decorators: [viewportHeight],
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('heading', { name: 'Activity' })).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'Back to agent' })).toHaveAttribute(
      'href',
      agent.path,
    )
    // Catches: an icon-only live toggle, which reads as nothing here without the list heading.
    const live = canvas.getByRole('button', { name: 'Live' })
    await expect(live).toBeVisible()
    await expect(live).toHaveTextContent(/^Live$/)
    await expect(live).toHaveAttribute('aria-pressed', 'false')
    await expect(live).toHaveAccessibleDescription('Follow the newest task or run.')
    await expect(live.querySelector('svg')).toHaveClass('lucide-locate')
    await userEvent.click(live)
    await waitFor(() => expect(live).toHaveAttribute('aria-pressed', 'true'))
    await expect(live).toHaveTextContent(/^Live$/)
    await expect(live.querySelector('svg')).toHaveClass('lucide-locate-fixed')
  },
}
// Catches: a link into the agent's Activity list opening the dashboard instead.
export const AgentActivityAfterRecord: Story = {
  ...AgentFullActivity,
  parameters: { initialEntry: `${agent.path}?anchor=x&tree=1` },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('link', { name: 'Back to agent' })).toBeVisible()
    await expect(canvas.queryByRole('heading', { name: 'Recent activity' })).toBeNull()
  },
}
// Catches: an agent with no provider requirements showing an empty Accounts card.
export const AgentWithoutAccounts: Story = {
  render: () => (
    <AgentActivityPage
      workspace={workspace}
      agent={agent}
      providers={[]}
      form={form}
      activity={busyAgentActivity}
      recentActivity={agentActivityGlimpse}
    />
  ),
  parameters: { initialEntry: agent.path },
  play: async ({ canvas }) => {
    await expect(canvas.queryByRole('region', { name: 'Accounts' })).toBeNull()
    await expect(canvas.getByRole('heading', { name: 'Recent activity' })).toBeVisible()
  },
}
// Catches: a client's setup losing the workspace endpoint or the key placeholder.
export const NewAgentActivity: Story = {
  render: () => (
    <AgentActivityPage
      workspace={workspace}
      agent={agent}
      providers={agentProviders}
      form={form}
      activity={emptyAgentActivity}
      recentActivity={emptyAgentGlimpse}
      idle
    />
  ),
  parameters: { initialEntry: agent.path },
  play: async ({ canvas }) => {
    const card = within(canvas.getByRole('region', { name: 'Connect this agent' }))
    const endpoint = workspace.connectEndpoint
    for (const [client, field, value] of [
      [
        'Claude Code',
        'Run in a terminal',
        `claude mcp add --transport http coral ${endpoint} --header "Authorization: Bearer <AGENT_API_KEY>"`,
      ],
      ['Other', 'MCP URL', endpoint],
      ['Other', 'Authorization header', 'Bearer <AGENT_API_KEY>'],
    ] as const) {
      await userEvent.click(card.getByRole('tab', { name: client }))
      const step = within(card.getByRole('group', { name: field }))
      await expect(step.getByText(value, { exact: true })).toBeVisible()
    }
    await expect(card.getByRole('link', { name: 'Replace it in settings' })).toHaveAttribute(
      'href',
      agent.settingsPath,
    )
    // Catches: setup hiding the accounts the agent still needs, or offering the wrong action.
    const accounts = within(canvas.getByRole('region', { name: 'Accounts' }))
    await expect(
      accounts.getByRole('button', { name: 'Connect account for separate' }),
    ).toHaveTextContent('Connect')
    await expect(
      accounts.getByRole('button', { name: 'Reconnect account for reconnect' }),
    ).toHaveTextContent('Reconnect')
    await expect(
      accounts.getByRole('link', { name: 'Manage account for connected' }),
    ).toHaveAttribute('href', '/accounts')
    await expect(
      accounts.getByRole('link', { name: 'Inspect shared account for shared' }),
    ).toBeVisible()
    // A blocked provider says why, not only that its account is missing, and lists
    // every blocker without a hover.
    await expect(accounts.getByText('Provider disabled')).toBeVisible()
    await expect(
      accounts.getByText('Also blocked: Connect an account for this agent'),
    ).toBeVisible()
    await expect(
      accounts.queryByRole('button', { name: 'Connect account for disabled' }),
    ).toBeNull()
  },
}
// Catches: an accounts outage reading as a workspace with no providers.
export const AgentAccountsUnavailable: Story = {
  render: () => (
    <AgentActivityPage
      workspace={workspace}
      agent={agent}
      providers={null}
      form={form}
      activity={emptyAgentActivity}
      recentActivity={emptyAgentGlimpse}
      idle
    />
  ),
  parameters: { initialEntry: agent.path },
  play: async ({ canvas }) => {
    const accounts = within(canvas.getByRole('region', { name: 'Accounts' }))
    await expect(
      accounts.getByText('Coral could not load the accounts. Reload the page to try again.'),
    ).toBeVisible()
    await expect(accounts.queryByText('This workspace has no providers yet.')).toBeNull()
    await expect(canvas.getByRole('region', { name: 'Connect this agent' })).toBeVisible()
  },
}
// Catches: an archived workspace offering setup for work it cannot start.
export const ArchivedAgentActivity: Story = {
  render: () => (
    <AgentActivityPage
      workspace={archived}
      agent={{ ...agent, accessBlockers: ['Workspace archived'] }}
      providers={agentProviders}
      form={form}
      activity={emptyAgentActivity}
      recentActivity={emptyAgentGlimpse}
      idle
    />
  ),
  parameters: { initialEntry: agent.path },
  play: async ({ canvas }) => {
    await expect(canvas.queryByRole('region', { name: 'Connect this agent' })).toBeNull()
    const accounts = within(canvas.getByRole('region', { name: 'Accounts' }))
    await expect(
      accounts.queryByRole('button', { name: /^(Connect|Reconnect) account for/ }),
    ).toBeNull()
  },
}
// Catches: the complete Activity of an archived workspace's agent hiding the archive.
export const ArchivedAgentFullActivity: Story = {
  ...ArchivedAgentActivity,
  parameters: { initialEntry: `${agent.path}?view=activity` },
  play: async ({ canvas }) => {
    await expect(canvas.getByText('This workspace is archived')).toBeVisible()
  },
}
export const AgentSettings: Story = {
  render: () => (
    <AgentSettingsPage
      workspace={workspace}
      agent={agent}
      registration={registration}
      form={form}
    />
  ),
}
export const RotatedCredential: Story = {
  render: () => (
    <AgentSettingsPage
      workspace={workspace}
      agent={agent}
      registration={registration}
      form={form}
      result={{ error: null, credential: 'storybook-only-not-a-valid-credential' }}
    />
  ),
}
export const DisabledAgent: Story = {
  render: () => (
    <AgentSettingsPage
      workspace={workspace}
      agent={{
        ...agent,
        state: 'disabled',
        accessBlockers: ['Agent disabled', 'API access suspended'],
      }}
      registration={{ ...registration, enabled: false }}
      form={form}
    />
  ),
}
export const ExpiredRegistration: Story = {
  render: () => (
    <AgentSettingsPage
      workspace={workspace}
      agent={{ ...agent, accessBlockers: ['Access expired'] }}
      registration={{ ...registration, expiration: '2020-01-01T00:00:00Z' }}
      form={form}
    />
  ),
  // The pill says the date has passed in words, not only in amber.
  play: async ({ canvas }) => {
    await expect(canvas.getByText('Expired 2020-01-01 00:00 UTC')).toBeVisible()
  },
}
export const ArchivedAgent: Story = {
  render: () => (
    <AgentSettingsPage
      workspace={archived}
      agent={{ ...agent, accessBlockers: ['Workspace archived'] }}
      registration={registration}
      form={form}
    />
  ),
}
export const DeleteConfirmation: Story = {
  ...AgentSettings,
  play: async ({ canvas, canvasElement }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Delete agent' }))
    const dialog = await within(canvasElement.ownerDocument.body).findByRole('dialog', {
      name: 'Delete agent',
    })
    await waitFor(() =>
      expect(within(dialog).getByRole('button', { name: 'Delete agent' })).toBeVisible(),
    )
  },
}

// Members
const sam = { user: 'users/sam', name: 'Sam Rivera', email: 'sam@example.com', local: false }
const organizationPeople = [jordan, sam].map((profile) => ({
  name: `organizationMemberships/${profile.user.slice(6)}`,
  profile,
}))
const organizationMembers = {
  form,
  currentUser: jordan.user,
  nextPage: '',
  memberships: [jordan, alice].map((profile) => ({
    name: `organizationMemberships/${profile.user.slice(6)}`,
    etag: profile.user,
    profile,
    role: profile === jordan ? 'Owner' : 'Member',
    enabled: profile === jordan,
    soleOwnerOf: [] as string[],
  })),
  candidates: organizationPeople,
}
export const OrganizationMembers: Story = {
  render: () => <OrganizationMembersPage {...organizationMembers} />,
  parameters: { chromatic: { viewports: [390, 1280] } },
  play: async ({ canvas }) => {
    await expect(canvas.getByText('Jordan Lee (You)')).toBeVisible()
    await expect(canvas.getByTitle(alice.name)).toBeVisible()
    await expect(canvas.getByRole('cell', { name: alice.email })).toBeVisible()
    await expect(canvas.getByText('Disabled')).toBeVisible()
  },
}
export const FilterOrganizationMembers: Story = {
  render: OrganizationMembers.render,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.click(canvas.getByRole('button', { name: 'Filter members by role: All roles' }))
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'Member' }))
    await shownRows(canvas, ['Actions for Alice Chen'])

    await userEvent.click(canvas.getByRole('button', { name: 'Filter members by role: Member' }))
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'All roles' }))
    // The filter matches an email as well as a name.
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter members' }), 'jordan@')
    await shownRows(canvas, ['Actions for Jordan Lee'])
  },
}
export const OrganizationMembersEmpty: Story = {
  render: () => <OrganizationMembersPage {...organizationMembers} memberships={[]} />,
}
export const RemoveOrganizationMember: Story = {
  ...OrganizationMembers,
  play: async ({ canvas, canvasElement }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Actions for Alice Chen' }))
    const body = within(canvasElement.ownerDocument.body)
    await userEvent.click(await body.findByRole('menuitem', { name: 'Remove from organization' }))
    const dialog = await body.findByRole('dialog', { name: 'Remove from organization' })
    const submit = within(dialog).getByRole('button', {
      name: 'Remove from organization',
    }) as HTMLButtonElement
    const fields = new FormData(submit.form!)
    await expect(fields.get('intent')).toBe('removeOrganizationMember')
    await expect(fields.get('name')).toBe('organizationMemberships/alice')
    await expect(fields.get('etag')).toBe(alice.user)
  },
}
const soleOwner = {
  ...organizationMembers,
  memberships: organizationMembers.memberships.map((m) =>
    m.profile === alice ? { ...m, soleOwnerOf: ['Research', 'Finance archive'] } : m,
  ),
}
async function openRemoveAlice(canvas: ReturnType<typeof within>, root: HTMLElement) {
  await userEvent.click(canvas.getByRole('button', { name: 'Actions for Alice Chen' }))
  const body = within(root.ownerDocument.body)
  await userEvent.click(await body.findByRole('menuitem', { name: 'Remove from organization' }))
  return body.findByRole('dialog', { name: 'Remove from organization' })
}
// Removing the only owner of a workspace needs a person to take it over first.
export const RemoveSoleOwner: Story = {
  render: () => <OrganizationMembersPage {...soleOwner} />,
  play: async ({ canvas, canvasElement }) => {
    const dialog = within(await openRemoveAlice(canvas, canvasElement))
    const names = within(dialog.getByRole('list', { name: 'Workspaces that need a new owner' }))
    await expect(names.getAllByRole('listitem').map((item) => item.textContent)).toEqual([
      'Research',
      'Finance archive',
    ])
    const submit = dialog.getByRole('button', {
      name: 'Remove from organization',
    }) as HTMLButtonElement
    await expect(submit).toBeDisabled()
    await userEvent.click(dialog.getByRole('button', { name: 'New owner' }))
    const body = within(canvasElement.ownerDocument.body)
    // The person you remove cannot take over their own workspaces.
    await expect(body.queryByRole('menuitemradio', { name: /Alice Chen/ })).toBeNull()
    await userEvent.click(await body.findByRole('menuitemradio', { name: 'Sam Rivera' }))
    await waitFor(() => expect(submit).toBeEnabled())
    await expect(new FormData(submit.form!).get('transferTo')).toBe('organizationMemberships/sam')
  },
}
// With no one to take over, the removal cannot go ahead.
export const RemoveSoleOwnerNoCandidate: Story = {
  render: () => <OrganizationMembersPage {...soleOwner} candidates={[]} />,
  play: async ({ canvas, canvasElement }) => {
    const dialog = within(await openRemoveAlice(canvas, canvasElement))
    await waitFor(() =>
      expect(dialog.getByText(/No other enabled member can take over/)).toBeVisible(),
    )
    await expect(dialog.getByRole('button', { name: 'Remove from organization' })).toBeDisabled()
  },
}

// Organization workspaces
const organizationWorkspaces = {
  form,
  currentUser: jordan.user,
  nextPage: '',
  candidates: organizationPeople,
  workspaces: [
    {
      name: 'workspaces/demo',
      displayName: 'Operations team',
      state: 'active' as const,
      etag: 'operations-etag',
      owners: [
        { user: jordan.user, profile: jordan, enabled: true },
        { user: alice.user, profile: alice, enabled: false },
      ],
    },
    {
      name: 'workspaces/research',
      displayName: 'Research',
      state: 'archived' as const,
      etag: 'research-etag',
      owners: [{ user: sam.user, profile: sam, enabled: true }],
    },
  ],
}
async function openWorkspaceActions(
  canvas: ReturnType<typeof within>,
  root: HTMLElement,
  name: string,
) {
  await userEvent.click(canvas.getByRole('button', { name: `Actions for ${name}` }))
  const body = within(root.ownerDocument.body)
  await body.findByRole('menu')
  return body
}
// One active workspace with a disabled owner, and one archived workspace.
export const OrganizationWorkspaces: Story = {
  render: () => <OrganizationWorkspacesPage {...organizationWorkspaces} />,
  parameters: { chromatic: { viewports: [390, 1280] }, initialEntry: '/organization/workspaces' },
  play: async ({ canvas }) => {
    const active = within(canvas.getByRole('row', { name: /Operations team/ }))
    await expect(active.getByText('Active')).toBeVisible()
    // Each owner is one pill. The text marks you and a disabled owner.
    await expect(active.getByText('Jordan Lee (You)')).toBeVisible()
    await expect(active.getByText('Alice Chen · Disabled')).toBeVisible()
    await expect(
      within(canvas.getByRole('row', { name: /Research/ })).getByText('Archived'),
    ).toBeVisible()
  },
}
export const FilterOrganizationWorkspaces: Story = {
  render: OrganizationWorkspaces.render,
  parameters: { initialEntry: '/organization/workspaces' },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.click(
      canvas.getByRole('button', { name: 'Filter workspaces by state: All states' }),
    )
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'Archived' }))
    await shownRows(canvas, ['Actions for Research'])

    await userEvent.click(
      canvas.getByRole('button', { name: 'Filter workspaces by state: Archived' }),
    )
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'All states' }))
    // The filter matches an owner's email as well as the workspace name.
    const filter = canvas.getByRole('searchbox', { name: 'Filter workspaces' })
    await userEvent.type(filter, sam.email)
    await shownRows(canvas, ['Actions for Research'])

    await userEvent.clear(filter)
    await userEvent.type(filter, 'nothing')
    await shownRows(canvas, [])
    await expect(canvas.getByText('No workspaces match “nothing”.')).toBeVisible()
  },
}
// Owners that do not fit on one line fold into a "+N" pill.
export const ManyWorkspaceOwners: Story = {
  render: () => (
    <OrganizationWorkspacesPage
      {...organizationWorkspaces}
      workspaces={[
        {
          ...organizationWorkspaces.workspaces[0],
          owners: Array.from({ length: 12 }, (_, i) => ({
            user: `users/owner-${i}`,
            profile: {
              user: `users/owner-${i}`,
              name: `Owner number ${i}`,
              email: `owner-${i}@example.com`,
              local: false,
            },
            enabled: true,
          })),
        },
      ]}
    />
  ),
  parameters: { chromatic: { viewports: [390, 1280] }, initialEntry: '/organization/workspaces' },
  play: async ({ canvas }) => {
    const row = within(canvas.getByRole('row', { name: /Operations team/ }))
    await expect(await row.findByRole('button', { name: /^\d+ more owners$/ })).toBeVisible()
    await expect(row.queryByText('Owner number 11')).not.toBeInTheDocument()
  },
}
export const OrganizationWorkspacesEmpty: Story = {
  render: () => <OrganizationWorkspacesPage {...organizationWorkspaces} workspaces={[]} />,
  parameters: { initialEntry: '/organization/workspaces' },
}
// An archived workspace offers only its restore.
export const ArchivedWorkspaceActions: Story = {
  ...OrganizationWorkspaces,
  play: async ({ canvas, canvasElement }) => {
    const body = await openWorkspaceActions(canvas, canvasElement, 'Research')
    await expect(body.getAllByRole('menuitem').map((item) => item.textContent)).toEqual([
      'Restore workspace',
    ])
  },
}
const riley = {
  user: 'users/riley',
  name: 'Riley Park',
  email: 'riley@example.com',
  local: false,
}
// Catches: a current owner offered again, or a batch that sends anything but
// the organization membership names of the people you pick.
export const AssignWorkspaceOwners: Story = {
  render: () => (
    <OrganizationWorkspacesPage
      {...organizationWorkspaces}
      candidates={[
        ...organizationPeople,
        { name: 'organizationMemberships/riley', profile: riley },
      ]}
    />
  ),
  parameters: { initialEntry: '/organization/workspaces' },
  play: async ({ canvas, canvasElement }) => {
    const body = await openWorkspaceActions(canvas, canvasElement, 'Operations team')
    await expect(body.getAllByRole('menuitem').map((item) => item.textContent)).toEqual([
      'Assign owners',
      'Archive workspace',
    ])
    await userEvent.click(body.getByRole('menuitem', { name: 'Assign owners' }))
    const dialog = within(await body.findByRole('dialog', { name: 'Assign owners' }))
    await expect(dialog.getByRole('button', { name: 'Assign owner' })).toBeDisabled()
    await userEvent.click(dialog.getByRole('combobox', { name: 'People' }))
    // Jordan already owns the workspace.
    await expect((await body.findAllByRole('option')).map((option) => option.textContent)).toEqual([
      'Sam Rivera (sam@example.com)',
      'Riley Park (riley@example.com)',
    ])
    // Only organization members can own a workspace, so a typed email is not offered.
    await userEvent.keyboard('new@example.com')
    await expect(body.queryByRole('option', { name: /new@example\.com/ })).not.toBeInTheDocument()
    await userEvent.clear(dialog.getByRole('combobox', { name: 'People' }))
    await userEvent.keyboard('{Escape}')
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    await pickPerson(dialog, canvasElement, /Riley Park/)
    const submit = dialog.getByRole('button', { name: 'Assign 2 owners' }) as HTMLButtonElement
    await waitFor(() => expect(submit).toBeEnabled())
    const fields = new FormData(submit.form!)
    await expect(fields.get('intent')).toBe('assignOwner')
    await expect(fields.get('name')).toBe('workspaces/demo')
    await expect(fields.getAll('owner')).toEqual([
      'organizationMemberships/sam',
      'organizationMemberships/riley',
    ])
  },
}

export const OwnerMembers: Story = {
  render: () => <WorkspaceMembersPage {...members} />,
  play: async ({ canvas }) => {
    // The breadcrumb names the page; there is no page heading.
    const breadcrumb = within(canvas.getByRole('navigation', { name: 'breadcrumb' }))
    await expect(breadcrumb.getByText('Members')).toBeVisible()
    await expect(canvas.getByRole('table', { name: 'Members' })).toBeVisible()
  },
}
export const MembersAsMember: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      workspace={member}
      memberships={[]}
      ownMembership={ownMembership}
    />
  ),
}
export const ArchivedMembers: Story = {
  render: () => <WorkspaceMembersPage {...members} workspace={archived} />,
}
// A disabled person keeps their membership and role. The State column says so in words.
export const DisabledOwner: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      memberships={members.memberships.map((m) => ({ ...m, enabled: m.user !== alice.user }))}
      owners={owners.map((o) => ({ ...o, enabled: o.user !== alice.user }))}
    />
  ),
  play: async ({ canvas }) => {
    const row = canvas.getByRole('row', { name: /Alice Chen/ })
    await expect(within(row).getByText('Owner')).toBeVisible()
    await expect(within(row).getByText('Disabled')).toBeVisible()
    await expect(canvas.getAllByText('Disabled')).toHaveLength(1)
  },
}
// A member sees the owners on record, with a disabled owner marked.
export const DisabledOwnerAsMember: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      workspace={member}
      memberships={[]}
      ownMembership={{ ...ownMembership, role: 'member' }}
      owners={[{ user: alice.user, profile: alice, enabled: false }]}
    />
  ),
  play: async ({ canvas }) => {
    await expect(
      within(canvas.getByRole('row', { name: /Alice Chen/ })).getByText('Disabled'),
    ).toBeVisible()
  },
}
export const StaleConfiguration: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      result={{
        error: 'This resource changed. Reload before trying again.',
        credential: null,
      }}
    />
  ),
}
// Two people share an email, and two share a name. Each is a choice of its own.
export const AddMembersLookalikes: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      candidates={[
        { user: 'users/sam', name: 'Sam Rivera', email: 'sam@example.com', local: false },
        { user: 'users/sam2', name: 'Sam Rivera', email: 'sam@other.com', local: false },
        { user: 'users/priya', name: 'Priya Natarajan', email: 'priya@example.com', local: false },
        { user: 'users/pat', name: 'Pat Natarajan', email: 'priya@example.com', local: false },
      ]}
    />
  ),
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await userEvent.click(dialog.getByRole('combobox', { name: 'People' }))
  },
}
async function openAddMembers(canvas: ReturnType<typeof within>, root: HTMLElement) {
  await userEvent.click(canvas.getByRole('button', { name: 'Add members' }))
  return within(await within(root.ownerDocument.body).findByRole('dialog', { name: 'Add members' }))
}
/**
 * Waits for the rows a filter shows. The filter applies through a deferred
 * value, so the rows can lag a render.
 */
function shownRows(canvas: ReturnType<typeof within>, expected: string[]) {
  return waitFor(() =>
    expect(
      canvas
        .queryAllByRole('button', { name: /^Actions for / })
        .map((button: HTMLElement) => button.getAttribute('aria-label')),
    ).toEqual(expected),
  )
}
/** Picks one person, then closes the list, which hides the rest of the dialog while open. */
async function pickPerson(dialog: ReturnType<typeof within>, root: HTMLElement, name: RegExp) {
  const input = dialog.getByRole('combobox')
  await userEvent.click(input)
  await userEvent.click(await within(root.ownerDocument.body).findByRole('option', { name }))
  await userEvent.keyboard('{Escape}')
  await expect(input).toHaveAttribute('aria-expanded', 'false')
}
export const AddMemberDialog: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await userEvent.click(dialog.getByRole('combobox', { name: 'People' }))
  },
}
// Catches: the picker sending names instead of emails, or a label that does
// not count the people and role it adds.
export const AddMembersSelection: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await expect(dialog.getByRole('combobox', { name: 'People' })).toBeEnabled()
    await expect(dialog.getByRole('button', { name: 'Add member' })).toBeDisabled()
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    await pickPerson(dialog, canvasElement, /ops-bot@example\.com/)
    await userEvent.click(dialog.getByRole('button', { name: 'Role' }))
    await userEvent.click(
      await within(canvasElement.ownerDocument.body).findByRole('menuitemradio', {
        name: 'Owner',
      }),
    )
    const submit = dialog.getByRole('button', { name: 'Add 2 owners' }) as HTMLButtonElement
    const fields = new FormData(submit.form!)
    await expect(fields.getAll('email')).toEqual(['sam@example.com', 'ops-bot@example.com'])
    await expect(fields.get('role')).toBe('owner')
  },
}
// Catches: a workspace owner who cannot list the organization losing the way
// to add anyone, or an email typed in full that the picker will not take.
export const AddMemberByEmail: Story = {
  render: () => <WorkspaceMembersPage {...members} candidates={null} />,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await userEvent.type(dialog.getByRole('combobox', { name: 'People' }), 'person@example.com')
    await userEvent.keyboard('{Enter}')
    const submit = dialog.getByRole('button', { name: 'Add member' }) as HTMLButtonElement
    await expect(new FormData(submit.form!).getAll('email')).toEqual(['person@example.com'])
  },
}
// Catches: an organization owner unable to add someone the list does not hold,
// or a typed email that duplicates a listed person.
export const AddMembersTypedAndPicked: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    const input = dialog.getByRole('combobox', { name: 'People' })
    await userEvent.type(input, 'new@example.com')
    await userEvent.keyboard('{Enter}')
    await userEvent.type(input, 'PRIYA@example.com')
    await userEvent.click(
      await within(canvasElement.ownerDocument.body).findByRole('option', {
        name: /Priya Natarajan/,
      }),
    )
    await userEvent.keyboard('{Escape}')
    const submit = dialog.getByRole('button', { name: 'Add 3 members' }) as HTMLButtonElement
    await expect(new FormData(submit.form!).getAll('email')).toEqual([
      'sam@example.com',
      'new@example.com',
      'priya@example.com',
    ])
  },
}
// Catches: typed text wiped as the list hides between matches, so only the
// first typed email lands. It takes typing at a person's pace for the list to close.
export const AddMembersTypedTwice: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    const slow = userEvent.setup({ delay: 50 })
    const input = dialog.getByRole('combobox', { name: 'People' })
    await slow.type(input, 'one@example.com{Enter}')
    await slow.type(input, 'two@example.com{Enter}')
    await userEvent.keyboard('{Escape}')
    const submit = dialog.getByRole('button', { name: 'Add 2 members' }) as HTMLButtonElement
    await expect(new FormData(submit.form!).getAll('email')).toEqual([
      'one@example.com',
      'two@example.com',
    ])
  },
}
// Catches: a picked email typed again offered as a new add instead of the
// picked item, so Enter cannot remove it.
export const AddMembersTypedPicked: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    const slow = userEvent.setup({ delay: 50 })
    const input = dialog.getByRole('combobox', { name: 'People' })
    await slow.type(input, 'new@example.com{Enter}')
    await slow.type(input, 'NEW@example.com')
    const body = within(canvasElement.ownerDocument.body)
    await expect(await body.findByRole('option', { name: 'new@example.com' })).toBeVisible()
    await expect(body.queryByRole('option', { name: /^Add / })).toBeNull()
    await userEvent.keyboard('{Enter}{Escape}')
    await expect(dialog.getByRole('button', { name: 'Add member' })).toBeDisabled()
  },
}
export const AddMemberError: Story = {
  ...OwnerMembers,
  parameters: {
    dialogError: 'More than one person matches this email. Coral granted no access.',
  },
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    await userEvent.click(dialog.getByRole('button', { name: 'Add member' }))
    await expect(
      await dialog.findByText('More than one person matches this email. Coral granted no access.'),
    ).toBeVisible()
  },
}
// Catches: a retry after a partial add sending the people Coral already added.
export const AddMembersPartialFailure: Story = {
  ...OwnerMembers,
  parameters: {
    dialogError: 'Coral added Sam Rivera. ops-bot@example.com: Coral could not add this person.',
    dialogRefused: ['ops-bot@example.com'],
  },
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    await pickPerson(dialog, canvasElement, /ops-bot@example\.com/)
    await userEvent.click(dialog.getByRole('button', { name: 'Add 2 members' }))
    const retry = (await dialog.findByRole('button', { name: 'Add member' })) as HTMLButtonElement
    await expect(new FormData(retry.form!).getAll('email')).toEqual(['ops-bot@example.com'])
  },
}
export const AddMemberPending: Story = {
  ...OwnerMembers,
  parameters: { dialogPending: true },
  play: async ({ canvas, canvasElement }) => {
    const dialog = await openAddMembers(canvas, canvasElement)
    await pickPerson(dialog, canvasElement, /Sam Rivera/)
    const add = dialog.getByRole('button', { name: 'Add member' })
    await userEvent.click(add)
    await waitFor(() => expect(add).toHaveAttribute('aria-busy', 'true'))
    await expect(add).toHaveAttribute('aria-disabled', 'true')
    await expect(add).toHaveAccessibleName('Add member')
  },
}
export const MembershipRoleSelection: Story = {
  ...OwnerMembers,
  play: async ({ canvas, canvasElement }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Actions for Alice Chen' }))
    const body = within(canvasElement.ownerDocument.body)
    await userEvent.click(await body.findByRole('menuitem', { name: 'Change role' }))
    await userEvent.click(await body.findByRole('button', { name: 'Role for Alice Chen' }))
    await userEvent.click(await body.findByRole('menuitemradio', { name: 'Member' }))
    const save = (await body.findByRole('button', {
      name: 'Save role',
    })) as HTMLButtonElement
    await expect(new FormData(save.form!).get('role')).toBe('member')
  },
}
export const PaginatedMembers: Story = {
  render: () => (
    <WorkspaceMembersPage {...members} nextMembers="next-members" nextOwners="next-owners" />
  ),
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('link', { name: 'Next members' })).toHaveAttribute(
      'href',
      '/workspaces/demo/members?members=next-members',
    )
    await expect(canvas.getByRole('link', { name: 'Next owners' })).toHaveAttribute(
      'href',
      '/workspaces/demo/members?owners=next-owners',
    )
  },
}
export const ProfileFallbacks: Story = {
  render: () => (
    <WorkspaceMembersPage
      {...members}
      memberships={[
        ...members.memberships,
        ...[
          {
            user: 'users/duplicate',
            name: 'Alice Chen',
            email: 'another-alice@example.com',
            local: false,
          },
          {
            user: 'users/email-only',
            name: '',
            email: 'person-with-a-very-long-email-address@very-long-organization.example.com',
            local: false,
          },
          {
            user: 'users/name-only',
            name: 'Person with a very long name and no email address',
            email: '',
            local: false,
          },
          { user: 'users/unnamed', name: '', email: '', local: false },
        ].map((profile) => ({
          name: `workspaces/demo/memberships/${profile.user.slice(6)}`,
          user: profile.user,
          profile,
          role: 'member',
          enabled: true,
          etag: profile.user,
        })),
      ]}
    />
  ),
  parameters: { chromatic: { viewports: [390, 1280] } },
  // Catches: a person with no name repeating their email as the name.
  play: async ({ canvas }) => {
    const row = canvas.getByRole('row', { name: /person-with-a-very-long-email-address/ })
    await expect(within(row).getByText('(no name)')).toBeVisible()
    await expect(within(row).getAllByText(/person-with-a-very-long-email-address/)).toHaveLength(1)
  },
}
export const LocalUser: Story = {
  render: () => (
    <WorkspaceOverview
      {...overview}
      workspace={{
        ...workspace,
        currentUser: 'users/coral:local',
        profile: { user: 'users/coral:local', name: 'Local user', email: '', local: true },
      }}
      owners={null}
    />
  ),
}

// Themes and widths
export const LightOverview: Story = {
  ...OwnerOverview,
  globals: { backgrounds: { value: 'light' } },
}
export const DarkOverview: Story = {
  ...OwnerOverview,
  globals: { backgrounds: { value: 'dark' } },
}
export const LightCredential: Story = {
  ...AgentCreated,
  globals: { backgrounds: { value: 'light' } },
}
export const DarkCredential: Story = {
  ...AgentCreated,
  globals: { backgrounds: { value: 'dark' } },
}
export const NarrowOverview: Story = {
  ...OwnerOverview,
  parameters: { chromatic: { viewports: [390] } },
}
export const NarrowAgent: Story = {
  ...DisabledAgent,
  parameters: { chromatic: { viewports: [390] } },
}
