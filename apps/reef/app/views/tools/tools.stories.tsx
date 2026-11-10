import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, screen, userEvent, waitFor, within } from 'storybook/test'
import githubIcon from '../../../recipes/github/github.svg'
import { CORAL_ICONS, PRIVATE_ICONS, SHARED_ICONS } from '@/views/providers/coral-icons'
import type { ProviderToolDetailView, ProviderView, ToolView } from '@/views/providers/types'
import { selectionKey } from '@/views/providers/selection'
import { ToolsPage } from '@/views/tools/tools-page'
import type {
  PrivateToolDetailView,
  PublishReviewResult,
  SharedToolDetailView,
} from '@/views/tools/types'
import { catalogRecipes, form, stripeRecipe, workspace } from '@/views/providers/story-fixtures'

const meta = {
  title: 'Management/Tools',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story, context) => {
      const action = () =>
        context.parameters.dialogPending
          ? new Promise(() => {})
          : {
              error: context.parameters.dialogError ?? null,
              credential: null,
            }
      const Stub = createRoutesStub([
        // The publish dialog reads its review here and sends its publish here.
        {
          path: '/workspaces/demo/shared-tools/publish',
          loader: () => context.parameters.review ?? null,
          action,
        },
        { path: '*', Component: Story, action },
      ])
      return (
        <div style={{ height: '100vh' }}>
          <Stub initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo/tools']} />
        </div>
      )
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

const member = { ...workspace, effectiveRole: 'member' }

function op(
  provider: string,
  relativePath: string,
  group: string,
  extra: Partial<ToolView> = {},
): ToolView {
  const readOnly = extra.hints?.readOnly === true
  return {
    key: relativePath,
    relativePath,
    // Mirrors the domain default for these fixtures, which carry no HTTP method:
    // read-only is enabled, anything else needs approval.
    hints: {},
    policy: readOnly ? 'enabled' : 'approval_required',
    available: true,
    reasons: [],
    group,
    ...extra,
  }
}
const githubOps = (provider: string) => [
  op(provider, 'issues.list', 'issues', {
    hints: { readOnly: true, destructive: false, idempotent: true, openWorld: false },
  }),
  op(provider, 'issues.create', 'issues', {
    hints: { readOnly: false, destructive: false, openWorld: true },
  }),
  op(provider, 'issues.close', 'issues', {
    hints: { readOnly: false, destructive: true, idempotent: true },
    available: false,
    reasons: ['The request body uses a content type Coral cannot send faithfully.'],
  }),
  op(provider, 'repos.get', 'repos', {
    hints: { readOnly: true },
  }),
]
const createIssue: ProviderToolDetailView = {
  kind: 'provider',
  relativePath: 'issues.create',
  summary: 'Create an issue',
  description:
    'Create an issue in the selected repository.\n\n- Set `title` to name the issue.\n- Set `body` to describe the issue.\n\nYou need **write access** to add labels or assignees.',
}
const github: ProviderView = {
  id: 'prov-github',
  displayName: 'GitHub',
  namespace: 'github',
  callRoot: 'coral.providers.github',
  kind: 'added',
  account: 'Individual accounts',
  blockers: [],
  personalAccountAction: null,
  searchIndex: null,
  readyCount: 3,
  totalCount: 4,
  etag: 'prov-etag',
  path: '/workspaces/demo/providers/prov-github',
  enabled: true,
  tools: githubOps('prov-github'),
  icons: { light: githubIcon },
}
const weather: ProviderView = {
  id: 'prov-weather',
  displayName: 'Weather',
  namespace: 'weather',
  callRoot: 'coral.providers.weather',
  kind: 'added',
  account: 'No account',
  blockers: [],
  personalAccountAction: null,
  searchIndex: null,
  readyCount: 1,
  totalCount: 1,
  etag: 'prov-etag-2',
  path: '/workspaces/demo/providers/prov-weather',
  enabled: true,
  tools: [
    {
      key: 'getForecast',
      relativePath: 'getForecast',
      hints: {},
      policy: 'approval_required',
      available: true,
      reasons: [],
      group: '',
    },
  ],
}
const blocked: ProviderView = {
  ...github,
  id: 'prov-hr',
  displayName: 'HR system',
  namespace: 'hr',
  icons: undefined,
  account: 'One shared account',
  enabled: false,
  blockers: ['Provider disabled'],
  path: '/workspaces/demo/providers/prov-hr',
  tools: githubOps('prov-hr'),
}
// Coral's Built-in Provider, which leads the rail under Coral's mark.
const coralTools = [
  'addProvider',
  'listProviders',
  'getProvider',
  'getPolicy',
  'updateProvider',
].map((key) => ({
  ...weather.tools[0]!,
  key,
  relativePath: key,
  // Read-only, so they start enabled.
  ...(['listProviders', 'getProvider', 'getPolicy'].includes(key) && {
    hints: { readOnly: true, idempotent: true },
    policy: 'enabled' as const,
  }),
}))
const coral: ProviderView = {
  ...weather,
  id: 'prov-coral',
  displayName: 'Coral',
  namespace: '',
  callRoot: 'coral',
  kind: 'coral',
  icons: CORAL_ICONS,
  path: '/workspaces/demo/providers/prov-coral',
  readyCount: coralTools.length,
  totalCount: coralTools.length,
  tools: coralTools,
}
const treeProps = {
  workspace,
  recipes: catalogRecipes,
  providers: [github, weather, blocked],
  selectedProvider: null,
  selectedTool: null,
  form,
}

export const TreeAddProviderLauncher: Story = {
  // Another tab is current, and the toolbar still ends with the Add button.
  render: () => <ToolsPage {...treeProps} selectedProvider="prov-weather" />,
  play: async ({ canvas }) => {
    const add = within(canvas.getByRole('main')).getByRole('button', { name: 'Add provider' })
    await expect(add).toBeVisible()
    await userEvent.click(add)
    const dialog = within(await screen.findByRole('dialog'))
    // The dialog fades in.
    await waitFor(() =>
      expect(dialog.getByRole('searchbox', { name: 'Search the catalog' })).toBeVisible(),
    )
    // Typing goes straight to the search.
    await waitFor(() =>
      expect(dialog.getByRole('searchbox', { name: 'Search the catalog' })).toHaveFocus(),
    )
    await expect(dialog.getByRole('button', { name: 'Add GitHub API' })).toBeVisible()
  },
}
// A rail filter that matches no installed provider points to the catalog.
export const TreeFilterFindsCatalogProvider: Story = {
  render: () => <ToolsPage {...treeProps} />,
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByLabelText('Filter providers'), 'stripe')
  },
}
// A long name breaks inside the message and truncates on the button.
export const TreeFilterFindsLongCatalogName: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      recipes={[
        ...catalogRecipes,
        {
          ...stripeRecipe,
          id: 'enterprise_resource_planning_and_procurement_suite_for_global_operations',
          displayName:
            'Enterprise Resource Planning and Procurement Suite for Global Operations and Multinational Finance Teams',
        },
      ]}
    />
  ),
  play: async ({ canvas }) => {
    await userEvent.type(
      canvas.getByLabelText('Filter providers'),
      'enterprise_resource_planning_and_procurement_suite_for_global_operations',
    )
  },
}
export const TreeFilterFindsNothing: Story = {
  render: () => <ToolsPage {...treeProps} />,
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByLabelText('Filter providers'), 'notion')
  },
}
export const EmptyTree: Story = {
  render: () => <ToolsPage {...treeProps} providers={[]} />,
}
export const EmptyWorkspace: Story = {
  render: () => <ToolsPage {...treeProps} providers={[]} />,
}
export const TreeNothingSelected: Story = {
  render: () => <ToolsPage {...treeProps} />,
  play: async ({ canvas }) => {
    // The checkbox's click target reaches the row's edge, its top and bottom,
    // and into the gap before the next column.
    const [box] = canvas.getAllByRole('checkbox', { name: 'Select issues.list' })
    const row = box.closest<HTMLElement>('[role="row"]')!.getBoundingClientRect()
    const name = canvas.getAllByRole('link', { name: 'issues.list' })[0]!.closest('[role="cell"]')!
    const gap = name.getBoundingClientRect().left - 2
    for (const [x, y] of [
      [row.left + 1, row.top + 1],
      [row.left + 1, row.bottom - 2],
      [gap, row.top + row.height / 2],
    ] as const) {
      const hit = box.ownerDocument.elementFromPoint(x, y)
      await expect(hit?.closest('label')).toBe(box.closest('label'))
    }
  },
}
/** A month of daily calls, oldest first. */
const usage = (perDay: (day: number) => number) => {
  const daily = Array.from({ length: 30 }, (_, day) => perDay(day))
  const calls = daily.reduce((sum, count) => sum + count, 0)
  const agent = Math.round(calls * 0.7)
  const callers = [
    {
      key: 'agent',
      label: 'PR review agent',
      agentPath: '/workspaces/demo/agents/review',
      calls: agent,
    },
    { key: 'you', label: 'You', calls: calls - agent },
  ].filter((caller) => caller.calls > 0)
  return { calls, daily, callers }
}
const toolUsage = {
  days: 30,
  tools: {
    [selectionKey('prov-github', 'issues.list')]: usage((day) => 300 + ((day * 37) % 160)),
    [selectionKey('prov-github', 'issues.create')]: usage((day) => (day > 20 ? 30 + day : 2)),
    [selectionKey('prov-weather', 'getForecast')]: usage((day) => (day % 9 === 0 ? 1 : 0)),
  },
}
// The Usage column: each tool's calls as a bar against the busiest tool's.
export const TreeToolCalls: Story = {
  render: () => <ToolsPage {...treeProps} toolCalls={toolUsage} />,
}
// The drawer's usage: the total, each day as a bar, and who called it.
export const TreeToolUsage: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={createIssue}
      toolCalls={toolUsage}
    />
  ),
}
// A provider heading's status: a destructive tool that runs without approval on
// GitHub, and unavailable tools on HR system.
export const TreeProviderStatus: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[
        {
          ...github,
          // issues.close becomes available below, so every GitHub tool is ready.
          readyCount: 4,
          tools: github.tools.map((tool) =>
            tool.relativePath === 'issues.close'
              ? { ...tool, available: true, policy: 'enabled' }
              : tool.relativePath === 'repos.get'
                ? { ...tool, policy: 'disabled' }
                : tool,
          ),
        },
        weather,
        blocked,
      ]}
    />
  ),
}
export const TreeCollapsedProvider: Story = {
  render: () => <ToolsPage {...treeProps} />,
  play: async ({ canvas }) => {
    // Collapsing a provider leaves its heading's content where it was.
    const heading = canvas.getAllByRole('rowgroup', { name: 'GitHub' })[0]!
    const label = () => within(heading).getAllByRole('cell')[1]!.getBoundingClientRect()
    const before = label()
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse GitHub' }))
    await expect(canvas.getByRole('button', { name: 'Expand GitHub' })).toBeVisible()
    await expect(label().top).toBe(before.top)
    await expect(label().height).toBe(before.height)
  },
}
export const TreeCollapseAll: Story = {
  render: () => <ToolsPage {...treeProps} />,
  play: async ({ canvas }) => {
    // The header's toggle lines up with the provider headings' toggles.
    const left = (name: string) => canvas.getByRole('button', { name }).getBoundingClientRect().left
    await expect(left('Collapse all')).toBe(left('Collapse GitHub'))
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse all' }))
    await expect(canvas.getByRole('button', { name: 'Expand all' })).toBeVisible()
  },
}
// Enough providers that their collapsed headings still scroll.
const manyProviders: ProviderView[] = Array.from({ length: 30 }, (_, index) => ({
  ...weather,
  id: `prov-many-${index + 1}`,
  displayName: `API ${String(index + 1).padStart(2, '0')}`,
  namespace: `api${index + 1}`,
  path: `/workspaces/demo/providers/prov-many-${index + 1}`,
}))
export const TreeExpandAllFromDownTheTable: Story = {
  render: () => <ToolsPage {...treeProps} providers={manyProviders} />,
  play: async ({ canvas }) => {
    // Expanding all from down the table returns it to its top.
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse all' }))
    const table = canvas.getByRole('table', { name: 'Tools' })
    table.scrollTo({ top: table.scrollHeight })
    await waitFor(() => expect(table.scrollTop).toBeGreaterThan(0))
    await userEvent.click(canvas.getByRole('button', { name: 'Expand all' }))
    await waitFor(() => expect(table.scrollTop).toBe(0))
  },
}
export const TreeWithBuiltinProvider: Story = {
  render: () => <ToolsPage {...treeProps} providers={[github, coral, weather]} />,
}
export const TreeProviderSelected: Story = {
  render: () => <ToolsPage {...treeProps} selectedProvider="prov-weather" />,
}
// A catalog large enough that the table virtualizes: only the tools and tool
// groups near the scroll position mount, while every provider heading does.
const largeProviders: ProviderView[] = Array.from({ length: 6 }, (_, providerIndex) => {
  const id = `prov-large-${providerIndex + 1}`
  const groups = ['issues', 'pulls', 'repos', 'users']
  const tools = Array.from({ length: 200 }, (__, index) =>
    op(id, `${groups[index % 4]}.op${index + 1}`, groups[index % 4]!, {
      hints: index % 3 === 0 ? { readOnly: true } : { readOnly: false },
    }),
  )
  return {
    ...github,
    id,
    displayName: `API ${providerIndex + 1}`,
    namespace: `api${providerIndex + 1}`,
    icons: undefined,
    path: `/workspaces/demo/providers/${id}`,
    readyCount: tools.length,
    totalCount: tools.length,
    tools,
  }
})
export const TreeLargeCatalog: Story = {
  render: () => <ToolsPage {...treeProps} providers={largeProviders} />,
}
// Your own private tools, and the agent Nightly sync's, one at a path of yours.
const privateTools: ProviderView = {
  ...weather,
  id: 'prov-private',
  displayName: 'Private',
  namespace: '',
  callRoot: 'coral.private',
  kind: 'private',
  icons: PRIVATE_ICONS,
  path: '/workspaces/demo/tools/private',
  readyCount: 4,
  totalCount: 4,
  // Each row shows the name inside the namespace that heads its group.
  tools: [
    op('prov-private', 'finance.monthlyTotals', 'finance', {
      policy: 'enabled',
      nameInGroup: 'monthlyTotals',
    }),
    op('prov-private', 'finance.refundCustomer', 'finance', { nameInGroup: 'refundCustomer' }),
    {
      ...op('prov-private', 'finance.monthlyTotals', 'finance', {
        policy: 'disabled',
        nameInGroup: 'monthlyTotals',
      }),
      key: 'agent/finance.monthlyTotals',
      agent: { id: 'nightly', label: 'Nightly sync', path: '/workspaces/demo/agents/nightly' },
    },
    op('prov-private', 'linear.completedSince', 'linear', {
      policy: 'enabled',
      nameInGroup: 'completedSince',
    }),
  ],
}
const monthlyTotals: PrivateToolDetailView = {
  kind: 'private',
  agent: null,
  relativePath: 'finance.monthlyTotals',
  description: 'Sums the **paid** invoices of one month, by currency.',
  declaration:
    'declare function monthlyTotals(input: { month: string }): Promise<Record<string, number>>',
  source: [
    'export default async function monthlyTotals({ month }: { month: string }) {',
    '  const { data } = await coral.providers.stripe.invoices.list({ status: "paid", month })',
    '  const totals: Record<string, number> = {}',
    '  for (const invoice of data) totals[invoice.currency] = (totals[invoice.currency] ?? 0) + invoice.total',
    '  return totals',
    '}',
  ].join('\n'),
  diagnostics: [
    {
      category: 'warning',
      code: 7006,
      message: "Parameter 'invoice' implicitly has an 'any' type.",
      line: 4,
      column: 14,
    },
  ],
  diagnosticsTruncated: false,
  savedAt: '2026-11-09T14:27:03.000Z',
}
// A provider of 200 tools, collapsed: its heading keeps its count, and the
// groups after it come into view. Coral leads, then the private tools.
export const TreeLargeProviderCollapsed: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[privateTools, weather, ...largeProviders.slice(0, 2), coral]}
    />
  ),
  play: async ({ canvas }) => {
    await userEvent.click(await canvas.findByRole('button', { name: 'Collapse API 1' }))
  },
}
// The private page for an owner: your own private tools and each agent's.
export const TreePrivateTools: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[coral, github, privateTools]}
      selectedProvider="prov-private"
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/private' },
}
// A member reads the policy of their own private tools, and changes none.
export const TreePrivateToolsAsMember: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      workspace={member}
      providers={[
        coral,
        github,
        { ...privateTools, tools: privateTools.tools.filter((tool) => !tool.agent) },
      ]}
      selectedProvider="prov-private"
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/private' },
}
// The private page with no private tool yet.
export const TreePrivateToolsEmpty: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[coral, github, { ...privateTools, tools: [], readyCount: 0, totalCount: 0 }]}
      selectedProvider="prov-private"
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/private' },
}
// One of your own private tools, as Coral saved it.
export const TreePrivateToolSelected: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[coral, github, privateTools]}
      selectedProvider="prov-private"
      selectedTool="finance.monthlyTotals"
      selectedToolDetail={monthlyTotals}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals' },
}
// An agent's private tool, which only an owner opens.
export const TreeAgentPrivateToolSelected: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[coral, github, privateTools]}
      selectedProvider="prov-private"
      selectedTool="finance.monthlyTotals"
      selectedToolAgent="nightly"
      selectedToolDetail={{ ...monthlyTotals, agent: 'nightly' }}
    />
  ),
  parameters: {
    initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals/agents/nightly',
  },
}
// A member reads that only an owner can publish.
export const TreePrivateToolSelectedAsMember: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      workspace={member}
      providers={[
        coral,
        github,
        { ...privateTools, tools: privateTools.tools.filter((tool) => !tool.agent) },
      ]}
      selectedProvider="prov-private"
      selectedTool="finance.monthlyTotals"
      selectedToolDetail={monthlyTotals}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals' },
}

// The workspace's shared tools, which every member reads.
const sharedTools: ProviderView = {
  ...weather,
  id: 'prov-shared',
  displayName: 'Shared',
  namespace: '',
  callRoot: 'coral.shared',
  kind: 'shared',
  icons: SHARED_ICONS,
  path: '/workspaces/demo/tools/shared',
  readyCount: 3,
  totalCount: 3,
  tools: [
    op('prov-shared', 'finance.monthlyTotals', 'finance', {
      policy: 'enabled',
      nameInGroup: 'monthlyTotals',
    }),
    op('prov-shared', 'finance.refundCustomer', 'finance', { nameInGroup: 'refundCustomer' }),
    op('prov-shared', 'linear.completedSince', 'linear', {
      policy: 'enabled',
      nameInGroup: 'completedSince',
    }),
  ],
}
const ada = { user: 'users/ada', name: 'Ada Lovelace', email: 'ada@example.test', local: false }
const firstTotals = [
  'export default async function monthlyTotals({ month }: { month: string }) {',
  '  const { data } = await coral.providers.stripe.invoices.list({ month })',
  '  let total = 0',
  '  for (const invoice of data) total += invoice.total',
  '  return total',
  '}',
].join('\n')
const sharedTotals: SharedToolDetailView = {
  kind: 'shared',
  relativePath: 'finance.monthlyTotals',
  description: 'Sums the invoices of one month.',
  declaration: 'export default function monthlyTotals(input: { month: string }): Promise<number>;',
  source: firstTotals,
  diagnostics: [],
  diagnosticsTruncated: false,
  revision: 3,
  publisher: ada,
  publishedAt: '2026-11-09T14:27:03.000Z',
  revisions: [
    {
      revision: 3,
      description: 'Sums the invoices of one month.',
      publisher: ada,
      publishedAt: '2026-11-09T14:27:03.000Z',
      restoredFrom: 1,
    },
    {
      revision: 2,
      description: 'Sums the paid invoices of one month, by currency.',
      publisher: { ...workspace.profile },
      publishedAt: '2026-11-05T10:02:41.000Z',
      restoredFrom: null,
    },
    {
      revision: 1,
      description: 'Sums the invoices of one month.',
      // Whoever published it is no longer an owner of the workspace.
      publisher: null,
      publishedAt: '2026-10-28T08:15:00.000Z',
      restoredFrom: null,
    },
  ],
  events: [
    { id: 'e3', action: 'published', revision: 3, actor: ada, at: '2026-11-09T14:27:03.000Z' },
    {
      id: 'e2',
      action: 'published',
      revision: 2,
      actor: { ...workspace.profile },
      at: '2026-11-05T10:02:41.000Z',
    },
    { id: 'e1', action: 'published', revision: 1, actor: null, at: '2026-10-28T08:15:00.000Z' },
  ],
  eventsTruncated: false,
}
const sharedPage = {
  ...treeProps,
  providers: [coral, github, sharedTools, privateTools],
  selectedProvider: 'prov-shared',
}
const sharedDrawer = {
  ...sharedPage,
  selectedTool: 'finance.monthlyTotals',
  selectedToolDetail: sharedTotals,
}
const sharedDrawerEntry = '/workspaces/demo/tools/shared/finance.monthlyTotals'
// The shared page: the tools an owner published for the whole workspace.
export const TreeSharedTools: Story = {
  render: () => <ToolsPage {...sharedPage} />,
  parameters: { initialEntry: '/workspaces/demo/tools/shared' },
}
// The shared page before any owner published a tool.
export const TreeSharedToolsEmpty: Story = {
  render: () => (
    <ToolsPage
      {...sharedPage}
      providers={[coral, github, { ...sharedTools, tools: [], readyCount: 0, totalCount: 0 }]}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/tools/shared' },
}
// One shared tool: who published it, its revision, and its source.
export const TreeSharedToolSelected: Story = {
  render: () => <ToolsPage {...sharedDrawer} />,
  parameters: { initialEntry: sharedDrawerEntry },
}
// Every revision, newest first. An owner restores any but the current one.
export const TreeSharedToolRevisions: Story = {
  ...TreeSharedToolSelected,
  play: async () => {
    await userEvent.click(await screen.findByRole('tab', { name: 'Revisions' }))
  },
}
// Each publish, with who did it. A person who is no longer an owner has no name.
export const TreeSharedToolHistory: Story = {
  ...TreeSharedToolSelected,
  play: async () => {
    await userEvent.click(await screen.findByRole('tab', { name: 'History' }))
  },
}
// A member reads a shared tool, and restores or removes nothing.
export const TreeSharedToolRevisionsAsMember: Story = {
  render: () => <ToolsPage {...sharedDrawer} workspace={member} />,
  parameters: { initialEntry: sharedDrawerEntry },
  play: TreeSharedToolRevisions.play,
}
// Coral refuses the removal while another shared tool calls this one, and names it.
export const TreeSharedToolRemovalRefused: Story = {
  ...TreeSharedToolSelected,
  parameters: {
    initialEntry: sharedDrawerEntry,
    dialogError:
      'The shared tool coral.shared.finance.yearlyTotals calls this one. Change or remove that shared tool first.',
  },
  play: async () => {
    await userEvent.click(await screen.findByRole('button', { name: 'Remove shared tool' }))
    const dialog = await screen.findByRole('dialog', { name: 'Remove shared tool' })
    await userEvent.click(within(dialog).getByRole('button', { name: 'Remove shared tool' }))
    await within(dialog).findByText(/calls this one/)
  },
}

const firstReview: PublishReviewResult = {
  refusal: null,
  review: {
    origin: { kind: 'private', path: 'finance.monthlyTotals', agent: null },
    path: 'finance.monthlyTotals',
    description: monthlyTotals.description,
    source: monthlyTotals.source,
    declaration:
      'export default function monthlyTotals(input: { month: string }): Promise<Record<string, number>>;',
    diagnostics: monthlyTotals.diagnostics,
    diagnosticsTruncated: false,
    previous: null,
    calledTools: ['coral.providers.stripe.invoices.list', 'coral.shared.finance.currencies'],
    sharedTools: [{ path: 'finance.currencies', revision: 4 }],
    runtimeBuiltPaths: [],
    defaultPolicy: null,
  },
}
const openPublish = async () => {
  await userEvent.click(await screen.findByRole('button', { name: 'Publish' }))
  await screen.findByRole('heading', { name: 'Publish shared tool' })
}
// The review of a first publish: the path, the reminder, the full source,
// what it calls, the diagnostics, and the policy to start with.
export const PublishFirstRevision: Story = {
  ...TreePrivateToolSelected,
  parameters: {
    initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals',
    review: firstReview,
  },
  play: async () => {
    await openPublish()
    await screen.findByRole('button', { name: 'Publish revision 1' })
  },
}
// The review of a later publish: the source and the declaration as diffs
// against the revision it replaces, and a path the source builds at runtime.
export const PublishNextRevision: Story = {
  ...TreePrivateToolSelected,
  parameters: {
    initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals',
    review: {
      refusal: null,
      review: {
        ...firstReview.review!,
        previous: {
          revision: 3,
          description: sharedTotals.description,
          source: sharedTotals.source,
          declaration: sharedTotals.declaration,
        },
        runtimeBuiltPaths: ['coral.providers.stripe'],
        defaultPolicy: 'approval_required',
      },
    } satisfies PublishReviewResult,
  },
  play: async () => {
    await openPublish()
    await screen.findByRole('button', { name: 'Publish revision 4' })
    await screen.findByRole('group', { name: 'Source changes' })
    await screen.findByRole('group', { name: 'Declaration changes' })
  },
}
// Coral refuses the review of a path, and the owner enters another.
export const PublishPathRefused: Story = {
  ...TreePrivateToolSelected,
  parameters: {
    initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals',
    review: {
      review: null,
      path: 'finance.monthlyTotals',
      refusal: 'Another shared tool uses this path, or a part of it. Enter another path.',
    } satisfies PublishReviewResult,
  },
  play: async () => {
    await openPublish()
    await screen.findByText(/Enter another path/)
  },
}
// Another owner published first: the dialog says so and shows the new review.
export const PublishRefused: Story = {
  ...TreePrivateToolSelected,
  parameters: {
    initialEntry: '/workspaces/demo/tools/private/finance.monthlyTotals',
    review: firstReview,
    dialogError: 'Another owner published this shared tool first. Review the changes again.',
  },
  play: async () => {
    await openPublish()
    await userEvent.click(await screen.findByRole('button', { name: 'Publish revision 1' }))
    await screen.findByText(/published this shared tool first/)
  },
}
// A restore reviews the earlier revision against the current one, at a fixed path.
export const RestoreRevision: Story = {
  ...TreeSharedToolSelected,
  parameters: {
    initialEntry: sharedDrawerEntry,
    review: {
      refusal: null,
      review: {
        ...firstReview.review!,
        origin: { kind: 'restore', revision: 2 },
        description: 'Sums the paid invoices of one month, by currency.',
        previous: {
          revision: 3,
          description: sharedTotals.description,
          source: sharedTotals.source,
          declaration: sharedTotals.declaration,
        },
        defaultPolicy: 'enabled',
      },
    } satisfies PublishReviewResult,
  },
  play: async () => {
    await userEvent.click(await screen.findByRole('tab', { name: 'Revisions' }))
    await userEvent.click(await screen.findByRole('button', { name: 'Restore revision 2' }))
    await screen.findByRole('button', { name: 'Publish revision 4' })
  },
}

// Too many tool groups to mount: only those near the scroll position do, and
// the one a reader scrolls through still pins, at the same scroll position.
const manyGroups: ProviderView = {
  ...github,
  tools: Array.from({ length: 900 }, (_, index) => {
    const group = `group${String(Math.floor(index / 3)).padStart(3, '0')}`
    return op('prov-github', `${group}.op${index}`, group)
  }),
  readyCount: 900,
  totalCount: 900,
}
export const TreeManyToolGroups: Story = {
  render: () => <ToolsPage {...treeProps} providers={[manyGroups]} />,
  play: async ({ canvasElement }) => {
    const table = within(canvasElement).getByRole('table', { name: 'Tools' })
    const headings = () => [
      ...table.querySelectorAll<HTMLElement>('[data-pinned-heading][aria-level="2"]'),
    ]
    const height = table.scrollHeight
    for (const top of [4000, 12000, 2000]) {
      table.scrollTop = top
      await waitFor(() => {
        const provider = table.querySelector('[data-pinned-heading][aria-level="1"]')!
        const slot = provider.getBoundingClientRect().bottom
        // A mounted group always spans the slot, so no spacer shows under it.
        const spanning = headings().find((heading) => {
          const group = heading.parentElement!.getBoundingClientRect()
          return group.top <= slot && group.bottom >= slot
        })
        expect(spanning).toBeDefined()
        // Its heading pins there, until the end of its group pushes it out.
        const heading = spanning!.getBoundingClientRect()
        const end = spanning!.parentElement!.getBoundingClientRect().bottom
        expect(Math.abs(heading.top - Math.min(slot, end - heading.height))).toBeLessThanOrEqual(1)
      })
      // Rows correct their estimates as they mount, by a pixel or so each.
      await expect(Math.abs(table.scrollTop - top)).toBeLessThan(40)
      await expect(Math.abs(table.scrollHeight - height)).toBeLessThan(400)
    }
  },
}
export const TreeAuthenticatedProviderSelected: Story = {
  render: () => <ToolsPage {...treeProps} selectedProvider="prov-github" />,
}
// Labels show only hints the provider declared. A row shows those declared
// true; the drawer also shows those declared false. Undeclared shows nothing.
export const TreeToolSelected: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={createIssue}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const row = (name: string) =>
      within(
        canvas
          // The open drawer is modal, so the table sits outside the accessibility tree.
          .getAllByRole('checkbox', { name: `Select ${name}`, hidden: true })[0]!
          .closest<HTMLElement>('[role="row"]')!,
      )
    const labels = ['Read-only', 'Destructive', 'Idempotent', 'Open world']
    const shown = (scope: ReturnType<typeof within>) =>
      labels.filter((label) => scope.queryByText(label, { exact: true }))
    await expect(shown(row('issues.list'))).toEqual(['Read-only', 'Idempotent'])
    await expect(shown(row('issues.create'))).toEqual(['Open world'])
    // The drawer renders in a portal outside the canvas.
    const drawer = within(
      await within(canvasElement.ownerDocument.body).findByRole('dialog', {
        name: 'issues.create',
      }),
    )
    // The drawer shows only the hints declared true, and the policy under its own heading.
    await expect(drawer.getByText('Open world', { exact: true })).toBeVisible()
    await expect(drawer.queryByText(/read-only|destructive|idempotent/i)).toBeNull()
    await expect(drawer.getByRole('heading', { name: 'Policy' })).toBeVisible()
  },
}
// Weather declares no hints, so its row shows no labels.
export const TreeToolWithoutDescription: Story = {
  render: () => (
    <ToolsPage {...treeProps} selectedProvider="prov-weather" selectedTool="getForecast" />
  ),
  play: async ({ canvasElement }) => {
    // The open drawer is modal, so the table sits outside the accessibility tree.
    const row = within(
      within(canvasElement)
        .getByRole('checkbox', { name: 'Select getForecast', hidden: true })
        .closest<HTMLElement>('[role="row"]')!,
    )
    for (const label of ['Read-only', 'Destructive', 'Idempotent', 'Open world'])
      await expect(row.queryByText(label, { exact: true })).toBeNull()
  },
}
export const TreeToolWithSummaryAsDescription: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[github]}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={{ ...createIssue, description: createIssue.summary }}
    />
  ),
}
export const TreeUnavailableToolSelected: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedTool="issues.close"
      selectedToolDetail={{
        kind: 'provider',
        relativePath: 'issues.close',
        summary: 'Close an issue',
        description: '',
      }}
    />
  ),
}
// Search failed to index GitHub's active Generation. Its heading and its
// tools say so; Weather, which search indexed, says nothing.
export const TreeSearchIndexFailed: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[
        { ...github, searchIndex: { state: 'failed', reason: 'Indexing took too long.' } },
        weather,
      ]}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={createIssue}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getAllByText('Not in search', { exact: true })).toHaveLength(1)
    const drawer = within(
      await within(canvasElement.ownerDocument.body).findByRole('dialog', {
        name: 'issues.create',
      }),
    )
    await expect(drawer.getByText(/Search cannot find this tool\./)).toBeVisible()
  },
}
// The pill's tooltip names the error the failed attempt recorded.
export const TreeSearchIndexFailureReason: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[
        { ...github, searchIndex: { state: 'failed', reason: 'Indexing took too long.' } },
        weather,
      ]}
    />
  ),
  play: async ({ canvasElement }) => {
    await userEvent.hover(within(canvasElement).getByText('Not in search', { exact: true }))
    await waitFor(() => expect(screen.getByText(/Reason: Indexing took too long\./)).toBeVisible())
  },
}
// Search has not finished indexing GitHub's active Generation yet.
export const TreeSearchIndexPending: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[{ ...github, searchIndex: { state: 'pending' } }, weather]}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={createIssue}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getAllByText('Indexing…', { exact: true })).toHaveLength(1)
    await expect(canvas.queryByText('Not in search', { exact: true })).toBeNull()
    const drawer = within(
      await within(canvasElement.ownerDocument.body).findByRole('dialog', {
        name: 'issues.create',
      }),
    )
    await expect(drawer.getByText(/Search cannot find this tool yet\./)).toBeVisible()
  },
}
export const TreeAsMember: Story = {
  render: () => <ToolsPage {...treeProps} workspace={member} selectedProvider="prov-github" />,
}
export const TreeAtPhoneWidth: Story = {
  render: () => <ToolsPage {...treeProps} />,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}
export const TreeAtPhoneWidthSelected: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedTool="issues.create"
      selectedToolDetail={createIssue}
    />
  ),
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}
export const TreeAfterAdded: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-github"
      result={{
        error: null,
        credential: null,
        message: 'Added. Check the tools before you use them.',
      }}
    />
  ),
}
export const TreeConflict: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      selectedProvider="prov-weather"
      result={{
        error: 'This provider changed. Reload the page. Review the changes before you try again.',
        credential: null,
      }}
    />
  ),
}

const catalogViewports = { chromatic: { viewports: [390, 1440] } }
async function showEmptyCatalogFilter(canvasElement: HTMLElement, label: string) {
  const canvas = within(canvasElement)
  await userEvent.type(canvas.getByRole('searchbox', { name: label }), 'no match')
  // Every provider leaves the table, and one status says nothing matches.
  await expect(canvas.getByText('No tools match “no match”.')).toBeVisible()
}

// Coral refuses a row's policy change: the row keeps its policy and says why.
export const TreePolicyRefused: Story = {
  ...TreeNothingSelected,
  parameters: { dialogError: 'Coral refused the change.' },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // Both fixture providers list repos.get; use the first provider's row.
    const [select] = canvas.getAllByRole('checkbox', { name: 'Select repos.get' })
    const row = within(select!.closest<HTMLElement>('[role="row"]')!)
    await userEvent.click(row.getByRole('radio', { name: 'Block' }))
    await expect(await row.findByText('Not saved')).toBeVisible()
  },
}

// A bulk save in flight: the selection controls lock and the summary says so,
// while rows outside the selection still take their own changes.
export const TreeBulkPending: Story = {
  ...TreeNothingSelected,
  parameters: { dialogPending: true },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.click(canvas.getByRole('checkbox', { name: 'Select GitHub tools' }))
    // The "Bulk policy" form names the group; the trigger reads as its value.
    const bulk = canvas.getByRole('form', { name: 'Bulk policy' })
    await userEvent.click(within(bulk).getByRole('button', { name: 'Ask' }))
    // The menu renders in a portal outside the canvas.
    const body = within(canvasElement.ownerDocument.body)
    await userEvent.click(await body.findByRole('menuitemradio', { name: 'Block' }))
    await userEvent.click(within(bulk).getByRole('button', { name: 'Apply' }))
    await expect(canvas.getByRole('checkbox', { name: 'Select GitHub tools' })).toBeDisabled()
  },
}

// A provider with no tools yet keeps its heading and its settings.
export const TreeEmptyProvider: Story = {
  render: () => (
    <ToolsPage
      {...treeProps}
      providers={[
        github,
        {
          ...weather,
          id: 'prov-empty',
          displayName: 'Empty',
          namespace: 'empty',
          path: '/workspaces/demo/providers/prov-empty',
          tools: [],
          readyCount: 0,
          totalCount: 0,
        },
      ]}
    />
  ),
  parameters: catalogViewports,
}

export const TreeEmptyFilter: Story = {
  render: () => <ToolsPage {...treeProps} />,
  parameters: catalogViewports,
  play: ({ canvasElement }) => showEmptyCatalogFilter(canvasElement, 'Filter tools'),
}
export const TreeFilteredGroups: Story = {
  render: () => <ToolsPage {...treeProps} />,
  parameters: catalogViewports,
  // The filtered table, with each match in a tool's name in bold.
  // tools-page.dom.test.tsx owns what the filter matches.
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter tools' }), 'ISSUES')
    await expect(canvas.getAllByRole('row', { name: /issues\.close/ }).length).toBeGreaterThan(0)
  },
}

export const TreeProviderDisabled: Story = {
  render: () => <ToolsPage {...treeProps} selectedProvider="prov-hr" />,
}
