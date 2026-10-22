import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, screen, userEvent, waitFor, within } from 'storybook/test'

import type { CreationAttempt, UpdateAttempt } from '@/lib/provider-publication-attempt/model'
import type { SourceChoice } from '@/lib/provider-recipes/choice'
import githubIcon from '../../../recipes/github/github.svg'
import stripeIcon from '../../../recipes/stripe/stripe.svg'
import { StepperPage } from './stepper-page'
import type { ProviderView, OperationView, ReviewOperationView, SetupView } from './types'
import { UpdatePage } from './update-page'
import { EditConfigurationPage } from './edit-configuration-page'
import { WorkspaceProvidersPage } from './workspace-providers-page'

const form = { csrf: 'storybook-csrf', requestId: 'storybook-request' }
const jordan = {
  user: 'users/jordan',
  name: 'Jordan Lee',
  email: 'jordan@example.com',
  local: false,
}
export const workspace = {
  name: 'workspaces/demo',
  displayName: 'Finance ops',
  state: 'active',
  etag: 'workspace-etag',
  effectiveRole: 'owner',
  currentUser: 'users/jordan',
  profile: jordan,
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  path: '/workspaces/demo',
}
const member = { ...workspace, effectiveRole: 'member' }

function op(
  provider: string,
  base: string,
  relativePath: string,
  summary: string,
  group: string,
  extra: Partial<OperationView> = {},
): OperationView {
  const readOnly = extra.hints?.readOnly === true
  return {
    // The management resource name is keyed by the provider id, not its namespace.
    name: `providers/${provider}/operations/${relativePath}`,
    relativePath,
    callPath: base ? `coral.providers.${base}.${relativePath}` : relativePath,
    summary,
    description: '',
    // Mirrors the domain default for these fixtures, which carry no HTTP method:
    // read-only is enabled, anything else needs approval.
    hints: {},
    policy: readOnly ? 'enabled' : 'approval_required',
    available: true,
    reasons: [],
    group,
    path: `/workspaces/demo/providers/${provider}/ops/${relativePath}`,
    ...extra,
  }
}
const githubOps = (provider: string, base: string) => [
  op(provider, base, 'issues.list', 'List issues in a repository', 'issues', {
    description:
      'List issues that you can access in the repository.\n\nUse `state` to include closed issues. Results include pull requests.',
    hints: { readOnly: true, destructive: false, idempotent: true, openWorld: false },
  }),
  op(provider, base, 'issues.create', 'Create an issue', 'issues', {
    description:
      'Create an issue in the selected repository.\n\n- Set `title` to name the issue.\n- Set `body` to describe the issue.\n\nYou need **write access** to add labels or assignees.',
    hints: { readOnly: false, destructive: false, openWorld: true },
  }),
  op(provider, base, 'issues.close', 'Close an issue', 'issues', {
    hints: { readOnly: false, destructive: true, idempotent: true },
    available: false,
    reasons: ['The request body uses a content type Coral cannot send faithfully.'],
  }),
  op(provider, base, 'repos.get', 'Get a repository', 'repos', {
    hints: { readOnly: true },
  }),
]
const github: ProviderView = {
  id: 'prov-github',
  displayName: 'GitHub',
  namespace: 'github',
  builtin: false,
  account: 'Individual accounts',
  blockers: [],
  personalAccountAction: null,
  searchIndex: null,
  readyCount: 3,
  totalCount: 4,
  etag: 'prov-etag',
  path: '/workspaces/demo/providers/prov-github',
  enabled: true,
  operations: githubOps('prov-github', 'github'),
}
const weather: ProviderView = {
  id: 'prov-weather',
  displayName: 'Weather',
  namespace: 'weather',
  builtin: false,
  account: 'No account',
  blockers: [],
  personalAccountAction: null,
  searchIndex: null,
  readyCount: 1,
  totalCount: 1,
  etag: 'prov-etag-2',
  path: '/workspaces/demo/providers/prov-weather',
  enabled: true,
  operations: [
    {
      name: 'providers/prov-weather/operations/getForecast',
      relativePath: 'getForecast',
      callPath: 'coral.providers.weather.getForecast',
      summary: '',
      description: '',
      hints: {},
      policy: 'approval_required',
      available: true,
      reasons: [],
      group: '',
      path: '/workspaces/demo/providers/prov-weather/ops/getForecast',
    },
  ],
}
const blocked: ProviderView = {
  ...github,
  id: 'prov-hr',
  displayName: 'HR system',
  namespace: 'hr',
  account: 'One shared account',
  enabled: false,
  blockers: ['Provider disabled'],
  path: '/workspaces/demo/providers/prov-hr',
  operations: githubOps('prov-hr', 'hr'),
}
// Coral's Built-in Provider, which leads the rail under Coral's mark.
const coralOperations = ['createProvider', 'listProviders', 'getProvider', 'updateProvider'].map(
  (key) => ({
    ...weather.operations[0]!,
    name: `providers/prov-coral/operations/${key}`,
    relativePath: key,
    callPath: `coral.workspace.${key}`,
    path: `/workspaces/demo/providers/prov-coral/ops/${key}`,
    // Read-only, so they start enabled.
    ...((key === 'listProviders' || key === 'getProvider') && {
      hints: { readOnly: true, idempotent: true },
      policy: 'enabled' as const,
    }),
  }),
)
const coral: ProviderView = {
  ...weather,
  id: 'prov-coral',
  displayName: 'Coral',
  namespace: '',
  builtin: true,
  path: '/workspaces/demo/providers/prov-coral',
  readyCount: coralOperations.length,
  totalCount: coralOperations.length,
  operations: coralOperations,
}
const meta = {
  title: 'Management/Providers',
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
                },
        },
      ])
      return (
        <div style={{ height: '100vh' }}>
          <Stub
            initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo/providers']}
          />
        </div>
      )
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

// Recipes as the catalog receives them from the bundled library.
const githubRecipe: SourceChoice = {
  id: 'github',
  displayName: 'GitHub',
  description: 'Issues, pull requests, repositories, actions and releases from GitHub.',
  kind: 'openapi',
  url: 'https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.yaml',
  icons: { light: githubIcon },
}
const stripeRecipe: SourceChoice = {
  id: 'stripe',
  displayName: 'Stripe',
  description: 'Charges, customers, subscriptions, invoices, payouts and balance from Stripe.',
  kind: 'openapi',
  url: 'https://raw.githubusercontent.com/stripe/openapi/refs/heads/master/latest/openapi.spec3.yaml',
  icons: { light: stripeIcon },
}
// A second recipe for one service: the catalog offers both on the GitHub card.
const githubMcpRecipe: SourceChoice = {
  id: 'github_mcp',
  service: 'github',
  displayName: 'GitHub',
  description: "GitHub's hosted MCP server.",
  kind: 'mcp',
  url: 'https://api.githubcopilot.com/mcp/',
  icons: { light: githubIcon },
}
const catalogRecipes = [githubRecipe, githubMcpRecipe, stripeRecipe]
const treeProps = {
  workspace,
  recipes: catalogRecipes,
  providers: [github, weather, blocked],
  selectedProvider: null,
  selectedOperation: null,
  form,
}

export const TreeAddProviderLauncher: Story = {
  // Another tab is current, and the toolbar still ends with the Add button.
  render: () => <WorkspaceProvidersPage {...treeProps} selectedProvider="prov-weather" />,
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
export const EmptyTree: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} providers={[]} />,
}
export const EmptyWorkspace: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} providers={[]} />,
}
export const TreeNothingSelected: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} />,
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
export const TreeCollapsedProvider: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} />,
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
  render: () => <WorkspaceProvidersPage {...treeProps} />,
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
  render: () => <WorkspaceProvidersPage {...treeProps} providers={manyProviders} />,
  play: async ({ canvas }) => {
    // Expanding all from down the table returns it to its top.
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse all' }))
    const table = canvas.getByRole('table', { name: 'Operations' })
    table.scrollTo({ top: table.scrollHeight })
    await waitFor(() => expect(table.scrollTop).toBeGreaterThan(0))
    await userEvent.click(canvas.getByRole('button', { name: 'Expand all' }))
    await waitFor(() => expect(table.scrollTop).toBe(0))
  },
}
export const TreeWithBuiltinProvider: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} providers={[github, coral, weather]} />,
}
export const TreeProviderSelected: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} selectedProvider="prov-weather" />,
}
// A catalog large enough that the table virtualizes: only the operations near
// the scroll position mount, while every provider and group heading does.
const largeProviders: ProviderView[] = Array.from({ length: 6 }, (_, providerIndex) => {
  const id = `prov-large-${providerIndex + 1}`
  const groups = ['issues', 'pulls', 'repos', 'users']
  const operations = Array.from({ length: 200 }, (__, index) =>
    op(
      id,
      `api${providerIndex + 1}`,
      `${groups[index % 4]}.op${index + 1}`,
      '',
      groups[index % 4]!,
      {
        hints: index % 3 === 0 ? { readOnly: true } : { readOnly: false },
      },
    ),
  )
  return {
    ...github,
    id,
    displayName: `API ${providerIndex + 1}`,
    namespace: `api${providerIndex + 1}`,
    path: `/workspaces/demo/providers/${id}`,
    readyCount: operations.length,
    totalCount: operations.length,
    operations,
  }
})
export const TreeLargeCatalog: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} providers={largeProviders} />,
}
export const TreeAuthenticatedProviderSelected: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} selectedProvider="prov-github" />,
}
// Labels show only hints the provider declared. A row shows those declared
// true; the drawer also shows those declared false. Undeclared shows nothing.
export const TreeOperationSelected: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedOperation="issues.create"
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
    const labels = ['Read-only', 'Destructive', 'Idempotent', 'Open world', 'Not read-only']
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
    await expect(drawer.getByText('Not read-only', { exact: true })).toBeVisible()
    await expect(drawer.getByText('Not destructive', { exact: true })).toBeVisible()
    await expect(drawer.getByText('Open world', { exact: true })).toBeVisible()
    await expect(drawer.queryByText(/idempotent/i)).toBeNull()
  },
}
// Weather declares no hints, so its row shows no labels.
export const TreeOperationWithoutDescription: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      selectedProvider="prov-weather"
      selectedOperation="getForecast"
    />
  ),
  play: async ({ canvasElement }) => {
    // The open drawer is modal, so the table sits outside the accessibility tree.
    const row = within(
      within(canvasElement)
        .getByRole('checkbox', { name: 'Select getForecast', hidden: true })
        .closest<HTMLElement>('[role="row"]')!,
    )
    for (const label of ['Read-only', 'Destructive', 'Idempotent', 'Open world', 'Not read-only'])
      await expect(row.queryByText(label, { exact: true })).toBeNull()
  },
}
export const TreeOperationWithSummaryAsDescription: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      providers={[
        {
          ...github,
          operations: github.operations.map((o) => ({ ...o, description: o.summary })),
        },
      ]}
      selectedProvider="prov-github"
      selectedOperation="issues.create"
    />
  ),
}
export const TreeUnavailableOperationSelected: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedOperation="issues.close"
    />
  ),
}
// Search failed to index GitHub's active Generation. Its heading and its
// operations say so; Weather, which search indexed, says nothing.
export const TreeSearchIndexFailed: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      providers={[
        { ...github, searchIndex: { state: 'failed', reason: 'Indexing took too long.' } },
        weather,
      ]}
      selectedProvider="prov-github"
      selectedOperation="issues.create"
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
    await expect(drawer.getByText(/Search cannot find this operation\./)).toBeVisible()
  },
}
// The pill's tooltip names the error the failed attempt recorded.
export const TreeSearchIndexFailureReason: Story = {
  render: () => (
    <WorkspaceProvidersPage
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
    <WorkspaceProvidersPage
      {...treeProps}
      providers={[{ ...github, searchIndex: { state: 'pending' } }, weather]}
      selectedProvider="prov-github"
      selectedOperation="issues.create"
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
    await expect(drawer.getByText(/Search cannot find this operation yet\./)).toBeVisible()
  },
}
export const TreeAsMember: Story = {
  render: () => (
    <WorkspaceProvidersPage {...treeProps} workspace={member} selectedProvider="prov-github" />
  ),
}
export const TreeAtPhoneWidth: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} />,
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}
export const TreeAtPhoneWidthSelected: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      selectedProvider="prov-github"
      selectedOperation="issues.create"
    />
  ),
  globals: { viewport: { value: 'mobile1', isRotated: false } },
}
export const ConfigurationMissingAuthentication: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={configurationSetup} />,
  play: async ({ canvas }) => {
    // The current crumb names the provider being added and titles the page.
    await expect(canvas.getByRole('navigation', { name: 'breadcrumb' })).toHaveTextContent(
      'ProvidersAdd Example API',
    )
    await expect(canvas.getByRole('heading', { level: 1 })).toHaveTextContent('Add Example API')
    await expect(canvas.getByLabelText('Display name')).toHaveValue(
      configurationSetup.configuration!.values.displayName,
    )
    await expect(canvas.getByLabelText('Namespace')).toBeVisible()
    await expect(canvas.queryByRole('list', { name: 'Add provider' })).not.toBeInTheDocument()
  },
}
export const ConfigurationServers: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={configurationSetup} />,
}
export const ConfigurationUnusedSchemes: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: { ...configurationSetup.configuration!, authenticationStatus: 'unused' },
      }}
    />
  ),
}
export const ConfigurationAmbiguousAuthentication: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'ambiguous',
          documentDisplayName: 'Example API',
        },
      }}
    />
  ),
}
export const ConfigurationUnsupportedAuthentication: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'unsupported',
          documentDisplayName: 'Example API',
        },
      }}
    />
  ),
}
export const ConfigurationExplicitAnonymous: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'explicit_anonymous',
          documentDisplayName: 'Example API',
          values: { ...configurationSetup.configuration!.values, authentication: 'none' },
        },
      }}
    />
  ),
}
export const ConfigurationDeclaredBearer: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'supported',
          documentDisplayName: 'Example API',
          values: { ...configurationSetup.configuration!.values, authentication: 'bearer' },
        },
      }}
    />
  ),
}
async function chooseSignIn(canvasElement: HTMLElement, method: string) {
  const field = within(canvasElement).getByRole('group', { name: 'Sign-in' })
  await userEvent.click(within(field).getByRole('button'))
  const body = within(canvasElement.ownerDocument.body)
  await userEvent.click(await body.findByRole('menuitemradio', { name: method }))
}
// The declared method is a suggestion: the person can override it. Review then
// says the method changed from the document.
export const ConfigurationOverridesDocument: Story = {
  ...ConfigurationExplicitAnonymous,
  play: async ({ canvas, canvasElement }) => {
    await chooseSignIn(canvasElement, 'Bearer token')
    await expect(
      within(canvas.getByRole('group', { name: 'Sign-in' })).getByRole('button'),
    ).toHaveTextContent('Bearer token')
  },
}
export const ConfigurationFieldError: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={configurationSetup}
      result={{
        error: 'Select a server and complete its declared variables.',
        credential: null,
      }}
    />
  ),
}
export const ConfigurationOAuth: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'supported',
          documentDisplayName: 'Example API',
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'oauth',
            authorizationUrl: 'https://accounts.example.test/authorize',
            tokenUrl: 'https://accounts.example.test/token',
            scopes: ['read'],
          },
        },
      }}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const field = canvasElement.getBoundingClientRect()
    const expectLabelToFit = async (name: string) => {
      const label = canvas.getByRole('radio', { name }).closest('label')!
      const bounds = label.getBoundingClientRect()
      await expect(bounds.left).toBeGreaterThanOrEqual(field.left)
      await expect(bounds.right).toBeLessThanOrEqual(field.right)
    }
    for (const name of ['Public client', 'Secret in Basic header', 'Secret in request body'])
      await expectLabelToFit(name)
    await userEvent.click(canvas.getByRole('radio', { name: 'Secret in request body' }))
    await expect(canvas.getByLabelText('Client secret', { exact: true })).toBeVisible()
    await expectLabelToFit('Use S256 PKCE')
    await expectLabelToFit('Disable PKCE for this service')
  },
}
// A recipe's hint, such as Slack's app creation link, sits below the client ID.
export const ConfigurationOAuthClientHint: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      recipeConfiguration={{
        id: 'slack',
        inputs: [],
        suggestedMethods: ['oauth'],
        values: {},
        clientConfigHintMarkdown:
          '[Create a Slack app](https://api.slack.com/apps?new_app=1&manifest_yaml=display_information%3A%0A), then copy its client ID and client secret here.',
      }}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          authenticationStatus: 'supported',
          documentDisplayName: 'Slack',
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'oauth',
            authorizationUrl: 'https://slack.com/oauth/v2_user/authorize',
            tokenUrl: 'https://slack.com/api/oauth.v2.user.access',
            scopes: ['chat:write'],
          },
        },
      }}
    />
  ),
  play: async ({ canvasElement }) => {
    const link = within(canvasElement).getByRole('link', { name: 'Create a Slack app' })
    await expect(link).toHaveAttribute('target', '_blank')
    await expect(link).toHaveAttribute(
      'href',
      'https://api.slack.com/apps?new_app=1&manifest_yaml=display_information%3A%0A',
    )
  },
}
export const ConfigurationOAuthFieldErrors: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'oauth',
            authorizationUrl: 'https://accounts.example.test/authorize',
            tokenUrl: 'not-a-url',
            scopes: ['read', 'write'],
          },
          errors: {
            'authentication.token_url':
              'Enter a valid token URL without credentials or OAuth request parameters.',
          },
        },
      }}
    />
  ),
}

// Microsoft requests many scopes that share a URL prefix on each of two hosts.
export const ConfigurationOAuthManyScopes: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'oauth',
            authorizationUrl: 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize',
            tokenUrl: 'https://login.microsoftonline.com/common/oauth2/v2.0/token',
            scopes: [
              'offline_access',
              ...[
                'User.Read',
                'User.ReadBasic.All',
                'People.Read',
                'Mail.ReadWrite',
                'Mail.ReadWrite.Shared',
                'Mail.Send',
                'Mail.Send.Shared',
                'MailboxSettings.ReadWrite',
                'Calendars.ReadWrite',
                'Calendars.ReadWrite.Shared',
                'Contacts.ReadWrite',
                'Files.ReadWrite.All',
                'Sites.ReadWrite.All',
                'Sites.Manage.All',
                'Tasks.ReadWrite',
                'Chat.Create',
                'Chat.ReadWrite',
                'ChatMessage.Send',
                'Team.ReadBasic.All',
                'Channel.ReadBasic.All',
              ].map((scope) => `https://graph.microsoft.com/${scope}`),
              'https://outlook.office.com/IMAP.AccessAsUser.All',
              'https://outlook.office.com/SMTP.Send',
            ],
          },
        },
      }}
    />
  ),
}

export const ConfigurationRejectedOAuthDraft: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={configurationSetup}
      result={{
        error: null,
        credential: null,
        configurationErrors: {
          'application.parameters': 'Enter each authorization parameter once.',
        },
        configurationDraft: {
          values: {
            ...configurationSetup.configuration!.values,
            displayName: 'My edited provider',
            authentication: 'oauth',
            authorizationUrl: 'https://accounts.example.test/authorize',
            tokenUrl: 'https://accounts.example.test/token',
            scopes: ['read', 'write'],
          },
          namespace: 'edited_namespace',
          application: {
            mode: 'manual',
            clientId: 'edited-client',
            authentication: 'post',
            pkce: 'disabled',
            parameters: 'prompt=select_account&prompt=consent&access_type=offline',
          },
        },
      }}
    />
  ),
  parameters: { chromatic: { viewports: [390, 1440] } },
  play: async ({ canvas }) => {
    await expect(canvas.getByLabelText('Display name', { exact: true })).toHaveValue(
      'My edited provider',
    )
    await expect(canvas.getByLabelText('Namespace', { exact: true })).toHaveValue(
      'edited_namespace',
    )
    await expect(canvas.getByLabelText('Client ID', { exact: true })).toHaveValue('edited-client')
    await expect(canvas.getByLabelText('Client secret', { exact: true })).toHaveValue('')
    await expect(
      canvas.getByLabelText('Additional authorization parameters', { exact: true }),
    ).toHaveValue('prompt=select_account&prompt=consent&access_type=offline')
    await expect(
      canvas.getByText('Enter each authorization parameter once.', { exact: true }),
    ).toBeVisible()
  },
}

export const ConfigurationResolvedDestinations: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        state: 'ready',
        configuration: {
          ...configurationSetup.configuration!,
          revision: '1',
          destinations: {
            users: 'https://api.eu.example.test/v1',
            logs: 'https://logs.us.logs.test/',
          },
          unavailableOperations: [
            { key: 'events', reasons: ['review the selected server and its declared variables'] },
            {
              key: 'stream',
              reasons: ['an OpenAPI callback cannot be invoked by the v1 HTTP binding'],
            },
          ],
        },
      }}
    />
  ),
}

export const TreeAfterAdded: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      selectedProvider="prov-github"
      result={{
        error: null,
        credential: null,
        message: 'Added. Check the operations before you use them.',
      }}
    />
  ),
}
export const TreeConflict: Story = {
  render: () => (
    <WorkspaceProvidersPage
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
  // Each provider section says it has no match.
  for (const status of canvas.getAllByText('No operations match “no match”.')) {
    await expect(status).toBeVisible()
  }
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
    await userEvent.click(row.getByRole('radio', { name: 'Disabled' }))
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
    await userEvent.click(canvas.getByRole('checkbox', { name: 'Select GitHub operations' }))
    // The "Bulk policy" form names the group; the trigger reads as its value.
    const bulk = canvas.getByRole('form', { name: 'Bulk policy' })
    await userEvent.click(within(bulk).getByRole('button', { name: 'Needs approval' }))
    // The menu renders in a portal outside the canvas.
    const body = within(canvasElement.ownerDocument.body)
    await userEvent.click(await body.findByRole('menuitemradio', { name: 'Disabled' }))
    await userEvent.click(within(bulk).getByRole('button', { name: 'Apply' }))
    await expect(canvas.getByRole('checkbox', { name: 'Select GitHub operations' })).toBeDisabled()
  },
}

// A provider with no operations yet keeps its heading and its settings.
export const TreeEmptyProvider: Story = {
  render: () => (
    <WorkspaceProvidersPage
      {...treeProps}
      providers={[
        github,
        {
          ...weather,
          id: 'prov-empty',
          displayName: 'Empty',
          namespace: 'empty',
          path: '/workspaces/demo/providers/prov-empty',
          operations: [],
          readyCount: 0,
          totalCount: 0,
        },
      ]}
    />
  ),
  parameters: catalogViewports,
}

export const TreeEmptyFilter: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} />,
  parameters: catalogViewports,
  play: ({ canvasElement }) => showEmptyCatalogFilter(canvasElement, 'Filter operations'),
}
export const TreeFilteredGroups: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} />,
  parameters: catalogViewports,
  // The filtered table, with each match in an operation's name in bold.
  // workspace-providers-page.dom.test.tsx owns what the filter matches.
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter operations' }), 'ISSUES')
    await expect(canvas.getAllByRole('row', { name: /issues\.close/ }).length).toBeGreaterThan(0)
  },
}

// Add provider
const setupReady: SetupView = {
  configuration: {
    revision: '1',
    confirmed: true,
    authenticationMethods: ['none', 'bearer', 'header', 'oauth'],
    discoveryAuthorizationRequired: false,
    automaticClientAvailable: false,
    issuer: '',
    resource: '',
    differences: [],
    interruptsAccess: null,
    updating: false,
    values: {
      displayName: 'Example API',
      authentication: 'none',
      headers: [],
      parameters: [],
      basic: {
        username: { source: 'literal', value: '' },
        password: { source: 'literal', value: '' },
      },
      credentialFields: {},
      authorizationUrl: '',
      tokenUrl: '',
      scopes: [],
      servers: [],
    },
    authenticationStatus: 'explicit_anonymous',
    documentDisplayName: 'Example API',
    servers: [],
    destinations: {},
    operationCount: null,
    operations: [],
    unavailableOperations: [],
    diagnostic: '',
    readable: true,
    errors: {},
  },
  id: 'setup-1',
  kind: 'openapi',
  state: 'ready',
  credentialMethod: 'none',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'Uploaded document',
  sourceHost: '',
  serverName: '',
}
const setupBearer: SetupView = {
  ...setupReady,
  credentialMethod: 'bearer',
  configuration: {
    ...setupReady.configuration!,
    values: { ...setupReady.configuration!.values, authentication: 'bearer' },
  },
}
const configurationSetup: SetupView = {
  ...setupReady,
  state: 'awaiting_configuration',
  configuration: {
    ...setupReady.configuration!,
    confirmed: false,
    revision: '0',
    authenticationStatus: 'missing',
    documentDisplayName: 'Example API',
    values: {
      displayName: 'Example API',
      authentication: '',
      headers: [],
      parameters: [],
      basic: {
        username: { source: 'literal', value: '' },
        password: { source: 'literal', value: '' },
      },
      credentialFields: {},
      authorizationUrl: '',
      tokenUrl: '',
      scopes: [],
      servers: [
        {
          declaration: '#/servers',
          url: 'https://api.{site}/v1',
          custom: false,
          variables: { site: 'us.example.test' },
        },
      ],
    },
    servers: [
      {
        declaration: '#/servers',
        operations: ['users', 'events'],
        options: [
          {
            url: 'https://api.{site}/v1',
            variables: [
              {
                name: 'site',
                defaultValue: 'us.example.test',
                enumerated: true,
                values: ['us.example.test', 'eu.example.test'],
              },
            ],
          },
        ],
      },
      {
        declaration: '#/paths/~1logs/servers',
        operations: ['logs'],
        options: [
          {
            url: 'https://logs.{site}',
            variables: [
              { name: 'site', defaultValue: 'us.logs.test', enumerated: false, values: [] },
            ],
          },
        ],
      },
    ],
    destinations: {},
    operations: [],
    unavailableOperations: [],
    diagnostic: '',
    readable: true,
    errors: {},
  },
}
const largeOperationKeys = Array.from(
  { length: 2000 },
  (_, index) =>
    `actions/add-custom-labels-to-self-hosted-runner-for-org-${String(index + 1).padStart(4, '0')}`,
)
const largeCatalogReview: SetupView = {
  ...configurationSetup,
  configuration: {
    ...configurationSetup.configuration!,
    values: {
      ...configurationSetup.configuration!.values,
      servers: [
        {
          declaration: '#/servers',
          url: 'https://api.example.test/v1',
          custom: false,
          variables: {},
        },
      ],
    },
    servers: [
      {
        declaration: '#/servers',
        operations: largeOperationKeys,
        options: [{ url: 'https://api.example.test/v1', variables: [] }],
      },
    ],
  },
}
export const ConfigurationLargeCatalog: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={largeCatalogReview} />,
}
const setupOpenApiOAuth: SetupView = {
  ...setupReady,
  configuration: {
    ...setupReady.configuration!,
    application: { mode: 'manual', clientId: 'manual-client' },
    values: { ...setupReady.configuration!.values, authentication: 'oauth' },
  },
  credentialMethod: 'oauth',
  sourceLabel: 'https://api.example.com/openapi.json',
  sourceHost: 'api.example.com',
  serverName: 'api.example.com',
}
const setupAwaiting: SetupView = {
  ...setupReady,
  kind: 'mcp',
  configuration: {
    ...setupReady.configuration!,
    automaticClientAvailable: true,
    discoveryAuthorizationRequired: true,
    application: { mode: 'automatic', clientId: 'dcr-client' },
    values: { ...setupReady.configuration!.values, authentication: 'oauth' },
  },
  state: 'awaiting_authorization',
  credentialMethod: 'oauth',
  sourceLabel: 'https://mcp.example.com/mcp',
  sourceHost: 'mcp.example.com',
  serverName: 'mcp.example.com',
  expiresAt: '2026-09-10T09:00:00.000Z',
}
const pendingConsent = (clientId: string, bootstrap: 'registration' | 'manual') => ({
  state: 'pending' as const,
  requestedScopes: ['read', 'write'],
  bootstrap,
  failure: '',
  clientId,
  expiresAt: '2026-09-10T08:15:00.000Z',
})
const succeeded = (clientId: string, bootstrap: 'registration' | 'manual') => ({
  state: 'succeeded' as const,
  requestedScopes: ['read', 'write'],
  bootstrap,
  failure: '',
  clientId,
  expiresAt: '',
})
const setupMcpSignedIn: SetupView = {
  ...setupAwaiting,
  state: 'ready',
  authorized: true,
  provisionalGrantId: 'c0000000-0000-4000-8000-000000000001',
  grantedScope: 'read write',
  authorization: succeeded('dcr-client', 'registration'),
}
const stepperWorkspace = {
  workspace,
  recipes: catalogRecipes,
  basePath: '/workspaces/demo/providers/new',
  cancelPath: '/workspaces/demo/providers',
  attempt: {
    state: 'draft',
    kind: 'workspace',
    setupId: 'setup-1',
    configure: false,
    operationsAccepted: true,
    account: null,
    namespace: 'example',
  } as CreationAttempt,
  callbackUrl: 'https://reef.example.com/provider-setup/oauth/callback',
  form,
}
const workspaceEntry = { initialEntry: '/workspaces/demo/providers/new' }
export const StepWhereIsIt: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={null} />,
  parameters: workspaceEntry,
}
export const SourcePickerRecipes: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={null} />,
  parameters: workspaceEntry,
  play: async ({ canvas }) => {
    await expect(canvas.queryByRole('list', { name: 'Add provider' })).not.toBeInTheDocument()
    // GitHub has two recipes, so its card holds a button for each.
    await expect(canvas.getByRole('button', { name: 'Add GitHub API' })).toBeVisible()
    await expect(canvas.getByRole('button', { name: 'Add GitHub MCP' })).toBeVisible()
    await expect(canvas.getByRole('button', { name: 'Add Stripe API' })).toBeVisible()
  },
}
export const SourcePickerFiltered: Story = {
  ...SourcePickerRecipes,
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByRole('searchbox'), 'stripe')
    await expect(canvas.getByRole('button', { name: 'Add Stripe API' })).toBeVisible()
    await expect(canvas.queryByRole('button', { name: 'Add GitHub API' })).not.toBeInTheDocument()
    // The search reads the filter syntax, and a service matches by any recipe's id.
    await userEvent.clear(canvas.getByRole('searchbox'))
    await userEvent.type(canvas.getByRole('searchbox'), '^git*mcp$')
    await expect(canvas.getByRole('button', { name: 'Add GitHub MCP' })).toBeVisible()
    await expect(canvas.queryByRole('button', { name: 'Add Stripe API' })).not.toBeInTheDocument()
  },
}
export const SourcePickerNoMatch: Story = {
  ...SourcePickerRecipes,
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByRole('searchbox'), 'notion')
    await expect(canvas.getByText('No providers match “notion”')).toBeVisible()
  },
}
// Each kind names its own address, one above the other.
export const SourceOpenApiUrl: Story = {
  ...StepWhereIsIt,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('tab', { name: 'Add custom' }))
    await userEvent.type(
      canvas.getByLabelText('OpenAPI URL'),
      'https://api.example.com/openapi.yaml',
    )
    await expect(canvas.getByText('Drop an OpenAPI document here', { exact: false })).toBeVisible()
  },
}
export const SourceOpenApiUpload: Story = {
  ...StepWhereIsIt,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('tab', { name: 'Add custom' }))
    await expect(canvas.getByText('Drop an OpenAPI document here', { exact: false })).toBeVisible()
    await userEvent.upload(
      canvas.getByLabelText('OpenAPI file'),
      new File(['openapi: 3.1.0'], 'billing.yaml', { type: 'application/yaml' }),
    )
    // The zone reports what it holds through its own status region.
    await expect(
      canvas.getByText('billing.yaml', { exact: false }).closest('[role="status"]'),
    ).not.toBeNull()
  },
}
export const SourceMcpUrl: Story = {
  ...StepWhereIsIt,
  play: async ({ canvas }) => {
    // The catalog leads; a custom provider is one tab away.
    await expect(canvas.getByRole('tab', { name: 'Catalog' })).toHaveAttribute(
      'aria-selected',
      'true',
    )
    await userEvent.click(canvas.getByRole('tab', { name: 'Add custom' }))
    await userEvent.type(canvas.getByLabelText('MCP server URL'), 'https://mcp.example.com/mcp')
    // The OpenAPI document sits under the MCP server, ready as the alternative.
    await expect(canvas.getByLabelText('OpenAPI URL')).toBeVisible()
    await expect(canvas.getByLabelText('OpenAPI file')).toBeEnabled()
  },
}
export const StepWhereIsItInvalid: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={null}
      result={{
        error: 'Coral could not read it. Check the address and any bearer token.',
        credential: null,
      }}
    />
  ),
  parameters: workspaceEntry,
}
// Sign in provisionally to read protected operations before the operations step.
export const StepDiscoverySignIn: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupAwaiting} />,
  parameters: workspaceEntry,
}
export const StepDiscoverySignInPending: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{ ...setupAwaiting, authorization: pendingConsent('dcr-client', 'registration') }}
    />
  ),
  parameters: workspaceEntry,
}
export const StepDiscoverySignInScopeChallenge: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupAwaiting,
        authorization: {
          ...pendingConsent('manual-client', 'manual'),
          state: 'failed',
          failure: 'insufficient_scope',
        },
      }}
    />
  ),
  parameters: workspaceEntry,
}
// Retry a failed listing with the provisional discovery grant.
export const StepDiscoveryFailed: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupAwaiting,
        state: 'awaiting_discovery',
        authorized: true,
        grantedScope: 'read write',
        discoveryFailure: 'authenticated discovery did not complete',
        authorization: succeeded('dcr-client', 'registration'),
      }}
    />
  ),
  parameters: workspaceEntry,
}
// Step 2: what can it do?
/** One previewed Operation, as Coral projects it before anything is published. */
const previewedOperation = (
  key: string,
  group: string,
  summary: string,
  reasons: string[] = [],
): ReviewOperationView => ({
  key,
  relativePath: key.replace('/', '.'),
  summary,
  group,
  available: reasons.length === 0,
  reasons,
  path: '',
})
/** A confirmed source whose operations Coral has read, split across two hosts. */
const discovered: SetupView = {
  ...setupReady,
  configuration: {
    ...setupReady.configuration!,
    destinations: {
      'users/list': 'https://api.example.test/v1',
      'users/create': 'https://api.example.test/v1',
      'logs/submit': 'https://logs.example.test/',
    },
    unavailableOperations: [
      { key: 'events/stream', reasons: ['review the selected server and its declared variables'] },
    ],
    operations: [
      previewedOperation('users/list', 'users', 'List the people in the account.'),
      previewedOperation('users/create', 'users', 'Invite one person to the account.'),
      previewedOperation('logs/submit', 'logs', 'Send one log line.'),
      previewedOperation('events/stream', 'events', 'Read the event stream.', [
        'review the selected server and its declared variables',
      ]),
    ],
  },
}
const beforeOperations = { ...stepperWorkspace.attempt, operationsAccepted: false }
// Catches: a step's actions moving back to the page's top bar, or its title or
// count going missing.
export const StepOperations: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={discovered} attempt={beforeOperations} />,
  parameters: workspaceEntry,
  play: async ({ canvas, canvasElement }) => {
    await expect(canvas.getByRole('heading', { level: 2, name: 'Operations' })).toBeVisible()
    await expect(canvas.getByText('4 operations in 3 groups · 2 destinations')).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'Next' })).toBeVisible()
    const topBar = canvasElement.querySelector('header')!
    expect(within(topBar).queryByRole('link', { name: 'Next' })).toBeNull()
  },
}
export const StepOperationsEmpty: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupReady,
        kind: 'mcp',
        sourceLabel: 'https://mcp.example.test',
        configuration: {
          ...setupReady.configuration!,
          operationCount: 0,
          operations: [],
          diagnostic: 'empty_mcp_catalog',
        },
      }}
      attempt={beforeOperations}
    />
  ),
  parameters: workspaceEntry,
}

export const OperationsLargeCatalog: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      attempt={beforeOperations}
      setup={{
        ...largeCatalogReview,
        state: 'ready',
        configuration: {
          ...largeCatalogReview.configuration!,
          confirmed: true,
          values: { ...largeCatalogReview.configuration!.values, authentication: 'none' },
          destinations: Object.fromEntries(
            largeOperationKeys.map((key, index) => [
              key,
              index < 1900 ? 'https://api.example.test/v1' : 'https://api.example.test/v2',
            ]),
          ),
          unavailableOperations: Array.from({ length: 120 }, (_, index) => ({
            key: `events/unavailable-${index + 1}`,
            reasons: ['Review the selected server and its declared variables.'],
          })),
          operations: [
            ...largeOperationKeys.map((key) =>
              previewedOperation(key, 'actions', 'Add custom labels to a self-hosted runner.'),
            ),
            ...Array.from({ length: 120 }, (_, index) =>
              previewedOperation(
                `events/unavailable-${index + 1}`,
                'events',
                'Read the event stream.',
                ['Review the selected server and its declared variables.'],
              ),
            ),
          ],
        },
      }}
    />
  ),
}
export const OperationsLargeCatalogNoMatches: Story = {
  ...OperationsLargeCatalog,
  play: async ({ canvas }) => {
    await userEvent.type(
      canvas.getByRole('searchbox', { name: 'Find an operation' }),
      'not-an-operation',
    )
    await expect(canvas.getByText('No operations match your search.')).toBeVisible()
  },
}
export const OperationsUnavailableSearch: Story = {
  ...OperationsLargeCatalog,
  play: async ({ canvas }) => {
    // Every operation the source offers, including the 120 that cannot run.
    await expect(canvas.getByText('2,120 operations in 2 groups · 2 destinations')).toBeVisible()
    // What cannot run is a group of its own rather than a disclosure below.
    await expect(canvas.getByText('Cannot run', { exact: true })).toBeVisible()
    // A search reaches into the collapsed groups rather than past them.
    await userEvent.type(
      canvas.getByRole('searchbox', { name: 'Find an operation' }),
      'unavailable-120',
    )
    await expect(canvas.getByText('events.unavailable-120', { exact: true })).toBeVisible()
    expect(canvas.queryByText('actions', { exact: true })).toBeNull()
  },
}

// Step 3: whose account does it use?
export const StepAccountNoneNeeded: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupReady} />,
  parameters: workspaceEntry,
}
export const StepAccountToken: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupBearer} />,
  parameters: workspaceEntry,
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('heading', { level: 2, name: 'Account' })).toBeVisible()
  },
}
export const StepAccountTokenEachPerson: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupBearer}
      attempt={{ ...stepperWorkspace.attempt, account: 'caller' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=caller' },
}
export const StepAccountOAuthApplication: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupOpenApiOAuth} />,
  parameters: workspaceEntry,
}
export const StepAccountOAuthPending: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{ ...setupOpenApiOAuth, authorization: pendingConsent('manual-client', 'manual') }}
      attempt={{ ...stepperWorkspace.attempt, account: 'fixed' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=fixed' },
}
export const StepAccountOAuthInvalidResponse: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupOpenApiOAuth,
        authorization: {
          ...pendingConsent('manual-client', 'manual'),
          state: 'failed',
          failure: 'oauth_token_response_invalid',
        },
      }}
      attempt={{ ...stepperWorkspace.attempt, account: 'fixed' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=fixed' },
}
export const StepAccountOAuthExpired: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupOpenApiOAuth,
        authorization: { ...pendingConsent('manual-client', 'manual'), state: 'expired' },
      }}
      attempt={{ ...stepperWorkspace.attempt, account: 'fixed' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=fixed' },
}
export const StepAccountMcpSignedIn: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupMcpSignedIn} />,
  parameters: workspaceEntry,
}
export const StepAccountSeparateDiscoveryGrant: Story = {
  ...StepAccountMcpSignedIn,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A sign-in in hand is kept for the person who made it, not shared.
    await expect(canvas.getByRole('radio', { name: 'Connect now with this account' })).toBeChecked()
    await expect(canvas.getByRole('radio', { name: 'Use this account' })).not.toBeChecked()
    await expect(canvas.getByRole('button', { name: 'Add provider' })).toBeEnabled()
  },
}
export const StepAccountDifferentPersonal: Story = {
  ...StepAccountMcpSignedIn,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // Individual accounts can start with a new sign-in, without sharing it.
    await userEvent.click(canvas.getByRole('radio', { name: 'Connect now with another account' }))
    await expect(canvas.getByRole('button', { name: 'Sign in again' })).toBeEnabled()
  },
}
export const StepAccountPersonalOAuthPending: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{ ...setupOpenApiOAuth, authorization: pendingConsent('manual-client', 'manual') }}
      attempt={{ ...stepperWorkspace.attempt, account: 'caller', returnFromConnect: true }}
    />
  ),
  parameters: workspaceEntry,
}
export const StepAccountFailed: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupBearer}
      result={{
        error:
          'Coral could not save the bearer token or API key. Reload the page to check whether Coral applied the change before you submit it again.',
        credential: null,
      }}
    />
  ),
  parameters: workspaceEntry,
}
// Step 4: check it and add it.
export const StepNameIt: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupReady} />,
  parameters: workspaceEntry,
}
export const StepNameItSharedAccount: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupOpenApiOAuth,
        authorized: true,
        provisionalGrantId: 'c0000000-0000-4000-8000-000000000001',
        grantedScope: 'read write',
        authorization: succeeded('manual-client', 'manual'),
      }}
      attempt={{
        ...stepperWorkspace.attempt,
        account: 'fixed',
        accountGrant: 'c0000000-0000-4000-8000-000000000001',
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=fixed' },
}
export const StepNameItOwnAccount: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupMcpSignedIn}
      attempt={{
        ...stepperWorkspace.attempt,
        account: 'caller',
        accountGrant: setupMcpSignedIn.provisionalGrantId,
      }}
    />
  ),
  parameters: workspaceEntry,
}
export const StepNameItEachPerson: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupOpenApiOAuth}
      attempt={{ ...stepperWorkspace.attempt, account: 'caller' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/new?setup=setup-1&account=caller' },
}
export const StepNameItPending: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={setupReady} pending />,
  parameters: workspaceEntry,
}
export const StepNameItFailed: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupReady}
      result={{
        error: null,
        credential: null,
        failed: 'The document declares no server URL Coral can call.',
      }}
    />
  ),
  parameters: workspaceEntry,
}
export const StepNameItRetry: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={setupReady}
      attempt={{
        kind: 'workspace',
        state: 'frozen',
        setupId: setupReady.id,
        configurationRevision: '1',
        requestId: 'b0000000-0000-4000-8000-000000000001',
        namespace: 'weather',
        account: 'none',
      }}
    />
  ),
  parameters: {
    initialEntry:
      '/workspaces/demo/providers/new?setup=setup-1&requestId=b0000000-0000-4000-8000-000000000001',
  },
}
export const StepNameItConsumed: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{ ...setupReady, state: 'consumed' }}
      attempt={{
        kind: 'workspace',
        state: 'frozen',
        setupId: setupReady.id,
        configurationRevision: '1',
        requestId: 'b0000000-0000-4000-8000-000000000001',
        namespace: 'weather',
        account: 'none',
      }}
    />
  ),
}
export const UpdatePublicationRetry: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{ ...setupReady, state: 'consumed' }}
      attempt={{
        kind: 'update',
        state: 'frozen',
        setupId: setupReady.id,
        configurationRevision: '2',
        requestId: 'b0000000-0000-4000-8000-000000000001',
        etag: 'accepted-etag',
        allowIncompatibleConfiguration: true,
      }}
    />
  ),
}
export const UpdatePublicationFailedAfterReplay: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{ ...setupReady, state: 'consumed' }}
      attempt={{
        kind: 'update',
        state: 'frozen',
        setupId: setupReady.id,
        configurationRevision: '2',
        requestId: 'b0000000-0000-4000-8000-000000000001',
        etag: 'accepted-etag',
        allowIncompatibleConfiguration: true,
      }}
      result={{
        failed: 'The document declares duplicate operations.',
        error: null,
        credential: null,
      }}
    />
  ),
}
export const StepExpired: Story = {
  render: () => (
    <StepperPage {...stepperWorkspace} setup={{ ...setupAwaiting, state: 'expired' }} />
  ),
  parameters: workspaceEntry,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A typed URL starts again from the URL its Setup read.
    await expect(canvas.getByText('This setup has expired')).toBeVisible()
    await expect(canvas.getByRole('button', { name: 'Start again' })).toBeEnabled()
    await expect(canvas.getByRole('link', { name: 'Back to providers' })).toHaveAttribute(
      'href',
      '/workspaces/demo/providers',
    )
  },
}
export const StepExpiredRecipe: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      recipe={githubRecipe}
      attempt={{ ...stepperWorkspace.attempt, recipe: githubRecipe.id }}
      setup={{ ...setupReady, state: 'expired' }}
    />
  ),
  parameters: workspaceEntry,
}
export const StepExpiredUploadedDocument: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{ ...setupReady, state: 'expired', sourceLabel: 'Uploaded document', sourceHost: '' }}
    />
  ),
  parameters: workspaceEntry,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // Reef does not keep an uploaded document, so you choose it again.
    await expect(canvas.getByText('Upload the document again to continue.')).toBeVisible()
    await expect(canvas.queryByRole('button', { name: 'Start again' })).toBeNull()
    await expect(canvas.getByRole('link', { name: 'Choose a source' })).toBeVisible()
  },
}
export const StepCanceled: Story = {
  render: () => (
    <StepperPage {...stepperWorkspace} setup={{ ...setupAwaiting, state: 'canceled' }} />
  ),
  parameters: workspaceEntry,
}
export const RefreshUploadedDocument: Story = {
  render: () => (
    <UpdatePage
      displayName="GitHub"
      basePath="/workspaces/demo/providers/prov-github/update"
      cancelPath="/workspaces/demo/providers/prov-github"
      providersPath="/workspaces/demo/providers"
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: null,
        configure: false,
        operationsAccepted: true,
        etag: 'prov-etag',
        allowIncompatibleConfiguration: false,
      }}
      form={form}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/prov-github/update' },
}
export const UpdateReady: Story = {
  render: () => (
    <EditConfigurationPage
      displayName="GitHub"
      basePath="/workspaces/demo/providers/prov-github/update"
      cancelPath="/workspaces/demo/providers/prov-github"
      providersPath="/workspaces/demo/providers"
      setup={{ ...setupReady, sourceLabel: 'https://api.github.com/openapi.json' }}
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: setupReady.id,
        configure: false,
        operationsAccepted: true,
        etag: 'prov-etag',
        allowIncompatibleConfiguration: false,
      }}
      form={form}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/prov-github/update' },
}
export const UpdateFailed: Story = {
  render: () => (
    <EditConfigurationPage
      displayName="GitHub"
      basePath="/workspaces/demo/providers/prov-github/update"
      cancelPath="/workspaces/demo/providers/prov-github"
      providersPath="/workspaces/demo/providers"
      setup={setupReady}
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: setupReady.id,
        configure: false,
        operationsAccepted: true,
        etag: 'prov-etag',
        allowIncompatibleConfiguration: false,
      }}
      form={form}
      result={{
        error: null,
        credential: null,
        failed: 'The new document needs a different sign-in than the current account provides.',
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/prov-github/update' },
}

const mcpReview: SetupView = {
  ...setupAwaiting,
  state: 'awaiting_configuration',
  configuration: {
    ...configurationSetup.configuration!,
    authenticationMethods: ['bearer', 'oauth'],
    authenticationStatus: 'detected_oauth',
    documentDisplayName: 'Example API',
    discoveryAuthorizationRequired: true,
    automaticClientAvailable: true,
    issuer: 'https://accounts.example.test',
    resource: 'https://mcp.example.com/mcp',
    values: {
      displayName: 'Example API',
      authentication: 'oauth',
      headers: [],
      parameters: [],
      basic: {
        username: { source: 'literal', value: '' },
        password: { source: 'literal', value: '' },
      },
      credentialFields: {},
      authorizationUrl: 'https://accounts.example.test/authorize',
      tokenUrl: 'https://accounts.example.test/token',
      scopes: ['read', 'write'],
      servers: [
        {
          declaration: '#/servers',
          url: 'https://mcp.example.com/mcp',
          custom: true,
          variables: {},
        },
      ],
    },
    servers: [{ declaration: '#/servers', operations: [], options: [] }],
  },
}
export const ConfigurationMcpOAuthBeforeSignIn: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={mcpReview} />,
}
export const ConfigurationMcpOAuthDetails: Story = {
  ...ConfigurationMcpOAuthBeforeSignIn,
  play: async ({ canvas }) => {
    // The server's settings are the source column: stated, never offered for editing.
    const server = within(
      canvas.getByRole('complementary', { name: 'OAuth settings from the server' }),
    )
    const endpoint = server.getByRole('textbox', { name: 'Authorization URL' })
    await expect(endpoint).toHaveValue('https://accounts.example.test/authorize')
    await expect(endpoint).toBeDisabled()
  },
}
export const ConfigurationMcpApplicationChoice: Story = {
  ...ConfigurationMcpOAuthBeforeSignIn,
  play: async ({ canvas }) => {
    // An empty Client ID leaves the application to Coral; filling it takes it over.
    await expect(canvas.getByLabelText('Callback URL')).toHaveValue(stepperWorkspace.callbackUrl)
    await expect(canvas.getByLabelText('Client ID')).toHaveValue('')
    await expect(canvas.queryByRole('radio', { name: 'Public client' })).not.toBeInTheDocument()
    await userEvent.type(canvas.getByLabelText('Client ID'), 'mine')
    await expect(canvas.getByRole('radio', { name: 'Public client' })).toBeInTheDocument()
  },
}
export const ConfigurationMcpManualApplication: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...mcpReview,
        configuration: { ...mcpReview.configuration!, automaticClientAvailable: false },
      }}
    />
  ),
}
export const ConfigurationMcpMissingMetadata: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...mcpReview,
        credentialMethod: 'bearer',
        configuration: {
          ...mcpReview.configuration!,
          authenticationMethods: ['bearer'],
          authenticationStatus: 'metadata_missing',
          documentDisplayName: 'Example API',
          values: { ...mcpReview.configuration!.values, authentication: '' },
        },
      }}
    />
  ),
}
export const ConfigurationMcpAnonymous: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...mcpReview,
        credentialMethod: 'none',
        configuration: {
          ...mcpReview.configuration!,
          authenticationMethods: ['none', 'bearer'],
          authenticationStatus: 'detected_anonymous',
          documentDisplayName: 'Example API',
          discoveryAuthorizationRequired: false,
          confirmed: false,
          destinations: {},
          unavailableOperations: [],
          values: { ...mcpReview.configuration!.values, authentication: 'none' },
        },
      }}
    />
  ),
  play: async ({ canvas }) => {
    // An MCP catalog not counted yet is not an empty one.
    await expect(canvas.queryByText('No operations found')).not.toBeInTheDocument()
  },
}

export const ConfigurationEditAfterSignIn: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      attempt={{
        ...stepperWorkspace.attempt,
        state: 'draft',
        configure: true,
        operationsAccepted: false,
        account: 'caller',
      }}
      setup={{
        ...setupMcpSignedIn,
        configuration: { ...mcpReview.configuration!, confirmed: true },
      }}
    />
  ),
  parameters: {
    initialEntry: '/workspaces/demo/providers/new?setup=setup-1&configure=1&account=caller',
  },
}
const updateReview: SetupView = {
  ...configurationSetup,
  credentialMethod: 'bearer',
  configuration: {
    ...configurationSetup.configuration!,
    values: { ...configurationSetup.configuration!.values, authentication: 'bearer' },
    updating: true,
    differences: [
      { field: 'authentication.method', saved: 'bearer', document: 'none' },
      {
        field: '#/servers.url',
        saved: 'https://api.us.example.test',
        document: 'https://api.eu.example.test',
      },
      { field: '#/paths/~1removed/servers.url', saved: 'https://removed.example.test' },
    ],
  },
}
const updateStory = {
  displayName: 'Records',
  basePath: '/workspaces/demo/providers/records/update',
  cancelPath: '/workspaces/demo/providers/records',
  providersPath: '/workspaces/demo/providers',
  attempt: {
    kind: 'update',
    state: 'draft',
    setupId: 'setup-1',
    configure: false,
    operationsAccepted: true,
    etag: 'etag',
    allowIncompatibleConfiguration: false,
  } as UpdateAttempt,
  form,
}
export const UpdateExpired: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      attempt={{ ...updateStory.attempt, task: 'configuration' }}
      setup={{ ...setupReady, state: 'expired' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
}
export const UpdateCompareValues: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      attempt={{ ...updateStory.attempt, task: 'refresh' } as UpdateAttempt}
      setup={updateReview}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
}
export const UpdateUnreadableSource: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      attempt={{ ...updateStory.attempt, task: 'refresh' } as UpdateAttempt}
      setup={{
        ...updateReview,
        configuration: {
          ...updateReview.configuration!,
          differences: [],
          diagnostic: 'duplicate_operation_key',
          readable: false,
        },
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
}
export const UpdateRemovedDeclaration: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{
        ...updateReview,
        configuration: {
          ...updateReview.configuration!,
          values: {
            ...updateReview.configuration!.values,
            servers: [
              ...updateReview.configuration!.values.servers,
              {
                declaration: '#/paths/~1removed/servers',
                url: 'https://removed.example.test',
                custom: true,
                variables: {},
              },
            ],
          },
        },
      }}
    />
  ),
}
export const UpdateInvalidLocalServerSelection: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          updating: true,
          authenticationStatus: 'explicit_anonymous',
          documentDisplayName: 'Example API',
          differences: [
            {
              field: '#/paths/~1logs/servers',
              saved: 'https://logs.us.logs.test',
              document: 'https://logs.eu.logs.test',
            },
          ],
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'none',
            servers: [
              ...configurationSetup.configuration!.values.servers,
              {
                declaration: '#/paths/~1logs/servers',
                url: 'https://logs.{site}',
                custom: false,
                variables: { site: 'us.logs.test' },
              },
            ],
          },
          servers: configurationSetup.configuration!.servers.map((group) =>
            group.declaration === '#/servers'
              ? group
              : {
                  ...group,
                  options: [
                    {
                      url: 'https://logs.{site}',
                      variables: [
                        {
                          name: 'site',
                          defaultValue: 'eu.logs.test',
                          enumerated: true,
                          values: ['eu.logs.test'],
                        },
                      ],
                    },
                  ],
                },
          ),
          errors: { servers: 'Select a declared value for site.' },
        },
      }}
    />
  ),
}
export const UpdateIncompatibleWarning: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{
        ...updateReview,
        state: 'ready',
        configuration: { ...updateReview.configuration!, confirmed: true, interruptsAccess: true },
      }}
    />
  ),
}
export const UpdateCompatibleWithoutRepair: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{
        ...updateReview,
        state: 'ready',
        configuration: { ...updateReview.configuration!, confirmed: true, interruptsAccess: false },
      }}
    />
  ),
  play: async ({ canvas }) => {
    // Nothing breaks access, so Save publishes without asking.
    await expect(canvas.getByRole('button', { name: 'Save' })).toHaveAttribute('type', 'submit')
  },
}
export const UpdateMcpProtectedDiscovery: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      setup={{
        ...setupAwaiting,
        configuration: { ...setupAwaiting.configuration!, updating: true },
      }}
      callbackUrl={stepperWorkspace.callbackUrl}
    />
  ),
}

export const RefreshSavedSource: Story = {
  render: () => (
    <UpdatePage
      {...updateStory}
      source={{ url: 'https://api.example.test/openapi.json', kind: 'url' }}
    />
  ),
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('button', { name: 'Read operations' })).toBeEnabled()
    await expect(canvas.queryByRole('heading', { name: 'Changes' })).not.toBeInTheDocument()
    await expect(canvas.queryByRole('heading', { name: 'Review' })).not.toBeInTheDocument()
    await expect(canvas.queryByRole('button', { name: 'Change source' })).not.toBeInTheDocument()
    await expect(canvas.queryByRole('textbox', { name: 'Source URL' })).not.toBeInTheDocument()
  },
}

export const ReadingSavedSource: Story = {
  render: () => (
    <UpdatePage
      {...updateStory}
      pending
      source={{ url: 'https://api.example.test/openapi.json', kind: 'url' }}
    />
  ),
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('status')).toHaveTextContent('Reading operations…')
    await expect(canvas.getByRole('button', { name: 'Save' })).toBeDisabled()
  },
}
export const ReplaceProviderSource: Story = {
  render: () => <UpdatePage {...updateStory} replacingSource />,
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('textbox', { name: 'MCP server URL' })).toBeVisible()
  },
}

// Recipe configuration

export const TreeProviderDisabled: Story = {
  render: () => <WorkspaceProvidersPage {...treeProps} selectedProvider="prov-hr" />,
}
export const ConfigurationFromRecipe: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={configurationSetup}
      recipeConfiguration={{
        id: 'example',
        inputs: [
          {
            name: 'site',
            label: 'Example site',
            description: 'Choose the site for your account.',
            type: 'hostname',
            defaultValue: 'eu.example.test',
            choices: ['us.example.test', 'eu.example.test'],
          },
        ],
        suggestedMethods: ['bearer'],
        values: {
          authentication: 'bearer',
          servers: [
            {
              declaration: '#/servers',
              url: 'https://api.{site}/v1',
              custom: false,
              variables: { site: '{{input.site}}' },
            },
          ],
        },
      }}
    />
  ),
  parameters: workspaceEntry,
  play: async ({ canvas }) => {
    // The recipe's questions are plain fields now, so the story checks the
    // question and the value it filled in rather than a section around them.
    await expect(canvas.getByText('Example site')).toBeVisible()
    await expect(canvas.getByRole('button', { name: 'eu.example.test' })).toBeVisible()
  },
}
// Datadog's shape: two keys the person enters, each in its own header, and a fixed header.
export const ConfigurationHeaderCredentials: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...configurationSetup,
        configuration: {
          ...configurationSetup.configuration!,
          values: {
            ...configurationSetup.configuration!.values,
            authentication: 'header',
            headers: [
              { name: 'dd-api-key', source: 'field', field: 'api_key', prefix: '' },
              {
                name: 'dd-application-key',
                source: 'field',
                field: 'application_key',
                prefix: '',
              },
              { name: 'x-api-version', source: 'literal', value: '2024-01' },
            ],
            credentialFields: {
              api_key: { label: 'API key', description: '', sensitive: true },
              application_key: { label: 'Application key', description: '', sensitive: true },
            },
          },
        },
      }}
    />
  ),
  parameters: { ...workspaceEntry, chromatic: { viewports: [390, 1440] } },
}

const editStory = {
  ...updateStory,
  currentAccount: 'caller' as const,
  attempt: { ...updateStory.attempt, task: 'configuration' } as UpdateAttempt,
}
export const EditConfiguration: Story = {
  render: () => (
    <EditConfigurationPage
      {...editStory}
      setup={{ ...updateReview, state: 'awaiting_configuration' }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
}
/** Save confirmed a change that makes each account reconnect, so it asks first. */
export const EditConfigurationReconnect: Story = {
  render: () => (
    <EditConfigurationPage
      {...editStory}
      setup={{
        ...updateReview,
        state: 'ready',
        configuration: { ...updateReview.configuration!, interruptsAccess: true },
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
  play: async () => {
    const dialog = await screen.findByRole('dialog', { name: 'Save changes?' })
    await expect(within(dialog).getByRole('button', { name: 'Save' })).toBeEnabled()
  },
}
/** Change account setting compares the two settings as Add provider does. */
export const ChangeAccountSettingRoutes: Story = {
  render: () => (
    <EditConfigurationPage
      {...editStory}
      currentAccount="fixed"
      attempt={{ ...editStory.attempt, task: 'accounts' }}
      setup={{
        ...updateReview,
        state: 'ready',
        configuration: { ...updateReview.configuration!, interruptsAccess: false },
      }}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
}
/** After a refresh reads the source, the operations the live provider lacks group at the top. */
export const RefreshNewOperations: Story = {
  render: () => (
    <EditConfigurationPage
      {...updateStory}
      currentAccount="none"
      source={{ url: 'https://api.example.test/openapi.json', kind: 'url' }}
      attempt={{ ...updateStory.attempt, task: 'refresh' } as UpdateAttempt}
      setup={{
        ...discovered,
        credentialMethod: 'none',
        configuration: { ...discovered.configuration!, interruptsAccess: false },
      }}
      currentOperations={[
        { relativePath: 'users.list', summary: 'List the people in the account.' },
        { relativePath: 'events.stream', summary: 'Read the event stream.' },
        { relativePath: 'users.delete', summary: 'Remove one person from the account.' },
      ]}
    />
  ),
  parameters: { initialEntry: '/workspaces/demo/providers/records/update' },
  play: async ({ canvas }) => {
    // What the refresh changes opens; the provider's own groups stay closed.
    await expect(canvas.getByRole('button', { name: 'Collapse New' })).toBeInTheDocument()
    await expect(canvas.getByRole('button', { name: 'Collapse Removed' })).toBeInTheDocument()
    expect(canvas.queryAllByRole('button', { name: /^Expand / }).length).toBeGreaterThan(0)
    expect(canvas.queryAllByRole('button', { name: /^Collapse / })).toHaveLength(2)
  },
}
