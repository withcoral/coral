import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, screen, userEvent, within } from 'storybook/test'
import type { CreationAttempt, UpdateAttempt } from '@/lib/provider-publication-attempt/model'
import { StepperPage } from './stepper-page'
import type { ReviewToolView, SetupView } from './types'
import { UpdatePage } from './update-page'
import { EditConfigurationPage } from './edit-configuration-page'
import { catalogRecipes, form, githubRecipe, workspace } from './story-fixtures'

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
          <Stub initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo/tools']} />
        </div>
      )
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

export const ConfigurationMissingAuthentication: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={configurationSetup} />,
  play: async ({ canvas }) => {
    // The current crumb names the provider being added and titles the page.
    await expect(canvas.getByRole('navigation', { name: 'breadcrumb' })).toHaveTextContent(
      'ToolsAdd Example API',
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
        error: 'Choose a server and complete its variables.',
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
          '[Create a Slack app](https://api.slack.com/apps?new_app=1&manifest_yaml=display_information%3A%0A), then copy its client ID and client secret here. For help, read the [Slack setup guide](https://withcoral.com/docs/guides/providers/slack).',
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
    await expect(
      within(canvasElement).getByRole('link', { name: 'Slack setup guide' }),
    ).toHaveAttribute('href', 'https://withcoral.com/docs/guides/providers/slack')
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
          unavailableTools: [
            {
              key: 'events',
              reasons: ['Choose a server for this tool and complete its variables.'],
            },
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
    toolCount: null,
    tools: [],
    unavailableTools: [],
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
  allowedRoutes: ['none'],
  connectAvailable: false,
  connectLaterAvailable: false,
}
const setupBearer: SetupView = {
  ...setupReady,
  credentialMethod: 'bearer',
  allowedRoutes: ['caller', 'fixed'],
  connectAvailable: true,
  connectLaterAvailable: true,
  configuration: {
    ...setupReady.configuration!,
    values: { ...setupReady.configuration!.values, authentication: 'bearer' },
  },
}
const configurationSetup: SetupView = {
  ...setupReady,
  state: 'awaiting_configuration',
  // Coral allows no route until the configuration is confirmed.
  allowedRoutes: [],
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
        tools: ['users', 'events'],
        required: true,
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
        tools: ['logs'],
        required: true,
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
    tools: [],
    unavailableTools: [],
    diagnostic: '',
    readable: true,
    errors: {},
  },
}
const largeToolKeys = Array.from(
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
        tools: largeToolKeys,
        required: true,
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
  allowedRoutes: ['caller', 'fixed'],
  connectAvailable: true,
  connectLaterAvailable: true,
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
  // Coral sets up the application only at a sign-in, so nobody can connect later yet.
  allowedRoutes: ['caller', 'fixed'],
  connectAvailable: true,
  connectLaterAvailable: false,
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
  // The sign-in set up the application each person connects through later.
  connectLaterAvailable: true,
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
    toolsAccepted: true,
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
    // A service matches by any recipe's id.
    await userEvent.clear(canvas.getByRole('searchbox'))
    await userEvent.type(canvas.getByRole('searchbox'), 'github_mcp')
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
// Sign in provisionally to read protected tools before the tools step.
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
/** One previewed Tool, as Coral projects it before anything is published. */
const previewedTool = (
  key: string,
  group: string,
  summary: string,
  reasons: string[] = [],
): ReviewToolView => ({
  key,
  relativePath: key.replace('/', '.'),
  summary,
  group,
  available: reasons.length === 0,
  reasons,
  path: '',
})
/** A confirmed source whose tools Coral has read, split across two hosts. */
const discovered: SetupView = {
  ...setupReady,
  configuration: {
    ...setupReady.configuration!,
    destinations: {
      'users/list': 'https://api.example.test/v1',
      'users/create': 'https://api.example.test/v1',
      'logs/submit': 'https://logs.example.test/',
    },
    unavailableTools: [
      {
        key: 'events/stream',
        reasons: ['Choose a server for this tool and complete its variables.'],
      },
    ],
    tools: [
      previewedTool('users/list', 'users', 'List the people in the account.'),
      previewedTool('users/create', 'users', 'Invite one person to the account.'),
      previewedTool('logs/submit', 'logs', 'Send one log line.'),
      previewedTool('events/stream', 'events', 'Read the event stream.', [
        'Choose a server for this tool and complete its variables.',
      ]),
    ],
  },
}
const beforeTools = { ...stepperWorkspace.attempt, toolsAccepted: false }
// Catches: a step's actions moving back to the page's top bar, or its title or
// count going missing.
export const StepTools: Story = {
  render: () => <StepperPage {...stepperWorkspace} setup={discovered} attempt={beforeTools} />,
  parameters: workspaceEntry,
  play: async ({ canvas, canvasElement }) => {
    await expect(canvas.getByRole('heading', { level: 2, name: 'Tools' })).toBeVisible()
    await expect(canvas.getByText('4 tools in 3 groups · 2 destinations')).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'Next' })).toBeVisible()
    const topBar = canvasElement.querySelector('header')!
    expect(within(topBar).queryByRole('link', { name: 'Next' })).toBeNull()
  },
}
export const StepToolsEmpty: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      setup={{
        ...setupReady,
        kind: 'mcp',
        sourceLabel: 'https://mcp.example.test',
        configuration: {
          ...setupReady.configuration!,
          toolCount: 0,
          tools: [],
          diagnostic: 'empty_mcp_catalog',
        },
      }}
      attempt={beforeTools}
    />
  ),
  parameters: workspaceEntry,
}

export const ToolsLargeCatalog: Story = {
  render: () => (
    <StepperPage
      {...stepperWorkspace}
      attempt={beforeTools}
      setup={{
        ...largeCatalogReview,
        state: 'ready',
        configuration: {
          ...largeCatalogReview.configuration!,
          confirmed: true,
          values: { ...largeCatalogReview.configuration!.values, authentication: 'none' },
          destinations: Object.fromEntries(
            largeToolKeys.map((key, index) => [
              key,
              index < 1900 ? 'https://api.example.test/v1' : 'https://api.example.test/v2',
            ]),
          ),
          unavailableTools: Array.from({ length: 120 }, (_, index) => ({
            key: `events/unavailable-${index + 1}`,
            reasons: ['Review the selected server and its declared variables.'],
          })),
          tools: [
            ...largeToolKeys.map((key) =>
              previewedTool(key, 'actions', 'Add custom labels to a self-hosted runner.'),
            ),
            ...Array.from({ length: 120 }, (_, index) =>
              previewedTool(`events/unavailable-${index + 1}`, 'events', 'Read the event stream.', [
                'Review the selected server and its declared variables.',
              ]),
            ),
          ],
        },
      }}
    />
  ),
}
export const ToolsLargeCatalogNoMatches: Story = {
  ...ToolsLargeCatalog,
  play: async ({ canvas }) => {
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Find a tool' }), 'not-an-tool')
    await expect(canvas.getByText('No tools match your search.')).toBeVisible()
  },
}
export const ToolsUnavailableSearch: Story = {
  ...ToolsLargeCatalog,
  play: async ({ canvas }) => {
    // Every tool the source offers, including the 120 that cannot run.
    await expect(canvas.getByText('2,120 tools in 2 groups · 2 destinations')).toBeVisible()
    // What cannot run is a group of its own rather than a disclosure below.
    await expect(canvas.getByText('Cannot run', { exact: true })).toBeVisible()
    // A search reaches into the collapsed groups rather than past them.
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Find a tool' }), 'unavailable-120')
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
        failed: 'The document declares duplicate tools.',
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
      toolsPath="/workspaces/demo/tools"
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: null,
        configure: false,
        toolsAccepted: true,
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
      toolsPath="/workspaces/demo/tools"
      setup={{ ...setupReady, sourceLabel: 'https://api.github.com/openapi.json' }}
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: setupReady.id,
        configure: false,
        toolsAccepted: true,
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
      toolsPath="/workspaces/demo/tools"
      setup={setupReady}
      attempt={{
        kind: 'update',
        state: 'draft',
        setupId: setupReady.id,
        configure: false,
        toolsAccepted: true,
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
  allowedRoutes: [],
  connectAvailable: false,
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
    servers: [{ declaration: '#/servers', tools: [], required: false, options: [] }],
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
          unavailableTools: [],
          values: { ...mcpReview.configuration!.values, authentication: 'none' },
        },
      }}
    />
  ),
  play: async ({ canvas }) => {
    // An MCP catalog not counted yet is not an empty one.
    await expect(canvas.queryByText('No tools found')).not.toBeInTheDocument()
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
        toolsAccepted: false,
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
  toolsPath: '/workspaces/demo/tools',
  attempt: {
    kind: 'update',
    state: 'draft',
    setupId: 'setup-1',
    configure: false,
    toolsAccepted: true,
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
          diagnostic: 'duplicate_tool_key',
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
    await expect(canvas.getByRole('button', { name: 'Read tools' })).toBeEnabled()
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
    await expect(canvas.getByRole('status')).toHaveTextContent('Reading tools…')
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
/** After a refresh reads the source, the tools the live provider lacks group at the top. */
export const RefreshNewTools: Story = {
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
      currentTools={[
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
