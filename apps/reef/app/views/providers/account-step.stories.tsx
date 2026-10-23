import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'

import { LocalDeploymentContext } from '@/lib/deployment'
import type { CreationAttempt } from '@/lib/provider-publication-attempt/model'
import { AccountStep } from './account-step'
import type { SetupView } from './types'

/**
 * The last step of Add on its own, so its layout can be worked on without
 * walking the stepper to reach it. It asks whose account the provider uses and
 * adds it; every story renders the real step, and only the Setup and the
 * attempt change.
 */
const form = { csrf: 'storybook-csrf', requestId: 'storybook-request' }

/** A confirmed OpenAPI configuration: the shape every story here starts from. */
const datadog: SetupView = {
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
      displayName: 'Datadog API v1',
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
      servers: [
        {
          declaration: '#/servers',
          url: 'https://api.datadoghq.com/',
          custom: false,
          variables: {},
        },
      ],
    },
    authenticationStatus: 'explicit_anonymous',
    documentDisplayName: 'Datadog API v1',
    servers: [
      {
        declaration: '#/servers',
        operations: ['metrics/list', 'monitors/list'],
        options: [{ url: 'https://api.datadoghq.com/', variables: [] }],
      },
    ],
    operations: [],
    destinations: {
      ...Object.fromEntries(
        Array.from({ length: 233 }, (_, index) => [
          `metrics/operation-${index + 1}`,
          'https://api.datadoghq.com/',
        ]),
      ),
      'logs/submit': 'https://http-intake.logs.datadoghq.com/',
      'ip-ranges/get': 'https://ip-ranges.datadoghq.com/',
    },
    operationCount: null,
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
  sourceLabel:
    'https://raw.githubusercontent.com/DataDog/datadog-api-client-typescript/master/.generator/schemas/v1/openapi.yaml',
  sourceHost: 'raw.githubusercontent.com',
  serverName: 'api.datadoghq.com',
  allowedRoutes: ['none'],
  connectAvailable: false,
  connectLaterAvailable: false,
}

/** A shared account, signed in, with permissions worth naming before Add. */
const signedIn: SetupView = {
  ...datadog,
  credentialMethod: 'oauth',
  allowedRoutes: ['caller', 'fixed'],
  connectAvailable: true,
  connectLaterAvailable: true,
  authorized: true,
  grantedScope: 'metrics_read monitors_read',
  configuration: {
    ...datadog.configuration!,
    application: { mode: 'manual', clientId: 'manual-client' },
    values: { ...datadog.configuration!.values, authentication: 'oauth' },
  },
}

const workspaceAttempt: CreationAttempt = {
  state: 'draft',
  kind: 'workspace',
  setupId: 'setup-1',
  configure: false,
  operationsAccepted: true,
  account: null,
  namespace: 'datadog_v1',
}

function Account({
  attempt = workspaceAttempt,
  setup = datadog,
  ...rest
}: {
  attempt?: CreationAttempt
  setup?: SetupView
  pending?: boolean
  result?: Parameters<typeof AccountStep>[0]['result']
}) {
  return (
    <AccountStep
      attempt={attempt}
      setup={setup}
      basePath="/workspaces/demo/providers/new"
      form={form}
      pending={false}
      callbackUrl="https://coral.test/providers/callback"
      backPath="/workspaces/demo/providers/new?setup=setup-1"
      {...rest}
    />
  )
}

const meta = {
  title: 'Management/Providers/Account',
  component: Account,
  parameters: { layout: 'padded' },
  // The step submits a Form and links back to earlier steps, so it needs a router.
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: Story,
          action: () => ({ error: null, credential: null }),
        },
      ])
      return <Stub initialEntries={['/workspaces/demo/providers/new?setup=setup-1']} />
    },
  ],
} satisfies Meta<typeof Account>
export default meta
type Story = StoryObj<typeof meta>

/** No account needed: the shortest last step there is. */
export const NoAccount: Story = { args: {} }

/** A shared account that granted named permissions. */
export const SharedAccount: Story = {
  args: {
    setup: signedIn,
    attempt: { ...workspaceAttempt, account: 'fixed', accountGrant: 'grant-1' },
  },
}

/** Each caller signs in for themselves, so nothing is connected yet. */
export const CallerAccount: Story = {
  args: {
    setup: { ...signedIn, authorized: false, grantedScope: '' },
    attempt: { ...workspaceAttempt, account: 'caller' },
  },
}

/**
 * A sign-in the person just completed on their own behalf. The route is in the
 * URL but the grant is not, so the step keeps the sign-in rather than offering
 * to give it away.
 */
export const KeptSignIn: Story = {
  args: {
    setup: signedIn,
    attempt: { ...workspaceAttempt, account: 'caller' },
  },
}

/** A local deployment: only you and your agents use the account. */
export const LocalDeployment: Story = {
  args: KeptSignIn.args,
  decorators: [
    (Story) => (
      <LocalDeploymentContext value>
        <Story />
      </LocalDeploymentContext>
    ),
  ],
}

/**
 * A device sign-in in flight for "Connect now". The service opens
 * in a new tab, and this step shows the code to enter there.
 */
export const DeviceSignIn: Story = {
  args: {
    setup: {
      ...signedIn,
      authorized: false,
      grantedScope: '',
      credentialMethod: 'device',
      authorization: {
        userCode: 'ABCD-EFGH',
        state: 'pending',
        requestedScopes: [],
        bootstrap: 'manual',
        failure: '',
        clientId: 'manual-client',
        expiresAt: '',
      },
    },
    attempt: { ...workspaceAttempt, account: 'caller', returnFromConnect: true },
  },
}

/** An account that cannot refresh: the step says so before Add, not after. */
export const AccountWithoutRefresh: Story = {
  args: {
    setup: { ...signedIn, automaticRefresh: false },
    attempt: { ...workspaceAttempt, account: 'fixed', accountGrant: 'grant-1' },
  },
}

/** Adding is in flight: nothing can be changed while Coral applies it. */
export const Adding: Story = { args: { pending: true } }

/** A retry after Coral did not answer: the choices are frozen. */
export const Retrying: Story = {
  args: {
    attempt: {
      state: 'frozen',
      kind: 'workspace',
      setupId: 'setup-1',
      configurationRevision: '1',
      requestId: 'storybook-request',
      account: 'none',
      namespace: 'datadog_v1',
    } as CreationAttempt,
  },
}

/** Coral rejected the publication: the step offers only a way to start over. */
export const Failed: Story = {
  args: {
    result: {
      failed: 'A provider already uses this namespace.',
      error: '',
      credential: null,
    },
  },
}
