/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import { act } from 'react'
import { describe, expect, it, vi } from 'vitest'

import { LocalDeploymentContext } from '@/lib/deployment'
import type { CreationAttempt } from '@/lib/provider-publication-attempt/model'
import { markupRoute, renderRoute } from '@/test-support/route'
import { AccountStep } from './account-step'
import type { ConfigurationReviewView, SetupView } from './types'

const review: ConfigurationReviewView = {
  revision: '1',
  confirmed: true,
  authenticationMethods: ['none', 'bearer', 'oauth'],
  discoveryAuthorizationRequired: false,
  automaticClientAvailable: false,
  issuer: '',
  resource: '',
  differences: [],
  interruptsAccess: null,
  updating: false,
  values: {
    displayName: 'Example',
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
  documentDisplayName: 'Example',
  servers: [],
  destinations: {},
  operations: [],
  unavailableOperations: [],
  operationCount: 0,
  diagnostic: '',
  readable: true,
  errors: {},
}
const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
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
  configuration: review,
}
const held = {
  credentialMethod: 'bearer' as const,
  authorized: true,
  provisionalGrantId: '33333333-3333-4333-8333-333333333333',
}
const attempt: CreationAttempt = {
  state: 'draft',
  kind: 'workspace',
  setupId: setup.id,
  configure: false,
  operationsAccepted: true,
  account: null,
  namespace: 'example',
}

function render(
  over: Partial<SetupView> = {},
  over2: Partial<CreationAttempt> = {},
  local = false,
) {
  return markupRoute(
    <LocalDeploymentContext value={local}>
      <AccountStep
        attempt={{ ...attempt, ...over2 } as CreationAttempt}
        setup={{ ...setup, ...over }}
        basePath="/workspaces/demo/providers/new"
        form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
        pending={false}
        callbackUrl="https://coral.test/callback"
        backPath="/workspaces/demo/providers/new?setup=1"
      />
    </LocalDeploymentContext>,
  )
}

describe('the account step', () => {
  // Catches: a local deployment, where nobody else can sign in, describing
  // accounts that other people share or connect.
  it.each([
    ['a credential-free provider', {}, 'you and your agents can use it'],
    [
      'no sign-in in hand',
      { credentialMethod: 'bearer' as const },
      'You connect one for each agent that uses it',
    ],
    ['a sign-in in hand', held, 'You and your agents call the provider with the account'],
  ])('speaks only of you and your agents locally, with %s', (_, over, expected) => {
    const html = render(over, {}, true)
    expect(html).toContain(expected)
    expect(html).not.toMatch(/everyone|other people/i)
  })

  it('shows a credential-free provider its one answer, and adds it', () => {
    const html = render()
    expect(html).toContain('No account needed')
    expect(html).toContain('everyone in this workspace can use it')
    // The one answer is an option, not a sentence, and nothing else is offered.
    expect(html).toContain('aria-checked="true"')
    expect(html).not.toContain('One shared account')
    expect(html).toContain('Add provider')
  })

  it('keeps the sign-in in hand by default, and adds the provider from this page', () => {
    const html = render(held)
    // The answer that needs no sign-in commits the grant from this page.
    expect(html).toContain('name="accountGrant" value="33333333-3333-4333-8333-333333333333"')
    expect(html).toContain('Add provider')
    expect(html).not.toContain('>Next<')
  })

  it('keeps a sign-in the person just completed rather than discarding it', () => {
    // A returning OAuth callback names the route but carries no grant of its
    // own. Deriving the answer from the route alone gave the sign-in away.
    const html = render(held, { account: 'caller' })
    expect(html).toContain('name="accountGrant" value="33333333-3333-4333-8333-333333333333"')
    expect(html).toContain('Add provider')
    // Signing in again for an individual account must not move it onto the shared route.
    expect(html).toContain('value="own-connect"')
    expect(html).toContain('name="account" value="caller"')
  })

  it('drops the outcomes that need a sign-in in hand when there is none', () => {
    const html = render({ credentialMethod: 'bearer' })
    expect(html).toContain('Connect an account')
    expect(html).toContain('Connect now')
    expect(html).toContain(
      'Everyone, including you, connects an account when they first use the provider.',
    )
    // Nothing to keep, nothing to replace, and nothing to discard.
    expect(html).not.toContain('Use this account')
    expect(html).not.toContain('Connect now with this account')
    expect(html).not.toContain('Connect now with another account')
    expect(html).not.toContain('You sign in again')
    expect(html).not.toContain('Coral discards this sign-in')
    // Shared use is the default, and it needs a credential before it can add.
    expect(html).toContain('Connect the shared account')
    expect(html).not.toContain('Add provider')
  })

  it('states the committed answer when the attempt is being retried', () => {
    const html = render(held, {
      state: 'frozen',
      requestId: '44444444-4444-4444-8444-444444444444',
      configurationRevision: '1',
      account: 'caller',
      accountGrant: held.provisionalGrantId,
    } as Partial<CreationAttempt>)
    expect(html).toContain('You keep this sign-in for your account')
    expect(html).not.toContain('Whose account does it use?')
  })
})

// A device sign-in in flight, and what the page reads once it finishes in the other tab.
const device: Partial<SetupView> = {
  credentialMethod: 'device',
  authorization: {
    userCode: 'ABCD-EFGH',
    state: 'pending',
    requestedScopes: [],
    bootstrap: 'manual',
    failure: '',
    clientId: 'client',
    expiresAt: '',
  },
}
const deviceDone: Partial<SetupView> = {
  ...device,
  ...held,
  credentialMethod: 'device',
  authorization: { ...device.authorization!, state: 'succeeded' },
}

it.each([
  // Starting the sign-in reloads the page before any account is in hand.
  ['a device sign-in in flight', 'pending', true, 'Connect now'],
  // A failed sign-in keeps the answer, so the person can see the error and retry.
  ['a failed device sign-in', 'failed', true, 'Connect now'],
  // Choosing to connect later after starting a sign-in leaves it pending.
  // The later answer wins over the sign-in it abandoned.
  ['a sign-in abandoned for a later answer', 'pending', false, 'Connect later'],
] as const)(
  'keeps the right answer on reload after %s',
  (_case, state, returnFromConnect, chosen) => {
    renderRoute(() => (
      <AccountStep
        attempt={{ ...attempt, account: 'caller', returnFromConnect }}
        setup={{ ...setup, ...device, authorization: { ...device.authorization!, state } }}
        basePath="/workspaces/demo/providers/new"
        form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
        pending={false}
        callbackUrl="https://coral.test/callback"
        backPath="/workspaces/demo/providers/new?setup=1"
      />
    ))
    // The sign-in code dialog covers the step, so the answer sits behind it.
    expect(
      screen.getByRole('radio', { name: chosen, hidden: true }).getAttribute('aria-checked'),
    ).toBe('true')
  },
)

// A redirect sign-in returns through its callback with the account in hand.
const redirectDone: Partial<SetupView> = {
  ...held,
  credentialMethod: 'oauth',
  authorization: { ...deviceDone.authorization!, userCode: undefined },
}
// A Setup that a Program's addProvider call prepared.
const programSetup: Partial<SetupView> = {
  ...redirectDone,
  request: { namespace: 'weather', route: 'caller' },
}

it.each([
  // The device sign-in lands while this page revalidates in the first tab.
  ['a device sign-in for your own account', 'caller', true, device, deviceDone, true],
  ['a device sign-in for the shared account', 'fixed', true, device, deviceDone, true],
  // The redirect callback reloads this step with the account already in hand.
  ['a redirect sign-in', 'caller', true, redirectDone, redirectDone, true],
  // Add provider is unavailable, so the sign-in cannot send it either.
  ['a sign-in with an invalid namespace', 'caller', true, redirectDone, redirectDone, false, true],
  // The person confirms a Program's provider with Add provider.
  ["a sign-in for a Program's Setup", 'caller', true, programSetup, programSetup, false],
] as const)(
  'adds the provider without asking again after %s',
  async (_case, account, returnFromConnect, before, after, adds, disabled: boolean = false) => {
    const submissions: FormData[] = []
    const { router } = renderRoute(
      (location) => (
        <AccountStep
          attempt={{ ...attempt, account, returnFromConnect }}
          setup={{ ...setup, ...(location.search ? after : before) }}
          basePath="/workspaces/demo/providers/new"
          form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
          pending={false}
          disabled={disabled}
          callbackUrl="https://coral.test/callback"
          backPath="/workspaces/demo/providers/new?setup=1"
        />
      ),
      {
        routes: [
          {
            path: '/workspaces/demo/providers/new',
            action: async ({ request }) => {
              submissions.push(await request.formData())
              return null
            },
          },
        ],
      },
    )
    await act(() => router.navigate('/?done'))
    if (!adds) {
      expect(screen.getByRole('button', { name: 'Add provider' })).toBeTruthy()
      expect(submissions).toEqual([])
      return
    }
    await vi.waitFor(() => expect(submissions).toHaveLength(1))
    const sent = submissions[0]!
    expect(sent.get('intent')).toBe('publish')
    expect(sent.get('account')).toBe(account)
    expect(sent.get('accountGrant')).toBe(held.provisionalGrantId)
  },
)

// Catches: a failed publication returning to this step with no OAuth application
// Coral can set up, and the sign-in button then sending a form that Reef rejects.
// Otherwise attemptProgress sends such a Setup to configuration, not here.
it.each([
  ['the shared account', 'fixed'],
  ['your own account', 'caller'],
] as const)('starts no sign-in to %s until configuration names an application', (_, account) => {
  renderRoute(
    <AccountStep
      attempt={{ ...attempt, account, returnFromConnect: true }}
      setup={{ ...setup, credentialMethod: 'oauth', serverName: 'Gmail' }}
      basePath="/workspaces/demo/providers/new"
      form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
      result={{ error: null, credential: null, failed: 'publication_failed' }}
      pending={false}
      callbackUrl="https://coral.test/callback"
      backPath="/workspaces/demo/providers/new?setup=1"
    />,
  )
  expect(screen.getByRole('button', { name: 'Sign in with Gmail' })).toBeDisabled()
})

it('opens a device sign-in in a new tab, so this page stays to pick up the result', () => {
  renderRoute(
    <AccountStep
      attempt={{ ...attempt, account: 'caller', returnFromConnect: true }}
      setup={{ ...setup, ...device }}
      basePath="/workspaces/demo/providers/new"
      form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
      pending={false}
      callbackUrl="https://coral.test/callback"
      backPath="/workspaces/demo/providers/new?setup=1"
    />,
  )
  // The service opens in a new tab, so this page is still here to revalidate.
  const resume = screen.getByRole<HTMLButtonElement>('button', { name: 'Open sign-in page' })
  expect(resume.form!.getAttribute('target')).toBe('_blank')
})
