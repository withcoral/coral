/** @vitest-environment happy-dom */

import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createRoutesStub, redirect } from 'react-router'
import { expect, it } from 'vitest'

import { LocalDeploymentContext } from '@/lib/deployment'
import type { UpdateTask } from '@/lib/provider-publication-attempt/model'
import { EditConfigurationPage } from './edit-configuration-page'
import type { AccountChoice, ConfigurationReviewView, SetupView } from './types'

const review: ConfigurationReviewView = {
  revision: '1',
  confirmed: true,
  authenticationMethods: ['none'],
  discoveryAuthorizationRequired: false,
  automaticClientAvailable: false,
  issuer: '',
  resource: '',
  differences: [],
  interruptsAccess: null,
  updating: true,
  values: {
    displayName: 'Records',
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
  documentDisplayName: 'Records',
  servers: [],
  destinations: {},
  operations: [],
  unavailableOperations: [],
  operationCount: 1,
  diagnostic: '',
  errors: {},
}
const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'openapi',
  state: 'awaiting_configuration',
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
const providerPath = '/workspaces/demo/providers/records'

function renderPage({
  page = setup,
  task = 'configuration',
  account = 'none',
  local = false,
}: { page?: SetupView; task?: UpdateTask; account?: AccountChoice; local?: boolean } = {}) {
  const intents: string[] = []
  // Whether each publication accepted that accounts may stop working.
  const consents: string[] = []
  const Stub = createRoutesStub([
    {
      path: `${providerPath}/update`,
      Component: () => (
        <EditConfigurationPage
          currentAccount={account}
          displayName="Records"
          basePath={`${providerPath}/update`}
          cancelPath={providerPath}
          providersPath="/workspaces/demo/providers"
          setup={page}
          attempt={{
            kind: 'update',
            state: 'draft',
            setupId: page.id,
            configure: false,
            operationsAccepted: false,
            task,
            etag: 'v1',
            allowIncompatibleConfiguration: false,
          }}
          form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
        />
      ),
      action: async ({ request }) => {
        const fields = await request.formData()
        intents.push(String(fields.get('intent')))
        if (fields.get('intent') === 'publish')
          consents.push(
            String(
              fields.get('allowIncompatibleConfiguration') ??
                new URL(request.url).searchParams.get('allowIncompatibleConfiguration'),
            ),
          )
        // As Reef's action does: a discard through a link goes to that link.
        return redirect(String(fields.get('leaveTo') ?? providerPath))
      },
    },
    { path: providerPath, Component: () => <h1>Provider</h1> },
    { path: '/workspaces/demo/providers', Component: () => <h1>Providers</h1> },
  ])
  render(
    <LocalDeploymentContext value={local}>
      <Stub initialEntries={[`${providerPath}/update`]} />
    </LocalDeploymentContext>,
  )
  return { intents, consents }
}

const rename = async () => {
  const name = screen.getByRole('textbox', { name: /^Display name/ })
  await userEvent.clear(name)
  await userEvent.type(name, 'Renamed')
}

// Catches: Cancel asking about changes that do not exist.
it('cancels at once when nothing changed', async () => {
  const page = renderPage()
  await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
  await screen.findByRole('heading', { name: 'Provider' })
  expect(page.intents).toEqual(['cancel'])
})

// Catches: Cancel or a link silently dropping an edit, a Keep editing that
// still leaves, or a discard that leaves the Setup open.
it.each([
  ['Cancel', () => userEvent.click(screen.getByRole('button', { name: 'Cancel' })), 'Provider'],
  [
    'a link to another page',
    () => userEvent.click(screen.getByRole('link', { name: 'Providers' })),
    'Providers',
  ],
])('asks before %s discards changes', async (_, leave, destination) => {
  const page = renderPage()
  await rename()

  await leave()
  const dialog = await screen.findByRole('dialog', { name: 'Discard your changes?' })
  await userEvent.click(screen.getByRole('button', { name: 'Keep editing' }))
  expect(dialog).not.toBeInTheDocument()
  expect(screen.getByRole('textbox', { name: /^Display name/ })).toHaveValue('Renamed')
  expect(page.intents).toEqual([])

  await leave()
  await userEvent.click(await screen.findByRole('button', { name: 'Discard changes' }))
  await screen.findByRole('heading', { name: destination })
  expect(page.intents).toEqual(['cancel'])
})

// Catches: a local deployment, where nobody else can sign in, telling you that
// other users reconnect their own accounts.
it.each([
  ['a signed-in deployment', false, true],
  ['a local deployment', true, false],
])('asks only the right people to reconnect in %s', async (_, local, others) => {
  renderPage({
    page: {
      ...setup,
      state: 'ready',
      credentialMethod: 'bearer',
      configuration: { ...review, interruptsAccess: true },
    },
    task: 'refresh',
    account: 'caller',
    local,
  })
  await userEvent.click(screen.getByRole('button', { name: 'Save' }))
  const dialog = await screen.findByRole('dialog', { name: 'Save changes?' })
  expect(dialog).toHaveTextContent('Reconnect each agent account.')
  expect(dialog.textContent?.includes('Other users reconnect their own accounts.')).toBe(others)
})

// Catches: a reload or Back during a shared-account sign-in hiding that
// sign-in behind "Keep the current account", so its recovery controls vanish.
it.each([
  ['in progress', 'pending', 'Connect another account'],
  ['failed', 'failed', 'Connect another account'],
  ['complete', 'succeeded', 'Keep the current account'],
] as const)('reopens a sign-in that is %s on its account', (_, state, chosen) => {
  renderPage({
    page: {
      ...setup,
      state: 'ready',
      credentialMethod: 'oauth',
      configuration: { ...review, interruptsAccess: false },
      authorization: {
        state,
        requestedScopes: [],
        bootstrap: 'document',
        failure: state === 'failed' ? 'access_denied' : '',
        clientId: 'client',
        expiresAt: '',
      },
    },
    task: 'refresh',
    account: 'fixed',
  })
  expect(screen.getByRole('radio', { name: chosen })).toBeChecked()
})

// Catches: Save publishing without consent when Coral cannot check access, so
// Coral rejects a change that does break existing accounts.
it.each([
  ['cannot check', null, 'Accounts may need to reconnect after you save.', 'yes'],
  ['finds nothing breaks', false, 'Save the confirmed configuration.', 'no'],
] as const)('asks consent to save when Coral %s', async (_, interruptsAccess, says, consent) => {
  const page = renderPage({
    page: {
      ...setup,
      state: 'ready',
      credentialMethod: 'bearer',
      configuration: { ...review, interruptsAccess },
    },
    account: 'caller',
  })
  const dialog = await screen.findByRole('dialog', { name: 'Save changes?' })
  expect(dialog).toHaveTextContent(says)
  await userEvent.click(within(dialog).getByRole('button', { name: 'Save' }))
  await screen.findByRole('heading', { name: 'Provider' })
  expect(page.consents).toEqual([consent])
})

// Catches: a refresh that needs a configuration review hiding what the new
// document changed, the one thing the review is for.
it('shows what a new read changed before you confirm it', () => {
  renderPage({
    page: {
      ...setup,
      configuration: {
        ...review,
        differences: [{ field: 'display_name', saved: 'Records', document: 'Records v2' }],
      },
    },
    task: 'refresh',
  })
  expect(screen.getByRole('heading', { name: 'Changes in the new input' })).toBeVisible()
  expect(screen.getByRole('button', { name: 'Next' })).toBeVisible()
})
