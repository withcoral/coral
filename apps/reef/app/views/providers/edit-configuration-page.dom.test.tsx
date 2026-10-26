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
  readable: true,
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
  allowedRoutes: ['none'],
  connectAvailable: false,
  connectLaterAvailable: false,
  configuration: review,
}
const providerPath = '/workspaces/demo/providers/records'

function renderPage({
  page = setup,
  task = 'configuration',
  account = 'none',
  local = false,
  source,
}: {
  page?: SetupView
  task?: UpdateTask
  account?: AccountChoice
  local?: boolean
  source?: { url: string; kind: 'mcp' | 'url' }
} = {}) {
  const intents: string[] = []
  // What each Start again sends to open a new Setup.
  const prepares: Record<string, FormDataEntryValue>[] = []
  // Whether each publication accepted that accounts may stop working.
  const consents: string[] = []
  const Stub = createRoutesStub([
    {
      path: `${providerPath}/update`,
      Component: () => (
        <EditConfigurationPage
          currentAccount={account}
          displayName="Records"
          etag="v2"
          source={source}
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
        if (fields.get('intent') === 'prepare') prepares.push(Object.fromEntries(fields))
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
  return { intents, consents, prepares }
}

const rename = async () => {
  const name = screen.getByRole('textbox', { name: /^Display name/ })
  await userEvent.clear(name)
  await userEvent.type(name, 'Renamed')
}

const saveButton = () => screen.getByRole('button', { name: 'Save' })

// Catches: Cancel asking about changes that do not exist.
it('cancels at once when nothing changed', async () => {
  const page = renderPage()
  await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
  await screen.findByRole('heading', { name: 'Provider' })
  expect(page.intents).toEqual(['cancel'])
})

const cancel = () => userEvent.click(screen.getByRole('button', { name: 'Cancel' }))

// Catches: Cancel or a link silently dropping an edit, a Keep editing that
// still leaves, or a discard that leaves the Setup open. A correction to an
// account change that Coral sent back to the form is an edit too.
it.each([
  ['Cancel', 'configuration', cancel, 'Provider'],
  [
    'a link to another page',
    'configuration',
    () => userEvent.click(screen.getByRole('link', { name: 'Providers' })),
    'Providers',
  ],
  ['Cancel in an account change', 'accounts', cancel, 'Provider'],
] as const)('asks before %s discards changes', async (_, task, leave, destination) => {
  const page = renderPage({ task })
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
      allowedRoutes: ['caller', 'fixed'],
      connectAvailable: true,
      connectLaterAvailable: true,
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
      allowedRoutes: ['caller', 'fixed'],
      connectAvailable: true,
      connectLaterAvailable: true,
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
      allowedRoutes: ['caller', 'fixed'],
      connectAvailable: true,
      connectLaterAvailable: true,
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

// Catches: a refresh hiding what the new source changed, or Edit
// configuration listing the saved source's own values as changes.
it.each([
  ['a refresh', 'refresh', true],
  ['Edit configuration', 'configuration', false],
] as const)('compares saved and new values in %s: %s', async (_, task, shown) => {
  renderPage({
    page: {
      ...setup,
      configuration: {
        ...review,
        differences: [{ field: 'display_name', saved: 'Records', document: 'Records v2' }],
      },
    },
    task,
  })
  const open = screen.queryByRole('button', { name: 'Review changes' })
  expect(open !== null).toBe(shown)
  if (!open) return
  await userEvent.click(open)
  const dialog = await screen.findByRole('dialog', { name: 'Review changes' })
  const table = within(dialog).getByRole('table', { name: 'Saved and new values' })
  const [, row] = within(table).getAllByRole('row')
  expect(
    within(row)
      .getAllByRole('cell')
      .map((cell) => cell.textContent),
  ).toEqual(['Name', 'Records', 'Records v2'])
})

// Catches: Save publishing a configuration you did not change, or staying off
// after you correct one that Coral sent back to an account change.
it.each(['configuration', 'accounts'] as const)(
  'turns on Save only after you change the configuration: %s',
  async (task) => {
    renderPage({ task })
    expect(saveButton()).toHaveAttribute('aria-disabled', 'true')
    await rename()
    expect(saveButton()).not.toHaveAttribute('aria-disabled')
  },
)

// Catches: an expired Setup that strands you, or a Start again that opens the
// wrong task, rereads the saved source in place of a replacement, or carries the
// provider version it began with.
it.each([
  ['an edit', 'configuration', {}, { source: 'current' }],
  ['an account change', 'accounts', {}, { source: 'current' }],
  [
    'a replacement',
    'refresh',
    {
      kind: 'mcp',
      sourceLabel: 'https://records.example.com/v2/mcp',
      sourceHost: 'records.example.com',
    },
    { source: 'mcp', sourceUrl: 'https://records.example.com/v2/mcp' },
  ],
] as const)('starts %s again after its Setup expires', async (_, task, read, sent) => {
  const page = renderPage({
    page: { ...setup, ...read, state: 'expired' },
    task,
    source: { url: 'https://records.example.com/mcp', kind: 'mcp' },
  })
  expect(screen.getByText('This setup has expired')).toBeInTheDocument()

  await userEvent.click(screen.getByRole('button', { name: 'Start again' }))
  await screen.findByRole('heading', { name: 'Provider' })
  expect(page.prepares).toEqual([
    expect.objectContaining({ ...sent, task, etag: 'v2', intent: 'prepare' }),
  ])
})

// Catches: a Start again that reads the saved URL, because an uploaded document is not kept.
it('sends you to settings to read an uploaded document again', () => {
  renderPage({
    page: { ...setup, state: 'expired' },
    task: 'refresh',
    source: { url: 'https://records.example.com/openapi.json', kind: 'url' },
  })
  expect(screen.queryByRole('button', { name: 'Start again' })).not.toBeInTheDocument()
  expect(screen.getByRole('link', { name: 'Choose a source' })).toHaveAttribute(
    'href',
    `${providerPath}/settings`,
  )
})

// Catches: a form for a source Coral cannot read, where every save fails, in
// place of the error and a way to read the source again.
it('offers only a new read when Coral cannot read the source', async () => {
  const page = renderPage({
    page: {
      ...setup,
      sourceLabel: 'https://records.example.com/openapi.json',
      sourceHost: 'records.example.com',
      configuration: { ...review, diagnostic: 'duplicate_operation_key', readable: false },
    },
    task: 'refresh',
  })
  const error = screen.getByRole('alert')
  expect(within(error).getByText('Cannot read this document')).toBeVisible()
  expect(error).toHaveTextContent('Error code: duplicate_operation_key')
  expect(screen.queryByRole('textbox', { name: /^Display name/ })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Next' })).not.toBeInTheDocument()

  await userEvent.click(within(error).getByRole('button', { name: 'Start again' }))
  await screen.findByRole('heading', { name: 'Provider' })
  expect(page.prepares).toEqual([
    expect.objectContaining({
      source: 'url',
      sourceUrl: 'https://records.example.com/openapi.json',
      task: 'refresh',
    }),
  ])
})
