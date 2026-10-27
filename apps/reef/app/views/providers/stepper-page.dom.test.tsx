/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'

import { StepperPage } from './stepper-page'
import type { SetupView } from './types'

vi.mock('@/wax/components/toast', () => ({ addToast: vi.fn() }))

const basePath = '/workspaces/demo/providers/new'
const typed = 'https://records.example.com/mcp?token=secret'
const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'mcp',
  state: 'expired',
  credentialMethod: 'none',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: typed,
  sourceHost: 'records.example.com',
  serverName: '',
  allowedRoutes: [],
  connectAvailable: false,
  connectLaterAvailable: false,
}

// Catches: a typed URL, which can hold a token, sent in the action's address
// where access logs keep it; or a failed Start again with no way to fix the source.
it('starts a typed URL again without it in the address, and offers the source form after a refusal', async () => {
  const posts: { url: URL; sourceUrl: FormDataEntryValue | null }[] = []
  renderRoute(
    <StepperPage
      workspace={{
        name: 'workspaces/demo',
        displayName: 'Demo',
        state: 'active',
        etag: 'workspace-etag',
        effectiveRole: 'owner',
        currentUser: 'users/test',
        profile: { user: 'users/test', name: 'Test', email: '', local: true },
        mcpEndpoint: '',
        connectEndpoint: '',
        path: '/workspaces/demo',
      }}
      basePath={basePath}
      cancelPath="/workspaces/demo/providers"
      setup={setup}
      attempt={{
        state: 'draft',
        kind: 'workspace',
        setupId: setup.id,
        configure: false,
        operationsAccepted: false,
        account: null,
        namespace: '',
      }}
      callbackUrl=""
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
    {
      entry: `${basePath}?setup=${setup.id}`,
      action: async ({ request }) => {
        const fields = await request.formData()
        posts.push({ url: new URL(request.url), sourceUrl: fields.get('sourceUrl') })
        return { error: 'Coral cannot reach this server.' }
      },
    },
  )
  expect(screen.queryByRole('link', { name: 'Choose a source' })).not.toBeInTheDocument()

  await userEvent.click(screen.getByRole('button', { name: 'Start again' }))
  const choose = await screen.findByRole('link', { name: 'Choose a source' })
  expect(posts).toHaveLength(1)
  expect(posts[0].url.pathname).toBe(basePath)
  expect(posts[0].url.search).not.toContain('records.example.com')
  expect(posts[0].sourceUrl).toBe(typed)
  expect(choose).toHaveAttribute('href', basePath)
  expect(screen.getByRole('button', { name: 'Start again' })).toBeEnabled()
})

const github = 'https://raw.githubusercontent.com/PaddleHQ/paddle-openapi/main/openapi.yaml'
const document: SetupView = {
  ...setup,
  kind: 'openapi',
  state: 'awaiting_configuration',
  sourceLabel: github,
  sourceHost: 'raw.githubusercontent.com',
  configuration: {
    revision: '1',
    confirmed: false,
    authenticationMethods: ['none'],
    discoveryAuthorizationRequired: false,
    automaticClientAvailable: true,
    issuer: '',
    resource: '',
    differences: [],
    interruptsAccess: null,
    updating: false,
    values: {
      displayName: 'Paddle API',
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
    authenticationStatus: 'supported',
    documentDisplayName: 'Paddle API',
    servers: [],
    destinations: {},
    operationCount: null,
    operations: [],
    unavailableOperations: [],
    diagnostic: '',
    readable: true,
    errors: {},
  },
}

function renderConfiguration(namespace = '') {
  renderRoute(
    <StepperPage
      workspace={{
        name: 'workspaces/demo',
        displayName: 'Demo',
        state: 'active',
        etag: 'workspace-etag',
        effectiveRole: 'owner',
        currentUser: 'users/test',
        profile: { user: 'users/test', name: 'Test', email: '', local: true },
        mcpEndpoint: '',
        connectEndpoint: '',
        path: '/workspaces/demo',
      }}
      basePath={basePath}
      cancelPath="/workspaces/demo/providers"
      setup={document}
      attempt={{
        state: 'draft',
        kind: 'workspace',
        setupId: document.id,
        configure: false,
        operationsAccepted: false,
        account: null,
        namespace,
      }}
      callbackUrl=""
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
    { entry: `${basePath}?setup=${document.id}` },
  )
  return {
    displayName: screen.getByRole('textbox', { name: /Display name/ }),
    namespace: screen.getByRole('textbox', { name: /Namespace/ }),
  }
}

// Catches: a namespace named after the host that serves the document, such as
// `raw` for a spec on raw.githubusercontent.com, rather than after the service.
it("suggests the namespace from the document's name, not the host that serves it", () => {
  expect(renderConfiguration().namespace).toHaveValue('paddle_api')
})

// Catches: a renamed provider that keeps the old name's namespace, or a
// namespace the person chose that a later rename overwrites.
it.each([
  { case: 'the offered namespace', namespace: '', typed: null, expected: 'paddle_billing' },
  { case: 'a namespace typed first', namespace: '', typed: 'payments', expected: 'payments' },
  { case: 'a namespace of their own', namespace: 'billing', typed: null, expected: 'billing' },
])('follows the display name only while nobody chose the namespace: $case', async (row) => {
  const fields = renderConfiguration(row.namespace)
  if (row.typed !== null) {
    await userEvent.clear(fields.namespace)
    await userEvent.type(fields.namespace, row.typed)
  }
  await userEvent.clear(fields.displayName)
  await userEvent.type(fields.displayName, 'Paddle Billing')
  expect(fields.namespace).toHaveValue(row.expected)
})
