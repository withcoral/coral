import { expect, it } from 'vitest'

import { LocalDeploymentContext } from '@/lib/deployment'
import { markupRoute } from '@/test-support/route'
import type { ConfigurationReviewView, SetupView } from './types'
import { UpdatePage } from './update-page'

const review: ConfigurationReviewView = {
  revision: '2',
  confirmed: true,
  authenticationMethods: ['bearer'],
  discoveryAuthorizationRequired: false,
  automaticClientAvailable: false,
  issuer: '',
  resource: '',
  differences: [],
  // The new configuration stops existing accounts from working.
  interruptsAccess: true,
  updating: true,
  values: {
    displayName: 'Example',
    authentication: 'bearer',
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
  errors: {},
}
const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'openapi',
  state: 'ready',
  credentialMethod: 'bearer',
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

// Catches: a local deployment, where nobody else can sign in, telling you that
// other users reconnect their own accounts.
it.each([
  ['a signed-in deployment', false, true],
  ['a local deployment', true, false],
])('asks only the right people to reconnect in %s', (_, local, others) => {
  const html = markupRoute(
    <LocalDeploymentContext value={local}>
      <UpdatePage
        currentAccount="caller"
        displayName="Example"
        basePath="/workspaces/demo/providers/prov-example/update"
        cancelPath="/workspaces/demo/providers/prov-example"
        providersPath="/workspaces/demo/providers"
        setup={setup}
        attempt={{
          kind: 'update',
          state: 'draft',
          setupId: setup.id,
          configure: false,
          operationsAccepted: true,
          etag: 'prov-etag',
          allowIncompatibleConfiguration: false,
        }}
        form={{ csrf: 'test-csrf', requestId: '22222222-2222-4222-8222-222222222222' }}
      />
    </LocalDeploymentContext>,
  )
  expect(html).toContain('Reconnect each agent account.')
  expect(html.includes('Other users reconnect their own accounts.')).toBe(others)
})
