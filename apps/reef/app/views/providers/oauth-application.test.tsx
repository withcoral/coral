/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { OAuthApplicationBlock } from './oauth-application'
import type { ConfigurationReviewView, SetupView } from './types'

const review = {
  revision: '1',
  confirmed: true,
  automaticClientAvailable: true,
  values: { displayName: 'Example', authentication: 'oauth', tokenUrl: '' },
} as ConfigurationReviewView
const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'mcp',
  state: 'awaiting_authorization',
  credentialMethod: 'oauth',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'https://example.test/mcp',
  sourceHost: 'example.test',
  serverName: 'example.test',
  configuration: review,
}

/** The application fields the sign-in form posts. */
function posted(over: Partial<ConfigurationReviewView> = {}, setupOver: Partial<SetupView> = {}) {
  render(
    <form aria-label="Sign in">
      <OAuthApplicationBlock
        setup={{ ...setup, ...setupOver, configuration: { ...review, ...over } }}
        callbackUrl="https://coral.test/callback"
        clientSecretInput={
          <label>
            Client secret
            <input name="clientSecret" required />
          </label>
        }
      />
    </form>,
  )
  const form = new FormData(screen.getByRole('form', { name: 'Sign in' }) as HTMLFormElement)
  return { application: form.get('application'), clientId: form.get('clientId') }
}

describe('the sign-in application', () => {
  // Catches: an agent's addProvider confirms the configuration without an
  // application, and the sign-in form then posts none, so Reef rejects it; or
  // it posts automatic where Coral cannot register one, so sign-in fails.
  it.each([
    ['lets Coral choose the application when configuration chose none', true, 'automatic'],
    ['asks for an application where Coral cannot register one', false, null],
  ])('%s', (_, automaticClientAvailable, application) => {
    expect(posted({ automaticClientAvailable })).toEqual({ application, clientId: null })
    expect(
      screen.queryByText(
        'Enter the client ID of your OAuth application in configuration before you sign in.',
      ) !== null,
    ).toBe(application === null)
  })

  it('signs in with the application that configuration chose', () => {
    expect(
      posted({
        application: {
          mode: 'manual',
          clientId: 'reef-client',
          authentication: 'none',
          pkce: 's256',
          parameters: '',
        },
      }),
    ).toEqual({ application: 'explicit', clientId: 'reef-client' })
  })

  // Catches: an agent's addProvider names a confidential application but
  // never its secret, and the sign-in then has no way to take it.
  it.each([
    ['asks for the secret nobody saved yet', false, true],
    ['keeps the saved secret', true, false],
  ])('%s', (_, clientSecretConfigured, asks) => {
    posted(
      {
        application: {
          mode: 'manual',
          clientId: 'owner-app',
          authentication: 'post',
          pkce: 's256',
          parameters: '',
        },
      },
      { kind: 'openapi', state: 'ready', clientSecretConfigured },
    )
    const secret = screen.queryByLabelText('Client secret')
    expect(secret !== null).toBe(asks)
    if (secret) expect(secret).toBeRequired()
  })

  // Catches: an agent's addProvider choosing OAuth endpoints or servers the
  // person never sees before they sign in, such as a GitHub sign-in whose token
  // goes to another host, or a device sign-in naming its device authorization
  // endpoint as the place you sign in.
  it.each([
    [
      'an oauth sign-in to an MCP server',
      { credentialMethod: 'oauth' },
      {},
      'You sign in at github.com. Coral gets the token from auth.example.test. ' +
        'Coral sends the token to example.test.',
    ],
    [
      'a device sign-in to an MCP server',
      { credentialMethod: 'device' },
      {},
      'Coral gets the token from auth.example.test. Coral sends the token to example.test.',
    ],
    [
      'an oauth sign-in to an API',
      { credentialMethod: 'oauth', kind: 'openapi', sourceHost: 'docs.example.test' },
      {
        destinations: {
          listIssues: 'https://api.attacker.test/issues',
          getUser: 'https://api.attacker.test/user',
        },
      },
      'You sign in at github.com. Coral gets the token from auth.example.test. ' +
        'Coral sends the token to api.attacker.test.',
    ],
  ] as const)('names where %s sends you and the token', (_, setupOver, reviewOver, destination) => {
    posted(
      {
        ...reviewOver,
        values: {
          ...review.values,
          authorizationUrl: 'https://github.com/login/device/code',
          tokenUrl: 'https://auth.example.test/token',
        },
        application: {
          mode: 'manual',
          clientId: 'owner-app',
          authentication: 'none',
          pkce: 's256',
          parameters: '',
        },
      } as Partial<ConfigurationReviewView>,
      setupOver,
    )
    expect(screen.getByText(destination)).toBeInTheDocument()
  })
})
