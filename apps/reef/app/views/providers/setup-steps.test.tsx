/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createRoutesStub } from 'react-router'
import { describe, expect, it } from 'vitest'

import type { ReactNode } from 'react'

import { DiscoveryPrompt, SetupSecretForm } from './setup-steps'
import { UpdateAuthorization } from './update-authorization'
import type { ConfigurationReviewView, SetupView } from './types'

const setup: SetupView = {
  id: '11111111-1111-4111-8111-111111111111',
  kind: 'openapi',
  state: 'ready',
  credentialMethod: 'header',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'https://raw.example.test/openapi.json',
  sourceHost: 'raw.example.test',
  serverName: '',
  allowedRoutes: ['caller', 'fixed'],
  connectAvailable: true,
  connectLaterAvailable: true,
}

const form = { csrf: 'test-csrf', requestId: 'test-request' }

function renderForm(element: ReactNode) {
  const Stub = createRoutesStub([{ path: '/', Component: () => element }])
  render(<Stub initialEntries={['/']} />)
}

describe('where typed-in credentials go', () => {
  // Catches: an agent's addProvider pointing credentials at a server the
  // person never sees before they paste a secret.
  it.each([
    [
      'every host the operations call, once each',
      {
        configuration: {
          destinations: {
            listChannels: 'https://discord.com/api/v10/channels',
            sendMessage: 'https://discord.com/api/v10/messages',
            upload: 'https://cdn.discord.test/upload',
          },
        } as unknown as ConfigurationReviewView,
      },
      'Coral sends what you enter here to discord.com, cdn.discord.test.',
    ],
    [
      'the MCP server',
      { kind: 'mcp' as const, sourceHost: 'mcp.example.test' },
      'Coral sends what you enter here to mcp.example.test.',
    ],
  ])('names %s', (_, over, expected) => {
    renderForm(<SetupSecretForm setup={{ ...setup, ...over }} form={form} pending={false} />)
    expect(screen.getByText(expected)).toBeInTheDocument()
  })

  it('says nothing when no destination is known yet', () => {
    renderForm(<SetupSecretForm setup={setup} form={form} pending={false} />)
    expect(screen.queryByText(/Coral sends what you enter here/)).not.toBeInTheDocument()
  })

  // Catches: the form that replaces a provider's shared credentials taking a
  // secret without naming where it goes.
  it("names the destination when you replace a provider's credentials", () => {
    renderForm(
      <UpdateAuthorization
        setup={{ ...setup, kind: 'mcp', sourceHost: 'mcp.example.test' }}
        form={form}
        pending={false}
        callbackUrl="https://coral.test/callback"
      />,
    )
    expect(
      screen.getByText('Coral sends what you enter here to mcp.example.test.'),
    ).toBeInTheDocument()
  })
})

describe('signing in to read the operations', () => {
  // Catches: the sign-in dialog opening over a required client secret, so its
  // button fails validation on a field you cannot reach.
  it('asks for the client secret before the dialog covers it', async () => {
    const user = userEvent.setup()
    const oauth: SetupView = {
      ...setup,
      state: 'awaiting_authorization',
      credentialMethod: 'oauth',
      serverName: 'Weather',
      configuration: {
        revision: '1',
        confirmed: true,
        automaticClientAvailable: false,
        values: {
          displayName: 'Weather',
          authentication: 'oauth',
          tokenUrl: 'https://auth.example.test/token',
        },
        application: {
          mode: 'manual',
          clientId: 'owner-app',
          authentication: 'post',
          pkce: 's256',
          parameters: '',
        },
      } as ConfigurationReviewView,
    }
    const Stub = createRoutesStub([
      {
        path: '/',
        Component: () => (
          <DiscoveryPrompt
            setup={oauth}
            form={{ csrf: 'test-csrf', requestId: 'test-request' }}
            pending={false}
            callbackUrl="https://coral.test/callback"
          />
        ),
      },
    ])
    render(<Stub initialEntries={['/']} />)

    await user.click(screen.getByRole('button', { name: 'Sign in with Weather' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()

    await user.type(screen.getByLabelText('Client secret'), 'application-secret')
    await user.click(screen.getByRole('button', { name: 'Sign in with Weather' }))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })
})
