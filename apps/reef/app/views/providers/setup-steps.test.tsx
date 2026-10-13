/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { CredentialDestination } from './setup-steps'
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
}

describe('where typed-in credentials go', () => {
  // Catches: an agent's createProvider pointing credentials at a server the
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
    render(<CredentialDestination setup={{ ...setup, ...over }} />)
    expect(screen.getByText(expected)).toBeInTheDocument()
  })

  it('says nothing when no destination is known yet', () => {
    const { container } = render(<CredentialDestination setup={setup} />)
    expect(container).toBeEmptyDOMElement()
  })
})
