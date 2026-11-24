/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { AgentProviders, type AgentProviderView } from './agent-providers'

const provider = (
  name: string,
  account: AgentProviderView['account'],
  blockers: string[] = [],
): AgentProviderView => ({ provider: name, name, namespace: name, account, blockers })
const separate = (state: 'ready' | 'reconnect' | 'disconnected') =>
  ({ kind: 'separate', state, canConnect: true, path: '/accounts' }) as const

const PROVIDERS = [
  provider('Zendesk', { kind: 'shared', path: '/accounts' }),
  provider('Ledger', separate('reconnect'), ['Connect an account for this agent']),
  provider('attio', separate('ready')),
  provider('Notion', { kind: 'none' }),
  provider('Okta', separate('disconnected'), ['Provider disabled', 'Connect an account']),
  provider('Github', separate('disconnected'), ['Connect an account for this agent']),
]

/** The provider names, in the order of their rows. */
function shown() {
  const table = screen.getByRole('table', { name: 'Provider accounts' })
  return within(table)
    .getAllByRole('row')
    .slice(1)
    .map((row) => PROVIDERS.find(({ name }) => within(row).queryByText(name))?.name)
}

// Catches: providers that need you lost among the ready ones, and a heading that does
// not sort by the value its column shows.
it.each([
  // Not connected, Provider disabled, Reconnect, then the ready providers by name.
  [[], ['Github', 'Okta', 'Ledger', 'attio', 'Notion', 'Zendesk']],
  [['Status'], ['attio', 'Notion', 'Zendesk', 'Ledger', 'Okta', 'Github']],
  // Individual, None, Shared.
  [['Account'], ['attio', 'Github', 'Ledger', 'Okta', 'Notion', 'Zendesk']],
  [
    ['Provider', 'Provider'],
    ['Zendesk', 'Okta', 'Notion', 'Ledger', 'Github', 'attio'],
  ],
])('after sorting by %j, lists %j', async (clicks, order) => {
  renderRoute(
    <AgentProviders
      providers={PROVIDERS}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      canConnect={false}
    />,
  )
  for (const column of clicks) {
    await userEvent.click(screen.getByRole('button', { name: `Sort by ${column.toLowerCase()}` }))
  }
  expect(shown()).toEqual(order)
})
