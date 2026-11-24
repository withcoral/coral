/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import {
  OrganizationSecretStoresPage,
  type SecretStoreRow,
} from './organization-secret-stores-page'

const store = (name: string, kind: string | null, blockers: string[] = []): SecretStoreRow => ({
  name,
  kind,
  blockers,
  workspaces: [],
})

const STORES = [
  store('vault', 'Vault KV v2'),
  store('retired', null, ['Removed from config']),
  store('Aws', 'AWS Secrets Manager'),
  store('research', 'Vault KV v2', ['Needs Enterprise']),
]

/** The store names, in the order of their rows. */
function shown() {
  const table = screen.getByRole('table', { name: 'Secret stores' })
  return within(table)
    .getAllByRole('row')
    .slice(1)
    .map((row) => within(row).getAllByRole('cell')[0].textContent)
}

// Catches: stores Coral cannot read lost among the available ones, and a store that config
// removed sorted by a kind it does not show.
it.each([
  [[], ['Aws', 'research', 'retired', 'vault']],
  // Needs Enterprise, Removed from config, then the available stores by name.
  [['Status'], ['research', 'retired', 'Aws', 'vault']],
  // AWS Secrets Manager, Unknown, then the two Vault KV v2 stores by name.
  [['Kind'], ['Aws', 'retired', 'research', 'vault']],
  [
    ['Kind', 'Kind'],
    ['research', 'vault', 'retired', 'Aws'],
  ],
])('after sorting by %j, lists %j', async (clicks, order) => {
  renderRoute(
    <OrganizationSecretStoresPage
      stores={STORES}
      workspaces={[]}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
  )
  for (const column of clicks) {
    await userEvent.click(screen.getByRole('button', { name: `Sort by ${column.toLowerCase()}` }))
  }
  expect(shown()).toEqual(order)
})
