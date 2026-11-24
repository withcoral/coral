/** @vitest-environment happy-dom */
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import {
  OrganizationSecretStoresPage,
  type SecretStoreRow,
} from './organization-secret-stores-page'

const store = (
  name: string,
  kind: string | null,
  blockers: string[] = [],
  workspaces: string[] = [],
): SecretStoreRow => ({
  name,
  kind,
  blockers,
  workspaces: workspaces.map((displayName) => ({
    name: `workspaces/${displayName.toLowerCase()}`,
    displayName,
    archived: false,
  })),
})

const STORES = [
  store('vault', 'Vault KV v2', [], ['Support']),
  store('retired', null, ['Removed from config']),
  {
    // Two workspaces share a name, so each label adds the start of its ID.
    ...store('Aws', 'AWS Secrets Manager'),
    workspaces: [
      {
        name: 'workspaces/1a2b3c4d-0000-4000-8000-000000000001',
        displayName: 'Ops',
        archived: false,
      },
      {
        name: 'workspaces/9f8e7d6c-0000-4000-8000-000000000002',
        displayName: 'Ops',
        archived: false,
      },
    ],
  },
  store('research', 'Vault KV v2', ['Needs Enterprise']),
]

function renderPage() {
  renderRoute(
    <OrganizationSecretStoresPage
      stores={STORES}
      workspaces={[]}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
  )
}

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
  renderPage()
  for (const column of clicks) {
    await userEvent.click(screen.getByRole('button', { name: `Sort by ${column.toLowerCase()}` }))
  }
  expect(shown()).toEqual(order)
})

// Catches: a filter that misses a store by the kind or the workspace label its row shows,
// or a status filter that keeps a store Coral cannot read among the available ones.
it.each([
  ['retired', 'All statuses', ['retired']],
  ['secrets manager', 'All statuses', ['Aws']],
  // The kind a store config removed shows.
  ['unknown', 'All statuses', ['retired']],
  // The ID start that tells two workspaces of one name apart.
  ['9f8e7d6c', 'All statuses', ['Aws']],
  ['support', 'All statuses', ['vault']],
  ['', 'Available', ['Aws', 'vault']],
  ['', 'Blocked', ['research', 'retired']],
  ['vault kv', 'Blocked', ['research']],
])('filtering by %j and %s lists %j', async (text, status, names) => {
  renderPage()
  if (text)
    await userEvent.type(screen.getByRole('searchbox', { name: 'Filter secret stores' }), text)
  if (status !== 'All statuses') {
    await userEvent.click(screen.getByRole('button', { name: /^Filter secret stores by status/ }))
    await userEvent.click(await screen.findByRole('menuitemradio', { name: status }))
  }
  await waitFor(() => expect(shown()).toEqual(names))
})
