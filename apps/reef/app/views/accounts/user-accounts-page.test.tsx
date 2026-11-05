/** @vitest-environment happy-dom */
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { data, useLoaderData, type ActionFunctionArgs } from 'react-router'
import { expect, it } from 'vitest'
import type { ActionCount } from '@/lib/activity-live-types'
import type { ActionSignal } from '@/lib/use-action-signal'
import { renderRoute, shellSignal } from '@/test-support/route'
import { workspaceView } from '@/views/providers/fixtures'
import type { UserAccountRow, UserAccountsView } from './types'
import { UserAccountsPage } from './user-accounts-page'

const path = workspaceView.path
const request = `${workspaceView.name}/accountConnectionRequests/linear`
const linear: UserAccountRow = {
  provider: `${workspaceView.name}/providers/linear`,
  name: 'Linear',
  oauth: true,
  state: 'not_connected',
  canConnect: true,
  canDisconnect: false,
  connectionRequest: { name: request, expiresAt: '2099-01-01T00:14:00.000Z' },
  connectPath: '/connect/linear',
  detailPath: '/details/linear',
}
const ledger: UserAccountRow = {
  provider: `${workspaceView.name}/providers/ledger`,
  name: 'Ledger',
  oauth: false,
  state: 'needs_reconnect',
  accountId: 'account',
  canConnect: true,
  canDisconnect: true,
  connectPath: '/connect/ledger',
  detailPath: '/details/ledger',
}
const viewAt = (revision: string, accounts: UserAccountRow[]): UserAccountsView => ({
  workspace: workspaceView,
  path: `${path}/accounts`,
  revision,
  accounts,
  form: { csrf: 'test', requestId: 'test-request' },
})

const counted = (connections: string): ActionCount => ({
  total: '1',
  approvals: '0',
  connections: '1',
  revisions: { approvals: 'approvals', connections },
})
const signalOf = (count: ActionCount): ActionSignal => ({
  counts: { [path]: count },
  connected: true,
})

function LoadedRoute() {
  return <UserAccountsPage view={useLoaderData<UserAccountsView>()} />
}

/** The page as the app shell mounts it: its rows come from a loader that reads `coral.view`,
 * and `resolve` answers the decisions posted to the resolve route. */
async function renderLive(
  coral: { view: UserAccountsView },
  resolve: (form: FormData) => unknown = () => ({ connection: 'declined' }),
) {
  const { wrapper, signal } = shellSignal(signalOf(counted(coral.view.revision!)))
  renderRoute(<LoadedRoute />, {
    csrf: 'test',
    loader: () => coral.view,
    wrapper,
    routes: [
      {
        path: '/activity/resolve',
        action: async ({ request: posted }: ActionFunctionArgs) => resolve(await posted.formData()),
      },
      // Opening the connect dialog starts an authorization here.
      { path: '/connect/:provider', action: () => ({ error: 'Try later.' }) },
    ],
  })
  await screen.findByRole('table', { name: 'Accounts' })
  return { signal }
}

/** A modal dialog hides the rows from assistive technology, but they stay on the page. */
const rowOf = (name: string) =>
  screen.getByRole('row', { name: new RegExp(`^${name} account details`), hidden: true })

it('declines a waiting request from its row, next to Connect', async () => {
  const coral = { view: viewAt('one', [linear, ledger]) }
  const posted: Record<string, FormDataEntryValue>[] = []
  await renderLive(coral, (form) => {
    posted.push(Object.fromEntries(form))
    coral.view = viewAt('two', [{ ...linear, connectionRequest: undefined }, ledger])
    return { connection: 'declined' }
  })
  const row = within(rowOf('Linear'))
  expect(row.getByRole('button', { name: 'Connect Linear' })).toBeInTheDocument()
  expect(within(rowOf('Ledger')).queryByRole('button', { name: /^Decline/ })).toBeNull()

  await userEvent.click(row.getByRole('button', { name: 'Decline Linear' }))

  // The form's fields belong to DeclineConnection; the row chooses which request it declines.
  expect(posted.map((form) => form.name)).toEqual([request])
  // The decision reloads the page, and the settled request leaves the row.
  await waitFor(() =>
    expect(within(rowOf('Linear')).queryByText(/^Connection request expires in/)).toBeNull(),
  )
  expect(within(rowOf('Linear')).queryByRole('button', { name: /^Decline/ })).toBeNull()
})

it('says why a decline failed in place of the deadline, and keeps the request', async () => {
  await renderLive({ view: viewAt('one', [linear]) }, () =>
    data({ error: 'Coral could not apply this change.' }, { status: 503 }),
  )
  await userEvent.click(screen.getByRole('button', { name: 'Decline Linear' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Coral could not apply this change.')
  expect(rowOf('Linear').querySelector('time')).toBeNull()
  expect(screen.getByRole('button', { name: 'Decline Linear' })).toBeInTheDocument()
})

// Catches: a refresh that remounts the page, closing your connect dialog and clearing your filter.
it('refreshes in place when the connection revision changes, and keeps your dialog and filter', async () => {
  const coral = { view: viewAt('one', [linear, ledger]) }
  const { signal } = await renderLive(coral)
  await userEvent.type(screen.getByRole('searchbox', { name: 'Filter accounts' }), 'l')
  await userEvent.click(screen.getByRole('button', { name: 'Reconnect Ledger' }))
  const dialog = await screen.findByRole('dialog', { name: 'Reconnect Ledger' })

  // One request ends as another starts, so the count stays the same.
  coral.view = viewAt('two', [
    { ...linear, connectionRequest: undefined },
    { ...ledger, connectionRequest: { name: 'ledger', expiresAt: '2099-01-01T00:00:00Z' } },
  ])
  await signal(signalOf(counted('two')))

  await waitFor(() =>
    expect(within(rowOf('Ledger')).getByText(/^Connection request expires in/)).toBeTruthy(),
  )
  expect(within(rowOf('Linear')).queryByText(/^Connection request expires in/)).toBeNull()
  expect(screen.getByRole('dialog', { name: 'Reconnect Ledger' })).toBe(dialog)
  expect(screen.getByRole('searchbox', { name: 'Filter accounts', hidden: true })).toHaveValue('l')
})
