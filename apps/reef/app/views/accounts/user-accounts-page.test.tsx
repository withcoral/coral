/** @vitest-environment happy-dom */
import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { data, useLoaderData, type ActionFunctionArgs } from 'react-router'
import { beforeEach, expect, it, vi } from 'vitest'
import type { ActionCount } from '@/lib/activity-live-types'
import type { ActionSignal } from '@/lib/use-action-signal'
import { renderRoute, shellSignal } from '@/test-support/route'
import { workspaceView } from '@/views/providers/fixtures'
import type { UserAccountRow, UserAccountsView } from './types'
import { UserAccountsPage } from './user-accounts-page'

vi.mock('@/wax/components/toast', () => ({ addToast: vi.fn() }))
const { addToast } = await import('@/wax/components/toast')
beforeEach(() => vi.mocked(addToast).mockClear())

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
  const { router } = renderRoute(<LoadedRoute />, {
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
      { path: '/elsewhere', Component: () => <p>Elsewhere</p> },
    ],
  })
  await screen.findByRole('table', { name: 'Accounts' })
  return { signal, router }
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

it('says why a decline failed in a toast, and keeps the request', async () => {
  await renderLive({ view: viewAt('one', [linear]) }, () =>
    data({ error: 'Coral could not apply this change.' }, { status: 503 }),
  )
  await userEvent.click(screen.getByRole('button', { name: 'Decline Linear' }))
  await waitFor(() =>
    expect(vi.mocked(addToast)).toHaveBeenCalledExactlyOnceWith('error', {
      title: 'Coral could not apply this change.',
    }),
  )
  expect(rowOf('Linear').querySelector('time')).not.toBeNull()
  expect(screen.getByRole('button', { name: 'Decline Linear' })).toBeEnabled()
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

const account = (name: string, state: UserAccountRow['state']): UserAccountRow => ({
  ...ledger,
  provider: `${workspaceView.name}/providers/${name}`,
  name,
  state,
  detailPath: `/details/${name}`,
})
/** The provider names, in the order of their rows. */
const shownProviders = () =>
  screen.getAllByRole('link', { name: / account details$/ }).map((link) => link.textContent)

const unsorted = () =>
  viewAt('one', [
    account('Zendesk', 'connected'),
    account('github', 'not_connected'),
    account('Attio', 'needs_reconnect'),
    account('Linear', 'connected'),
  ])
/** What the Provider and Status headings report to assistive technology. */
const sorts = () =>
  screen
    .getAllByRole('columnheader')
    .slice(0, 2)
    .map((heading) => heading.getAttribute('aria-sort'))
const sortBy = (column: 'provider' | 'status') =>
  userEvent.click(screen.getByRole('button', { name: `Sort by ${column}` }))

// Catches: rows in the order Coral returns them, which is not an order you can scan.
it('lists the accounts by provider name, without regard to case', async () => {
  await renderLive({ view: unsorted() })
  expect(shownProviders()).toEqual(['Attio', 'github', 'Linear', 'Zendesk'])
  expect(sorts()).toEqual(['ascending', 'none'])
})

// Catches: a heading that sorts one way only, and equal statuses left in an order you
// cannot scan.
it('sorts by provider or by status label, in each direction', async () => {
  await renderLive({ view: unsorted() })

  await sortBy('provider')
  expect(shownProviders()).toEqual(['Zendesk', 'Linear', 'github', 'Attio'])
  expect(sorts()).toEqual(['descending', 'none'])

  // Connected, Needs reconnect, Not connected.
  await sortBy('status')
  expect(shownProviders()).toEqual(['Linear', 'Zendesk', 'Attio', 'github'])
  expect(sorts()).toEqual(['none', 'ascending'])

  // The two connected accounts stay in provider order.
  await sortBy('status')
  expect(shownProviders()).toEqual(['github', 'Attio', 'Linear', 'Zendesk'])
  expect(sorts()).toEqual(['none', 'descending'])

  await sortBy('provider')
  expect(shownProviders()).toEqual(['Attio', 'github', 'Linear', 'Zendesk'])
  expect(sorts()).toEqual(['ascending', 'none'])
})

// Catches: a filter that resets the order, or an order that drops the filter.
it('keeps the order while you filter, and the filter while you sort', async () => {
  await renderLive({ view: unsorted() })
  await sortBy('status')
  await sortBy('status')
  await userEvent.type(screen.getByRole('searchbox', { name: 'Filter accounts' }), 't')
  await waitFor(() => expect(shownProviders()).toEqual(['github', 'Attio']))

  await sortBy('provider')
  expect(shownProviders()).toEqual(['Attio', 'github'])
  expect(screen.getByRole('searchbox', { name: 'Filter accounts' })).toHaveValue('t')
})

// Catches: an order kept across visits, which the page does not promise.
it('lists by provider name again when you come back to the page', async () => {
  const { router } = await renderLive({ view: unsorted() })
  await sortBy('status')
  expect(sorts()).toEqual(['none', 'ascending'])

  await act(() => router.navigate('/elsewhere'))
  await act(() => router.navigate('/'))

  await screen.findByRole('table', { name: 'Accounts' })
  expect(shownProviders()).toEqual(['Attio', 'github', 'Linear', 'Zendesk'])
  expect(sorts()).toEqual(['ascending', 'none'])
})
