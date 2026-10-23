/** @vitest-environment happy-dom */

import { act, render as mount, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createMemoryRouter, RouterProvider, useLoaderData } from 'react-router'
import { beforeEach, expect, it, onTestFinished, vi } from 'vitest'
import type { AuthorizationView } from '@/views/accounts/types'
import { ConnectAccountDialog } from './connect-account-dialog'

vi.mock('@/wax/components/toast', () => ({ addToast: vi.fn() }))
const { addToast } = await import('@/wax/components/toast')

const agentPage = '/workspaces/w/agents/nightly'
const returnTo = new URLSearchParams({ returnTo: agentPage })
const authorization = (id: string) => `/workspaces/w/account-authorizations/${id}?${returnTo}`
const progress = (id: string) => `/workspaces/w/account-authorizations/${id}/progress?${returnTo}`
const pending: AuthorizationView = {
  providerName: 'GitHub',
  scope: 'agent',
  path: authorization('first'),
  returnTo: agentPage,
  parents: [],
  state: 'pending',
  oauth: false,
  grantedScope: '',
  credentialFields: [{ name: 'token', label: 'Token', description: '', sensitive: true }],
  form: { csrf: 'test', requestId: 'test-request' },
}
const device: AuthorizationView = { ...pending, oauth: true, userCode: 'WDJB-MJHT' }

type Started = { error: string | null; progress?: string }
type Progress =
  | { view: AuthorizationView }
  | { succeeded: true }
  | { signedOut: true }
  | { error: string }
let starts: { requestId: string }[] = []
let started: (attempt: number) => Started | Promise<Started>
let checks: (id: string, check: number) => Progress
let checked = 0
/** Whether the page's loader shows the agent's account ready. */
let pageReady = false
let pageLoads = 0

beforeEach(() => {
  vi.mocked(addToast).mockClear()
  starts = []
  started = () => ({ error: null, progress: progress('first') })
  checks = () => ({ view: pending })
  checked = 0
  pageReady = false
  pageLoads = 0
})

function Page() {
  const { ready } = useLoaderData<{ ready: boolean }>()
  return (
    <ConnectAccountDialog
      provider="workspaces/w/providers/github"
      providerName="GitHub"
      namespace="github"
      form={{ csrf: 'csrf', requestId: 'request' }}
      connectable={!ready}
      reconnect={false}
      ready={ready}
      fallback={<a href="/account">Manage account for github</a>}
    />
  )
}

/** The agent's page with one provider row. Its loader says whether the account is ready. */
async function render() {
  const router = createMemoryRouter(
    [
      {
        path: '/workspaces/:workspace/account-authorizations/:authorization/progress',
        loader: ({ params }) => checks(params.authorization!, ++checked),
      },
      {
        path: '*',
        loader: () => {
          pageLoads++
          return { ready: pageReady }
        },
        action: async ({ request }) => {
          const form = await request.formData()
          expect(form.get('intent')).toBe('connectAccount')
          starts.push({ requestId: String(form.get('requestId')) })
          return started(starts.length)
        },
        Component: Page,
      },
    ],
    { initialEntries: [agentPage] },
  )
  onTestFinished(() => router.dispose())
  mount(<RouterProvider router={router} />)
  await screen.findByRole('button', { name: 'Connect account for github' })
  return router
}
/** Coral connects the account; the page's next load shows it. */
async function connectElsewhere(router: Awaited<ReturnType<typeof render>>) {
  pageReady = true
  await act(() => router.revalidate())
}
const dialog = () => screen.findByRole('dialog')
const open = async () => {
  await userEvent.click(screen.getByRole('button', { name: 'Connect account for github' }))
  return dialog()
}

// Catches: a refused start leaving no way forward, or a retry replaying the refused request.
it('shows why Coral refused to start and tries again with a new request', async () => {
  started = (attempt) =>
    attempt === 1
      ? { error: 'Coral could not connect the account for this agent.' }
      : { error: null, progress: progress('first') }
  await render()
  const popup = within(await open())
  await popup.findByText('Coral could not connect the account for this agent.')
  await userEvent.click(popup.getByRole('button', { name: 'Try again' }))
  await popup.findByLabelText('Token')
  expect(starts).toHaveLength(2)
  expect(starts[0].requestId).not.toBe(starts[1].requestId)
})

// Catches: an ended authorization stranding you, or its retry reopening the ended one.
it.each([
  ['failed', 'The service rejected the sign-in code.', { failure: 'oauth_code_rejected' }],
  ['expired', 'This connection expired.', {}],
  ['canceled', 'This connection was canceled.', {}],
] as const)('says the authorization %s and starts a new one', async (state, message, extra) => {
  started = (attempt) => ({ error: null, progress: progress(`attempt-${attempt}`) })
  checks = (id) => ({
    view:
      id === 'attempt-1'
        ? { ...pending, state, ...extra }
        : { ...pending, path: authorization(id) },
  })
  await render()
  const popup = within(await open())
  await popup.findByText(message, { exact: false })
  await userEvent.click(popup.getByRole('button', { name: 'Try again' }))
  await popup.findByLabelText('Token')
  expect(starts).toHaveLength(2)
})

// Catches: a secret entering router state instead of going straight to Coral.
it('posts a secret as a document form to the authorization', async () => {
  await render()
  const popup = within(await open())
  await userEvent.type(await popup.findByLabelText('Token'), 'secret-value')
  // The router cancels a submission it takes over; the browser keeps a document one.
  const submissions: { action: string | null; routed: boolean }[] = []
  const observe = (event: SubmitEvent) => {
    submissions.push({
      action: (event.target as HTMLFormElement).getAttribute('action'),
      routed: event.defaultPrevented,
    })
    event.preventDefault()
  }
  document.addEventListener('submit', observe)
  onTestFinished(() => document.removeEventListener('submit', observe))
  await userEvent.click(popup.getByRole('button', { name: 'Connect account' }))
  expect(submissions).toEqual([{ action: pending.path, routed: false }])
})

// Catches: the dialog staying open over a connected account, or announcing one nobody made here.
it('closes and announces the account once the page shows it ready', async () => {
  const router = await render()
  await open()
  await connectElsewhere(router)
  await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  expect(screen.getByRole('link', { name: 'Manage account for github' })).toBeTruthy()
  expect(vi.mocked(addToast)).toHaveBeenCalledExactlyOnceWith('success', {
    title: 'Account connected.',
  })
})

it('stays quiet when the account turns ready with the dialog closed', async () => {
  const router = await render()
  await connectElsewhere(router)
  expect(screen.getByRole('link', { name: 'Manage account for github' })).toBeTruthy()
  expect(vi.mocked(addToast)).not.toHaveBeenCalled()
})

// Catches: closing and reopening the dialog leaving abandoned authorizations behind.
it('continues the same authorization when you reopen the dialog', async () => {
  await render()
  await within(await open()).findByLabelText('Token')
  await userEvent.click(within(await dialog()).getByRole('button', { name: 'Close' }))
  await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  await within(await open()).findByLabelText('Token')
  expect(starts).toHaveLength(1)
})

// Catches: a later reconnect reopening the finished authorization instead of a new one.
it('starts a new authorization after the last one connected the account', async () => {
  const router = await render()
  await within(await open()).findByLabelText('Token')
  await connectElsewhere(router)
  await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  // The account later needs you again.
  pageReady = false
  await act(() => router.revalidate())
  await within(await open()).findByLabelText('Token')
  expect(starts).toHaveLength(2)
})

// Catches: reopening during a slow start sending Coral a second authorization.
it('waits for a start still in flight when you reopen the dialog', async () => {
  let answer!: () => void
  const answered = new Promise<void>((resolve) => {
    answer = resolve
  })
  const respond = started
  started = async (attempt) => {
    await answered
    return respond(attempt)
  }
  await render()
  const popup = within(await open())
  await popup.findByText('Starting the connection…')
  await userEvent.keyboard('{Escape}')
  await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  await open()
  answer()
  await within(await dialog()).findByLabelText('Token')
  expect(starts).toHaveLength(1)
})

// Catches: a failed check replacing the agent page with an error page.
it('keeps the page when Coral cannot check the connection, and checks again', async () => {
  checks = (_id, check) => (check === 1 ? { error: 'Coral is not available.' } : { view: pending })
  await render()
  const popup = within(await open())
  await popup.findByText('Coral is not available.')
  await userEvent.click(popup.getByRole('button', { name: 'Try again' }))
  await popup.findByLabelText('Token')
  expect(starts).toHaveLength(1)
})

// Catches: a failed poll losing the code while you sign in in another tab.
it('keeps the code through a failed check while it waits', async () => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  checks = (_id, check) =>
    check === 2 ? { error: 'Coral could not check the connection.' } : { view: device }
  await render()
  const popup = within(await open())
  await popup.findByText('WDJB-MJHT')
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await popup.findByText('Coral could not check the connection. Trying again in a moment…')
  expect(popup.getByText('WDJB-MJHT')).toBeTruthy()
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await vi.waitFor(() => expect(popup.queryByText(/Trying again in a moment/)).toBeNull())
  expect(checked).toBe(3)
})

// Catches: a finished sign-in pushing a second copy of the page onto your history.
it('reloads the page in place when the authorization succeeds', async () => {
  checks = () => {
    pageReady = true
    return { succeeded: true }
  }
  const router = await render()
  const loads = pageLoads
  await open()
  await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  expect(screen.getByRole('link', { name: 'Manage account for github' })).toBeTruthy()
  expect(pageLoads).toBeGreaterThan(loads)
  expect(router.state.historyAction).toBe('POP')
  expect(router.state.location.pathname).toBe(agentPage)
  expect(vi.mocked(addToast)).toHaveBeenCalledExactlyOnceWith('success', {
    title: 'Account connected.',
  })
})

// Catches: a lapsed sign-in leaving the dialog polling forever with no way to sign in.
it('stops checking and reloads the page when your sign-in lapsed', async () => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  checks = (_id, check) => (check === 1 ? { view: device } : { signedOut: true })
  await render()
  const popup = within(await open())
  await popup.findByText('WDJB-MJHT')
  // After the start's own reload, so only the lapsed sign-in can reload the page.
  const loads = pageLoads
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await popup.findByText('Your sign-in expired. Opening the sign-in page…')
  // The page's own read meets the lapsed sign-in, and Reef then starts sign-in.
  await vi.waitFor(() => expect(pageLoads).toBeGreaterThan(loads))
  // The reload also rereads the dialog's last check once; the polling stops.
  const after = checked
  await act(() => vi.advanceTimersByTimeAsync(6000))
  expect(checked).toBe(after)
})
