/** @vitest-environment happy-dom */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, expect, test } from 'vitest'

import { ProviderSettingsPage } from './settings-page'
import type { ProviderSettingsPageProps, ProviderSettingsView, WorkspaceView } from './types'

;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

const workspace: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: 'users/test',
  profile: { user: 'users/test', name: 'Test', email: '', local: true },
  mcpEndpoint: '',
  connectEndpoint: '',
  path: '/workspaces/demo',
}
const provider: ProviderSettingsView = {
  id: '00000000-0000-4000-8000-000000000001',
  displayName: 'GitLab.com',
  namespace: 'gitlab',
  callRoot: 'coral.providers.gitlab',
  builtin: false,
  etag: 'etag-1',
  path: '/workspaces/demo/providers/00000000-0000-4000-8000-000000000001',
  account: 'caller',
  enabled: true,
  toolsReady: true,
  connection: 'connected',
  canConnect: true,
  canReconnect: true,
  accountSettingIncompatible: false,
}
const source: ProviderSettingsPageProps['source'] = {
  state: 'loaded',
  kind: 'openapi',
  location: { type: 'url', url: 'https://gitlab.com/api/v4/openapi.json', host: 'gitlab.com' },
}

const roots: Root[] = []
const routers: ReturnType<typeof createMemoryRouter>[] = []

afterEach(() => {
  for (const root of roots.splice(0)) act(() => root.unmount())
  for (const router of routers.splice(0)) router.dispose()
  document.body.innerHTML = ''
})

type Props = Partial<Omit<ProviderSettingsPageProps, 'provider'>> & {
  provider?: Partial<ProviderSettingsView>
}

async function render(
  { provider: overrides, ...props }: Props = {},
  action: (data: FormData) => unknown = () => ({ error: null, message: 'Provider name saved.' }),
) {
  const submissions: FormData[] = []
  const targets: string[] = []
  const router = createMemoryRouter(
    [
      {
        path: '*',
        action: async ({ request }) => {
          const data = await request.formData()
          submissions.push(data)
          const url = new URL(request.url)
          targets.push(url.pathname + url.search)
          return action(data)
        },
        Component: () => (
          <ProviderSettingsPage
            workspace={workspace}
            source={source}
            form={{ csrf: 'csrf', requestId: 'request' }}
            {...props}
            provider={{ ...provider, ...overrides }}
          />
        ),
      },
    ],
    { initialEntries: [`${provider.path}/settings`] },
  )
  const container = document.createElement('div')
  document.body.append(container)
  const root = createRoot(container)
  roots.push(root)
  routers.push(router)
  await act(async () => root.render(<RouterProvider router={router} />))
  return { submissions, targets }
}

// Your account under Accounts, with the way back to these settings.
const yourAccountPath = `/workspaces/demo/accounts/${provider.id}?${new URLSearchParams({
  returnTo: `${provider.path}/settings`,
})}`
const text = () => document.body.textContent ?? ''
const byLabel = (label: string) => document.querySelector<HTMLElement>(`[aria-label="${label}"]`)
const named = (role: 'button' | 'link', name: string) =>
  [...document.querySelectorAll<HTMLElement>(role === 'button' ? 'button' : 'a[href]')].find(
    (element) => (element.getAttribute('aria-label') ?? element.textContent?.trim()) === name,
  )
// The breadcrumb titles the page; the editable name heads its content.
const heading = () => document.querySelector('h2')?.textContent

async function click(element: Element | null | undefined) {
  if (!element) throw new Error('Nothing to click')
  await act(async () => element.dispatchEvent(new MouseEvent('click', { bubbles: true })))
}

async function type(input: HTMLInputElement, value: string) {
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, value)
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
}

async function press(element: Element, key: string) {
  await act(async () =>
    element.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true })),
  )
}

async function settle() {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
  })
}

const panel = () => document.querySelector('[aria-label="Source"]')!.textContent
const nameInput = () => byLabel('Provider name') as HTMLInputElement | null

// Catches: the page losing its way back to the provider list or to the
// provider's tools.
test('the breadcrumb leads back to the provider list and the provider', async () => {
  await render()
  const trail = document.querySelector('nav[aria-label="breadcrumb"]')!
  const links = [...trail.querySelectorAll('a')].map((a) => [a.textContent, a.getAttribute('href')])
  expect(links).toEqual([
    ['Providers', '/workspaces/demo/providers'],
    ['GitLab.com', provider.path],
  ])
  expect(trail.textContent).toContain('Settings')
  expect([...document.querySelectorAll('h1')].map((h) => h.textContent)).toEqual(['Settings'])
  expect(heading()).toBe('GitLab.com')
})

// Catches: Escape, Cancel, or clicking away saving the draft.
test('rename cancels on Escape, on Cancel, and never saves on blur', async () => {
  const { submissions } = await render()
  await click(byLabel('Edit provider name'))
  const input = nameInput()!
  expect(document.activeElement).toBe(input)
  expect(input.value).toBe('GitLab.com')

  await type(input, 'GitLab')
  await act(async () => input.dispatchEvent(new FocusEvent('blur')))
  expect(nameInput()).not.toBeNull()

  await press(input, 'Escape')
  expect(nameInput()).toBeNull()
  expect(heading()).toBe('GitLab.com')

  await click(byLabel('Edit provider name'))
  await type(nameInput()!, 'GitLab')
  await click(named('button', 'Cancel'))
  expect(nameInput()).toBeNull()
  expect(submissions).toHaveLength(0)
})

// Catches: Enter not saving, or the save losing the provider's etag.
test('Enter saves the new name', async () => {
  const { submissions } = await render()
  await click(byLabel('Edit provider name'))
  await type(nameInput()!, 'GitLab')
  await press(nameInput()!, 'Enter')
  await settle()
  expect(submissions).toHaveLength(1)
  expect(Object.fromEntries(submissions[0]!)).toMatchObject({
    intent: 'renameProvider',
    provider: provider.id,
    etag: 'etag-1',
    displayName: 'GitLab',
  })
  expect(nameInput()).toBeNull()
})

// Catches: a failed save discarding what the reader typed.
test('a failed save keeps the draft and shows the error inline', async () => {
  await render({}, () => ({ error: 'Coral could not change the provider name.' }))
  await click(byLabel('Edit provider name'))
  await type(nameInput()!, 'GitLab')
  await click(named('button', 'Save'))
  await settle()
  expect(nameInput()?.value).toBe('GitLab')
  expect(text()).toContain('Coral could not change the provider name.')
})

// Catches: separate and shared accounts read the same, or a no-account
// provider offering a connection control.
test('each account setting explains whose account calls use', async () => {
  await render({ provider: { account: 'caller', connection: 'not_connected' } })
  expect(text()).toContain(
    'Each person connects their own account. Workspace owners connect accounts for agents.',
  )
  expect(byLabel('Individual accounts')?.textContent).toContain('Not connected')
  // The account's actions sit inside its card.
  expect(byLabel('Individual accounts')?.textContent).toContain('Change account setting')
  // Your own account connects in a dialog on this page.
  expect(
    named('button', 'Connect your account')?.closest('[aria-label="Individual accounts"]'),
  ).not.toBeNull()
  expect(named('link', 'Connect your account')).toBeUndefined()
  act(() => roots.pop()!.unmount())

  await render({ provider: { account: 'fixed', connection: 'connected' } })
  expect(text()).toContain('People and agents use this shared account.')
  expect(byLabel('One shared account')?.textContent).toContain('Connected')
  expect(named('link', 'Manage shared account')?.getAttribute('href')).toBe(
    `${provider.path}/accounts?scope=shared`,
  )
  act(() => roots.pop()!.unmount())

  await render({ provider: { account: 'none' } })
  expect(text()).toContain('No account is required.')
  // The card's title says it all; no second line repeats it.
  expect(text()).not.toContain('Calls do not use an account.')
  expect(named('link', 'Connect your account')).toBeUndefined()
  expect(document.querySelector('a[href*="/accounts"]')).toBeNull()
})

// Catches: members seeing owner actions, owner-only source metadata, or a
// configuration section they cannot use.
test('a member sees readable settings and only their own account action', async () => {
  await render({
    workspace: { ...workspace, effectiveRole: 'member' },
    source: undefined,
    provider: { account: 'caller', connection: 'not_connected' },
  })
  expect(byLabel('Edit provider name')).toBeNull()
  expect(document.querySelector('[aria-label="Configuration"]')).toBeNull()
  expect(document.querySelector('[aria-label="Source"]')).toBeNull()
  expect(named('button', 'Change account setting')).toBeUndefined()
  expect(named('button', 'Remove provider')).toBeUndefined()
  expect(named('button', 'Connect your account')).toBeDefined()
})

test('a member of a shared-account provider is told who connects it', async () => {
  await render({
    workspace: { ...workspace, effectiveRole: 'member' },
    source: undefined,
    provider: { account: 'fixed', connection: 'not_connected' },
  })
  expect(text()).toContain('Ask a workspace owner to connect the shared account.')
  expect(document.querySelector('a[href*="/accounts"]')).toBeNull()
})

// Catches: an archived workspace offering changes Coral will refuse.
test('an archived workspace is read-only with a notice', async () => {
  await render({
    workspace: { ...workspace, state: 'archived' },
    provider: { account: 'caller', connection: 'not_connected' },
  })
  expect(text()).toContain('This workspace is archived')
  expect(byLabel('Edit provider name')).toBeNull()
  expect(document.querySelector('[aria-label="Configuration"]')).toBeNull()
  for (const name of ['Change account setting', 'Remove provider', 'Connect your account'])
    expect(named('button', name)).toBeUndefined()
  for (const name of ['Check for updates', 'Replace source'])
    expect(named('link', name)).toBeUndefined()
  // Owners still read the source where Coral lets them.
  expect(text()).toContain('https://gitlab.com/api/v4/openapi.json')
})

// Catches: settings showing configuration no provider read supplies, or an
// edit that skips the staged update flow.
test('configuration is an entry point to the update flow, without values', async () => {
  await render()
  const section = document.querySelector('[aria-label="Configuration"]')!
  expect(section.textContent).toContain('Change destinations or sign-in settings.')
  const edit = [...section.querySelectorAll('form')].find((form) =>
    form.textContent?.includes('Edit configuration'),
  )!
  expect(edit.getAttribute('action')).toBe(`${provider.path}/update`)
  expect(Object.fromEntries(new FormData(edit))).toMatchObject({
    intent: 'prepare',
    task: 'configuration',
  })
  for (const absent of ['Sign-in method', 'Destinations', 'Scopes', 'Authorization URL', 'Header'])
    expect(text()).not.toContain(absent)
})

// Catches: a failed source read blanking settings, or reading as an upload.
test('the source panel shows the kind, the address, or why it cannot', async () => {
  await render()
  expect(panel()).toContain('OpenAPI document')
  // The host reads at a glance; the whole URL stays on one line to copy.
  expect(panel()).toContain('gitlab.com')
  expect(panel()).toContain('https://gitlab.com/api/v4/openapi.json')
  expect(
    document.querySelector('[aria-label="Source"] [aria-label="Copy source URL"]'),
  ).not.toBeNull()
  expect(named('link', 'Check for updates')?.getAttribute('href')).toBe(`${provider.path}/update`)
  expect(named('link', 'Replace source')?.getAttribute('href')).toBe(
    `${provider.path}/update?replaceSource=1`,
  )
  act(() => roots.pop()!.unmount())

  await render({ source: { state: 'loaded', kind: 'mcp', location: { type: 'upload' } } })
  expect(panel()).toContain('Uploaded document')
  act(() => roots.pop()!.unmount())

  await render({ source: { state: 'failed' } })
  expect(panel()).toContain('Coral could not read the source.')
  expect(text()).toContain(
    'Each person connects their own account. Workspace owners connect accounts for agents.',
  )
})

// Catches: removal without confirmation, or a confirmation that does not name
// the provider and what goes with it.
test('removal asks for confirmation that names the provider and its consequences', async () => {
  const { submissions } = await render()
  await click(named('button', 'Remove provider'))
  const dialog = document.querySelector('[role="dialog"]')!
  expect(dialog.textContent).toContain('GitLab.com')
  expect(dialog.textContent).toContain('coral.providers.gitlab')
  expect(dialog.textContent).toContain('connected account')
  expect(dialog.textContent).toContain('You cannot undo this.')
  expect(dialog.textContent).toContain('run history')
  expect(submissions).toHaveLength(0)
})

// Catches: an account that needs a reconnect reading as connected, or a
// Connect or Reconnect button Coral would refuse.
test('the account card shows a reconnect and hides refused connects', async () => {
  await render({ provider: { account: 'caller', connection: 'needs_reconnect' } })
  // The same words as your account's own page under Accounts.
  expect(byLabel('Individual accounts')?.textContent).toContain('Needs reconnect')
  expect(named('button', 'Reconnect your account')).toBeDefined()
  act(() => roots.pop()!.unmount())

  await render({ provider: { account: 'caller', connection: 'not_connected', canConnect: false } })
  expect(named('button', 'Connect your account')).toBeUndefined()
  act(() => roots.pop()!.unmount())

  // A reconnect Coral refuses leaves the account to open, not to reconnect.
  await render({
    provider: { account: 'caller', connection: 'needs_reconnect', canReconnect: false },
  })
  expect(named('button', 'Reconnect your account')).toBeUndefined()
  // Manage opens your account under Accounts, which brings you back here.
  expect(named('link', 'Manage your account')?.getAttribute('href')).toBe(yourAccountPath)
})

// Catches: a disabled provider looking the same as an active one.
test('a disabled provider is tagged beside its name', async () => {
  await render({ provider: { enabled: false } })
  expect(document.querySelector('[data-provider-state="disabled"]')?.textContent).toBe('Disabled')
  act(() => roots.pop()!.unmount())
  await render()
  expect(document.querySelector('[data-provider-state="disabled"]')).toBeNull()
})

// Catches: an owner offered a change Coral refuses for the Built-in Provider.
test('the built-in provider offers no change', async () => {
  await render({
    source: undefined,
    provider: {
      builtin: true,
      displayName: 'Coral',
      namespace: '',
      callRoot: 'coral.workspace',
      account: 'none',
    },
  })
  expect(byLabel('Edit provider name')).toBeNull()
  expect(byLabel('Source')).toBeNull()
  expect(document.querySelector('[aria-label="Configuration"]')).toBeNull()
  for (const name of ['Change account setting', 'Remove provider'])
    expect(named('button', name)).toBeUndefined()
})

// Catches: a provider with no callable tool looking ready in settings.
test('a provider without ready tools is tagged beside its name', async () => {
  await render({ provider: { toolsReady: false } })
  expect(document.querySelector('[data-provider-state="no-tools"]')?.textContent).toBe(
    'No tools ready',
  )
  act(() => roots.pop()!.unmount())
  await render()
  expect(document.querySelector('[data-provider-state="no-tools"]')).toBeNull()
})

// Catches: a second status block repeating what the tags and the account
// card already say.
test('settings have no separate status section', async () => {
  await render({ provider: { connection: 'not_connected', toolsReady: false } })
  expect(byLabel('Status')).toBeNull()
})

// Catches: an account Coral could not read shown with a made-up state, or
// without a way to open it.
test('an unreadable account says so and still links to it', async () => {
  await render({ provider: { account: 'caller', connection: 'unknown' } })
  const card = byLabel('Individual accounts')!
  expect(card.textContent).not.toMatch(/Connected|Not connected|Needs reconnect/)
  expect(text()).toContain('Cannot read your account')
  expect(text()).toContain('Open it to see its state.')
  expect(named('link', 'Manage your account')?.getAttribute('href')).toBe(yourAccountPath)
})

// The card opens the provider's tools.
// The breadcrumb links the provider too; the card is the link outside it.
// Catches: configuration pushed above the account people come here to manage.
test('the account comes before configuration, and removal comes last', async () => {
  await render()
  const sections = [...document.querySelectorAll('section[aria-label]')].map((section) =>
    section.getAttribute('aria-label'),
  )
  expect(sections).toEqual(['Account', 'Configuration', 'Remove provider'])
})

// Catches: configuration left as loose text beside the cards around it.
test('configuration is a card with its edit action inside', async () => {
  await render()
  const section = document.querySelector('section[aria-label="Configuration"]')!
  // One card holds the title, the line, and the action.
  const card = section.firstElementChild!
  expect(section.children).toHaveLength(1)
  expect(card.textContent).toContain('Configuration')
  expect(card.textContent).toContain('Change destinations or sign-in settings.')
  expect(card.querySelector('form')?.textContent).toBe('Edit configuration')
})

// Catches: connecting your own account sending you away from settings, a sign-in
// that does not return here, or starting anyone's account but yours.
test('connects your own account in a dialog on this page', async () => {
  const { submissions, targets } = await render(
    { provider: { connection: 'not_connected' } },
    () => ({
      error: 'Stopped here.',
      credential: null,
    }),
  )
  await click(named('button', 'Connect your account'))
  // Opening the dialog starts your connection through your account page, which returns here.
  await expect.poll(() => submissions.length).toBe(1)
  expect(submissions[0]!.get('intent')).toBe('connectAccount')
  expect(targets[0]).toBe(yourAccountPath)
  const dialog = document.querySelector('[role="dialog"]')!
  expect(dialog.textContent).toContain('Connect GitLab.com')
  expect(dialog.textContent).toContain('Stopped here.')
})
