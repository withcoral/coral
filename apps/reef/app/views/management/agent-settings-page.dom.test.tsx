/** @vitest-environment happy-dom */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { createMemoryRouter, RouterProvider, useActionData } from 'react-router'
import { afterEach, expect, test } from 'vitest'

import { AgentSettingsPage } from './agent-settings-page'
import type { ActionResult, AgentView, RegistrationView, WorkspaceView } from './types'

;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

const person = { user: 'users/test', name: 'Test', email: '', local: true }
const workspace: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: person.user,
  profile: person,
  mcpEndpoint: '',
  connectEndpoint: '',
  path: '/workspaces/demo',
}
const agent: AgentView = {
  name: 'workspaces/demo/agents/nightly',
  displayName: 'Nightly',
  state: 'enabled',
  etag: 'agent-etag',
  createdBy: person.user,
  creator: person,
  accessBlockers: [],
  path: '/workspaces/demo/agents/nightly',
  settingsPath: '/workspaces/demo/agents/nightly/settings',
}
const registration: RegistrationView = {
  name: `${agent.name}/registration`,
  uid: 'registration',
  enabled: true,
  etag: 'registration-etag',
  expiration: '',
}

const roots: Root[] = []
const routers: ReturnType<typeof createMemoryRouter>[] = []

afterEach(() => {
  for (const root of roots.splice(0)) act(() => root.unmount())
  for (const router of routers.splice(0)) router.dispose()
  document.body.innerHTML = ''
})

/** Renders the page as its route does: the latest form result arrives as action data. */
async function render(action: (intent: string) => ActionResult) {
  const router = createMemoryRouter(
    [
      {
        path: '*',
        // Each response is a new object, as it is over the network.
        action: async ({ request }) => ({
          ...action(String((await request.formData()).get('intent'))),
        }),
        Component: function Route() {
          return (
            <AgentSettingsPage
              workspace={workspace}
              agent={agent}
              registration={registration}
              form={{ csrf: 'csrf', requestId: 'request' }}
              result={useActionData() as ActionResult | undefined}
            />
          )
        },
      },
    ],
    { initialEntries: [agent.settingsPath] },
  )
  const container = document.createElement('div')
  document.body.append(container)
  const root = createRoot(container)
  roots.push(root)
  routers.push(router)
  await act(async () => root.render(<RouterProvider router={router} />))
}

const text = () => document.body.textContent ?? ''
const button = (name: string) =>
  [...document.querySelectorAll<HTMLElement>('button')].findLast(
    (element) => (element.getAttribute('aria-label') ?? element.textContent?.trim()) === name,
  )
async function click(name: string) {
  const element = button(name)
  if (!element) throw new Error(`No button named ${name}`)
  await act(async () => element.dispatchEvent(new MouseEvent('click', { bubbles: true })))
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
  })
}

const saved = (message: string): ActionResult => ({
  error: null,
  credential: null,
  message,
})
const results: Record<string, ActionResult> = {
  agentState: saved('Agent disabled.'),
  rotate: { error: null, credential: 'new-agent-key' },
}

// Catches: a saved change staying on the page as a banner when the page already shows it.
test('a saved change leaves no notice on the page', async () => {
  await render((intent) => results[intent])
  await click('Disable agent')
  expect(text()).not.toContain('Agent disabled.')
})

// Catches: a failure that does not stay on the page for the reader to act on.
test('a failure stays on the page', async () => {
  await render(() => ({
    error: 'Coral could not enable or disable the agent. Try again.',
    credential: null,
  }))
  await click('Disable agent')
  expect(text()).toContain('Coral could not enable or disable the agent. Try again.')
})

// Catches: a replaced API key showing as a card on the page instead of the one-time key dialog.
test('a replaced key opens the key dialog and leaves the page when closed', async () => {
  await render((intent) => results[intent])
  await click('Replace API key')
  await click('Replace API key')
  const dialog = document.querySelector('[role="dialog"]')
  expect(dialog?.textContent).toContain('Save the API key for Nightly')
  await click('Close')
  expect(document.querySelector('[role="dialog"]')).toBeNull()
  expect(document.body.innerHTML).not.toContain('new-agent-key')
})
