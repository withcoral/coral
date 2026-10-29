/** @vitest-environment happy-dom */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, expect, it } from 'vitest'

import { ConfigurationStep } from './configuration-step'
import type { ConfigurationReviewView, RecipeConfigurationView, SetupView } from './types'

const roots: Root[] = []
const routers: ReturnType<typeof createMemoryRouter>[] = []

afterEach(() => {
  for (const root of roots.splice(0)) act(() => root.unmount())
  for (const router of routers.splice(0)) router.dispose()
  document.body.innerHTML = ''
})

const review: ConfigurationReviewView = {
  revision: '1',
  confirmed: false,
  authenticationMethods: ['none'],
  discoveryAuthorizationRequired: false,
  automaticClientAvailable: true,
  issuer: '',
  resource: '',
  differences: [],
  interruptsAccess: null,
  updating: false,
  values: {
    displayName: 'UniFi Network',
    authentication: 'none',
    headers: [],
    parameters: [],
    basic: {
      username: { source: 'literal', value: '' },
      password: { source: 'literal', value: '' },
    },
    credentialFields: {},
    authorizationUrl: '',
    tokenUrl: '',
    scopes: [],
    servers: [],
  },
  authenticationStatus: 'supported',
  documentDisplayName: 'UniFi Network',
  servers: [
    {
      declaration: '#/servers',
      tools: ['listSites'],
      options: [{ url: '/integration', variables: [] }],
    },
  ],
  destinations: {},
  toolCount: null,
  tools: [],
  unavailableTools: [],
  diagnostic: '',
  readable: true,
  errors: {},
}

const setup: SetupView = {
  id: 'setup-1',
  kind: 'openapi',
  state: 'awaiting_configuration',
  credentialMethod: 'none',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'Uploaded document',
  sourceHost: '',
  serverName: '',
  allowedRoutes: [],
  connectAvailable: false,
  connectLaterAvailable: false,
  configuration: review,
}

const consoleId = {
  name: 'console_id',
  label: 'Console ID',
  description: '',
  type: 'string' as const,
  defaultValue: '',
  choices: [],
}

/** A recipe whose console ID builds the root server URL, like UniFi Network's. */
const serverRecipe: RecipeConfigurationView = {
  id: 'unifi_network',
  inputs: [consoleId],
  suggestedMethods: ['none'],
  values: {
    authentication: 'none',
    servers: [
      {
        declaration: '#/servers',
        url: 'https://api.ui.com/v1/connector/consoles/{{input.console_id}}/network/integration',
        custom: true,
        variables: {},
      },
    ],
  },
}

/** A recipe whose console ID fills only a request header, which Next never waits for. */
const headerRecipe: RecipeConfigurationView = {
  id: 'header_only',
  inputs: [consoleId],
  suggestedMethods: ['none'],
  values: {
    authentication: 'none',
    requestHeaders: [{ name: 'x-console', value: '{{input.console_id}}' }],
  },
}

async function renderStep(recipe: RecipeConfigurationView | null, configuration = review) {
  // The step's form reloads the document, so a router action never sees it post.
  let submitted = false
  const router = createMemoryRouter(
    [
      {
        path: '*',
        Component: () => (
          <ConfigurationStep
            setup={{ ...setup, configuration }}
            form={{ csrf: 'test-csrf', requestId: 'test-request' }}
            pending={false}
            recipe={recipe}
          />
        ),
      },
    ],
    { initialEntries: ['/workspaces/demo/providers/new'] },
  )
  const container = document.createElement('div')
  document.body.append(container)
  container.addEventListener('submit', () => (submitted = true), { capture: true })
  const root = createRoot(container)
  roots.push(root)
  routers.push(router)
  await act(async () => root.render(<RouterProvider router={router} />))
  return { submitted: () => submitted }
}

const field = (label: string) =>
  [...document.querySelectorAll('label')].find((element) => element.textContent === label)!
    .control as HTMLInputElement
const next = () =>
  [...document.querySelectorAll('button')].find((button) => button.textContent === 'Next')!
const signIn = () =>
  [...document.querySelectorAll('button')].find(
    (button) => button.textContent === 'Choose a method',
  )!
const alerts = () =>
  [...document.querySelectorAll('[role="alert"]')].map((alert) => alert.textContent)

// Catches: a recipe answer the step waits for that looks optional, with Next
// disabled and no word on which field holds it back.
it.each([
  ['builds a server URL', serverRecipe, review, true],
  ['fills only a header', headerRecipe, { ...review, servers: [] }, false],
])('an input that %s is required: %s', async (_use, recipe, configuration, required) => {
  await renderStep(recipe, configuration)
  expect(field('Console ID').required).toBe(required)
  expect(next().getAttribute('aria-disabled')).toBe(required ? 'true' : null)
})

// Catches: a blank recipe answer that still blocks the form after the person
// typed their own server URL, which replaces the one the answer builds.
it('drops the requirement when the person types their own server URL', async () => {
  const step = await renderStep(serverRecipe)
  await act(async () => {
    const input = field('Main server')
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(
      input,
      'https://console.example.test/integration',
    )
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
  expect(field('Console ID').required).toBe(false)
  expect(next().getAttribute('aria-disabled')).toBeNull()
  await act(async () => next().click())
  expect(step.submitted()).toBe(true)
})

// Catches: a large document leaving Next unchanged while Coral confirms its
// configuration and prepares the tool list.
it('shows progress as soon as Next submits the configuration', async () => {
  const step = await renderStep(null, {
    ...review,
    values: {
      ...review.values,
      servers: [
        {
          declaration: '#/servers',
          url: 'https://console.example.test/integration',
          custom: true,
          variables: {},
        },
      ],
    },
  })

  await act(async () => next().click())

  expect(step.submitted()).toBe(true)
  expect(next()).toHaveAttribute('aria-busy', 'true')
  expect(next()).toHaveAttribute('aria-disabled', 'true')
  expect(next()).not.toBeDisabled()
})

// Catches: a disabled Next that cannot say why, or that submits anyway. The
// tooltip and focus name the field; no extra error appears under it.
it('moves focus to the missing answer when Next is pressed, and submits nothing', async () => {
  const step = await renderStep(serverRecipe)
  expect(alerts()).toEqual([])
  // The root server is required too; its combobox says so, as a text field would.
  expect(field('Main server').getAttribute('aria-required')).toBe('true')

  await act(async () => next().click())

  expect(document.activeElement).toBe(field('Console ID'))
  expect(alerts()).toEqual([])
  expect(step.submitted()).toBe(false)

  await act(async () => {
    const input = field('Console ID')
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, 'c1')
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
  expect(next().getAttribute('aria-disabled')).toBeNull()
})

// Catches: the blocker naming a later field while an earlier one is also empty,
// and a choice menu whose id never reaches the button that takes focus.
it.each<[string, RecipeConfigurationView, ConfigurationReviewView, () => Element]>([
  [
    'the display name, before the recipe answer',
    serverRecipe,
    { ...review, values: { ...review.values, displayName: ' ' } },
    () => field('Display name'),
  ],
  [
    'the sign-in menu',
    { ...headerRecipe, values: { requestHeaders: headerRecipe.values.requestHeaders } },
    { ...review, servers: [], values: { ...review.values, authentication: '' } },
    () => signIn(),
  ],
])('moves focus to %s', async (_name, recipe, configuration, expected) => {
  const step = await renderStep(recipe, configuration)
  await act(async () => next().click())
  expect(document.activeElement).toBe(expected())
  expect(step.submitted()).toBe(false)
})

// A saved configuration that a new read of the source no longer supports.
const stale: ConfigurationReviewView = {
  ...review,
  confirmed: true,
  servers: [
    {
      declaration: '#/servers',
      tools: ['listSites'],
      options: [{ url: 'https://api.example.test', variables: [] }],
    },
  ],
  values: {
    ...review.values,
    authentication: 'bearer',
    servers: [
      { declaration: '#/servers', url: 'https://api.example.test', custom: false, variables: {} },
      {
        declaration: '#/paths/~1removed/servers',
        url: 'https://removed.example.test',
        custom: true,
        variables: {},
      },
    ],
  },
}
const button = (label: string) =>
  [...document.querySelectorAll('button')].find(
    (element) => element.getAttribute('aria-label') === label || element.textContent === label,
  )!
const describedBy = (element: Element) =>
  document.getElementById(element.getAttribute('aria-describedby') ?? '')?.textContent

// Catches: a saved server or method that the source dropped, shown apart from
// its field, or a Next that confirms a value Coral cannot apply.
it('marks a saved server and method that the source dropped on their fields', async () => {
  const step = await renderStep(null, stale)
  const server = field('Server for /removed')
  expect(server.getAttribute('aria-invalid')).toBe('true')
  expect(describedBy(server)).toBe(
    'The source does not have this saved server. Remove it to continue.',
  )
  // The menu offers only supported methods, so it shows its placeholder.
  const method = signIn()
  expect(method.getAttribute('aria-invalid')).toBe('true')
  expect(describedBy(method)).toBe(
    'This document does not support Bearer token. Choose a supported method.',
  )
  expect(alerts()).toHaveLength(2)

  await act(async () => next().click())
  expect(step.submitted()).toBe(false)

  await act(async () => button('Remove Server for /removed').click())
  expect(document.querySelector('[aria-label="Server for /removed"]')).toBeNull()
  expect(alerts()).toEqual([
    'This document does not support Bearer token. Choose a supported method.',
  ])
})
