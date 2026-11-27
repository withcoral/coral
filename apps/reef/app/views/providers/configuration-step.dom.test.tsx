/** @vitest-environment happy-dom */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'

import { ConfigurationStep } from './configuration-step'
import type {
  ConfigurationReviewView,
  RecipeConfigurationView,
  SetupView,
  StepperResult,
} from './types'

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
      required: true,
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

async function renderStep(
  recipe: RecipeConfigurationView | null,
  configuration = review,
  result?: StepperResult,
) {
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
            result={result}
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

// Catches: Reef deciding for itself which servers need an answer. Coral sends
// that with each server; an unanswered required one blocks Next.
it.each([true, false])(
  'blocks Next for an unanswered server only when Coral marks it required (%s)',
  async (required) => {
    const step = await renderStep(null, {
      ...review,
      servers: [{ ...review.servers[0], required }],
    })

    expect(field('Main server').getAttribute('aria-required') === 'true').toBe(required)
    expect(next().getAttribute('aria-disabled') === 'true').toBe(required)
    await act(async () => next().click())
    expect(step.submitted()).toBe(!required)
  },
)

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
      required: true,
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

/** Two per-tool servers built from one site answer, as in Datadog's recipe. */
const siteServers = ['/logs', '/metrics'].map(
  (path) => `#/paths/~1v2${path.replace('/', '~1')}/servers`,
)
const site = { ...consoleId, name: 'site', label: 'Site', defaultValue: 'datadoghq.eu' }
const siteRecipe: RecipeConfigurationView = {
  id: 'datadog_v2',
  inputs: [site],
  suggestedMethods: ['none'],
  values: {
    authentication: 'none',
    servers: siteServers.map((declaration) => ({
      declaration,
      url: 'https://api.{{input.site}}',
      custom: true,
      variables: {},
    })),
  },
}
const siteReview: ConfigurationReviewView = {
  ...review,
  servers: siteServers.map((declaration) => ({
    declaration,
    tools: ['submit'],
    required: true,
    options: [{ url: 'https://{subdomain}.{site}', variables: [] }],
  })),
}
// The server fields above the box of default servers, and the ones inside it.
// The box is the last part of the Servers section, under its Servers heading.
const serverFields = (inBox: boolean) => {
  const heading = screen.queryByRole('heading', { name: 'Servers', level: 4 })
  const after = (label: Element) =>
    heading !== null &&
    Boolean(heading.compareDocumentPosition(label) & Node.DOCUMENT_POSITION_FOLLOWING)
  return screen
    .queryAllByText(/^Server for /, { selector: 'label' })
    .filter((label) => after(label) === inBox)
    .map((label) => label.textContent)
}

// Catches: a list of servers that each hold the recipe's URL making the part of
// Setup that needs nothing the longest part of it, or one with an error hidden
// in the box among the ones that need nothing.
it.each<[string, ConfigurationReviewView, string[], string[]]>([
  [
    'every server holds the suggested URL',
    siteReview,
    [],
    ['Server for /v2/logs', 'Server for /v2/metrics'],
  ],
  [
    'one server has an error',
    { ...siteReview, errors: { [siteServers[1]]: 'Enter a reachable URL.' } },
    ['Server for /v2/metrics'],
    ['Server for /v2/logs'],
  ],
])('boxes the suggested servers when %s', async (_case, configuration, shown, boxed) => {
  await renderStep(siteRecipe, configuration)
  expect(serverFields(false)).toEqual(shown)
  expect(serverFields(true)).toEqual(boxed)
  expect(document.body.textContent).toContain(
    'Each tool sends its requests to one of these URLs. Based on the Site config above.',
  )
})

// Catches: guidance that names the site for a box that also holds a fixed URL,
// which changing the site does not move.
it('names no question when a boxed server does not use it', async () => {
  const mixed: RecipeConfigurationView = {
    ...siteRecipe,
    values: {
      ...siteRecipe.values,
      servers: [
        siteRecipe.values.servers![0],
        { ...siteRecipe.values.servers![1], url: 'https://metrics.example.test' },
      ],
    },
  }
  await renderStep(mixed, siteReview)
  expect(serverFields(true)).toEqual(['Server for /v2/logs', 'Server for /v2/metrics'])
  expect(document.body.textContent).toContain('Each tool sends its requests to one of these URLs.')
  expect(document.body.textContent).not.toContain('Based on')
})

// Catches: a link to the question the URLs come from that leaves focus where it
// was, so a keyboard user still has to find the field, or that leaves the field
// under the pinned step header.
it('moves focus to the question the default servers come from', async () => {
  await renderStep(siteRecipe, siteReview)
  const link = [...document.querySelectorAll('a')].find((a) => a.textContent === 'Site')!
  const target = document.getElementById(link.getAttribute('href')!.slice(1))!
  const scrollIntoView = vi.fn()
  target.scrollIntoView = scrollIntoView
  await act(async () => link.click())
  expect(document.activeElement).toBe(target)
  // In the middle of the view, where the pinned step header cannot cover it.
  expect(scrollIntoView).toHaveBeenCalledWith(expect.objectContaining({ block: 'center' }))
})

// Catches: a server that leaves the box, and moves the field, while the person
// types a URL that differs from the suggested one.
it('keeps an edited server in the box', async () => {
  await renderStep(siteRecipe, siteReview)
  const input = field('Server for /v2/logs')
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(
      input,
      'https://logs.example.test',
    )
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
  expect(field('Server for /v2/logs').value).toBe('https://logs.example.test')
  expect(serverFields(true)).toEqual(['Server for /v2/logs', 'Server for /v2/metrics'])
})

// Catches: a refused automatic client setup shown as a banner at the foot of the
// page, far from the Client ID field that gets the person past it, with no word
// on how to get a client ID.
it('shows a refused automatic client setup on Client ID, in place of its hint', async () => {
  const message =
    'auth.example.test did not accept automatic client registration. ' +
    'Register an OAuth application with it. ' +
    'Add the callback URL to its allowed redirect URLs. Then enter its client ID here.'
  await renderStep(
    null,
    {
      ...review,
      authenticationMethods: ['oauth'],
      servers: [],
      values: {
        ...review.values,
        authentication: 'oauth',
        authorizationUrl: 'https://auth.example.test/authorize',
        tokenUrl: 'https://auth.example.test/token',
      },
    },
    {
      error: null,
      credential: null,
      configurationErrors: {
        'application.client_id':
          'The service did not accept automatic client registration. ' +
          'Register an OAuth application with the service, and give its client ID.',
      },
    },
  )
  const clientId = field('Client ID')
  expect(clientId.getAttribute('aria-invalid')).toBe('true')
  expect(clientId.required).toBe(true)
  expect(document.activeElement).toBe(clientId)
  expect(
    clientId
      .getAttribute('aria-describedby')!
      .split(' ')
      .map((id) => document.getElementById(id)?.textContent),
  ).toEqual([message])
  // Said once, as the focused field's description: not also an alert, no hint
  // beside it, and no banner repeating it.
  expect(document.body.textContent).not.toContain('Leave empty to use automatic client setup.')
  expect(alerts()).toEqual([])
  expect(document.body.textContent?.split(message).length).toBe(2)
})
