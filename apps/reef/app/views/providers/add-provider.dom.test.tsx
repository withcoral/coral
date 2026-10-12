/** @vitest-environment happy-dom */

import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import type { SourceChoice } from '@/lib/provider-recipes/choice'
import { renderRoute } from '@/test-support/route'

import { AddProviderLauncher } from './add-provider'

vi.mock('@/wax/components/toast', () => ({ addToast: vi.fn() }))
const { addToast } = await import('@/wax/components/toast')

afterEach(() => {
  vi.mocked(addToast).mockClear()
})

const recipes: SourceChoice[] = [
  { id: 'alpha', displayName: 'Alpha', description: '', kind: 'openapi', document: 'alpha.json' },
  { id: 'beta', displayName: 'Beta', description: '', kind: 'openapi', document: 'beta.json' },
]

// Catches: a catalog pick leaving the dialog before its read lands, another
// pick starting beside it, or a refusal closing the dialog without a reason.
test('a refused catalog read stays in the open dialog and says why', async () => {
  let refuse!: () => void
  const refused = new Promise<void>((resolve) => {
    refuse = resolve
  })
  const posted: FormData[] = []
  renderRoute(
    <AddProviderLauncher
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      pending={false}
      newPath="/workspaces/demo/providers/new"
      recipes={recipes}
    />,
    {
      entry: '/workspaces/demo/providers',
      action: async ({ request }) => {
        posted.push(await request.formData())
        await refused
        return { error: 'The document is not valid OpenAPI.' }
      },
    },
  )
  await userEvent.click(screen.getByRole('button', { name: 'Add provider' }))
  const dialog = await screen.findByRole('dialog')
  await userEvent.click(within(dialog).getByRole('button', { name: 'Add Alpha API' }))

  const alpha = within(dialog).getByRole('button', { name: 'Add Alpha API' })
  await vi.waitFor(() => expect(alpha).toHaveAttribute('aria-busy', 'true'))
  expect(within(dialog).getByRole('button', { name: 'Add Beta API' })).toBeDisabled()
  await userEvent.keyboard('{Escape}')
  expect(screen.getByRole('dialog')).toBe(dialog)
  expect(posted).toHaveLength(1)
  expect(Object.fromEntries(posted[0])).toMatchObject({
    intent: 'prepare',
    source: 'recipe',
    recipe: 'alpha',
    namespace: 'alpha',
  })

  await act(async () => refuse())
  await vi.waitFor(() => expect(addToast).toHaveBeenCalledTimes(1))
  expect(addToast).toHaveBeenCalledWith('error', {
    title: 'Coral could not read Alpha',
    description: 'The document is not valid OpenAPI.',
  })
  expect(screen.getByRole('dialog')).toBe(dialog)
  expect(alpha).not.toHaveAttribute('aria-busy')
  expect(within(dialog).getByRole('button', { name: 'Add Beta API' })).toBeEnabled()
})

// Catches: a service with two recipes shown as two cards, its card adding the
// wrong one of them, or cards in the order of recipe ids, not the names shown.
test('a service with an MCP and an OpenAPI recipe is one card that adds either', async () => {
  const posted: FormData[] = []
  let refused = Promise.resolve()
  renderRoute(
    <AddProviderLauncher
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      pending={false}
      newPath="/workspaces/demo/providers/new"
      recipes={[
        { ...recipes[1], id: 'a_beta' },
        recipes[0],
        {
          id: 'alpha_mcp',
          service: 'alpha',
          displayName: 'Alpha',
          description: '',
          kind: 'mcp',
          url: 'https://alpha.example/mcp',
        },
      ]}
    />,
    {
      entry: '/workspaces/demo/providers',
      action: async ({ request }) => {
        posted.push(await request.formData())
        await refused
        return { error: 'The server did not answer.' }
      },
    },
  )
  await userEvent.click(screen.getByRole('button', { name: 'Add provider' }))
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText('2 providers')).toBeInTheDocument()
  expect(
    within(dialog)
      .getAllByRole('button', { name: /^Add (Alpha|Beta)/ })
      .map((button) => button.getAttribute('aria-label')),
  ).toEqual(['Add Alpha MCP', 'Add Alpha API', 'Add Beta API'])

  let refuse!: () => void
  refused = new Promise<void>((resolve) => {
    refuse = resolve
  })
  await userEvent.click(within(dialog).getByRole('button', { name: 'Add Alpha MCP' }))
  await vi.waitFor(() =>
    expect(within(dialog).getByRole('button', { name: 'Add Alpha MCP' })).toHaveAttribute(
      'aria-busy',
      'true',
    ),
  )
  expect(within(dialog).getByRole('button', { name: 'Add Alpha API' })).toBeDisabled()
  await act(async () => refuse())
  await vi.waitFor(() => expect(posted).toHaveLength(1))
  expect(Object.fromEntries(posted[0])).toMatchObject({
    source: 'mcp',
    sourceUrl: 'https://alpha.example/mcp',
    recipe: 'alpha_mcp',
    // Both recipes suggest the service as the namespace.
    namespace: 'alpha',
  })
})

// Catches: a card that only its small source buttons answer, a click on the
// card adding the API when the service has an MCP, or a second read starting.
test.each([
  { card: 'Alpha', recipe: 'alpha_mcp', other: 'Add Beta API' },
  { card: 'Beta', recipe: 'beta', other: 'Add Alpha MCP' },
])('a click on the $card card adds its first source', async ({ card, recipe, other }) => {
  const posted: FormData[] = []
  let refuse!: () => void
  const refused = new Promise<void>((resolve) => {
    refuse = resolve
  })
  renderRoute(
    <AddProviderLauncher
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      pending={false}
      newPath="/workspaces/demo/providers/new"
      recipes={[
        recipes[0],
        recipes[1],
        {
          id: 'alpha_mcp',
          service: 'alpha',
          displayName: 'Alpha',
          description: '',
          kind: 'mcp',
          url: 'https://alpha.example/mcp',
        },
      ]}
    />,
    {
      entry: '/workspaces/demo/providers',
      action: async ({ request }) => {
        posted.push(await request.formData())
        await refused
        return { error: 'The server did not answer.' }
      },
    },
  )
  await userEvent.click(screen.getByRole('button', { name: 'Add provider' }))
  const dialog = await screen.findByRole('dialog')
  await userEvent.click(within(dialog).getByText(card))
  await vi.waitFor(() => expect(posted).toHaveLength(1))
  expect(Object.fromEntries(posted[0])).toMatchObject({ recipe })

  // While it reads, no other card starts a read.
  await vi.waitFor(() => expect(within(dialog).getByRole('button', { name: other })).toBeDisabled())
  await userEvent.click(within(dialog).getByText(card === 'Alpha' ? 'Beta' : 'Alpha'))
  await act(async () => refuse())
  expect(posted).toHaveLength(1)
})

const keyed: SourceChoice = {
  id: 'gamma',
  displayName: 'Gamma',
  description: '',
  kind: 'mcp',
  url: 'https://gamma.example/mcp',
  requestHeaders: [{ name: 'Authorization', value: '' }],
}

// Catches: a read posted by the source form, not the catalog, letting the
// dialog close mid-read and losing the refusal with the unmounted form.
test.each([
  {
    path: 'a typed address',
    choose: async (dialog: HTMLElement) => {
      await userEvent.click(within(dialog).getByRole('tab', { name: 'Add custom' }))
      await userEvent.type(
        within(dialog).getByLabelText('MCP server URL'),
        'https://delta.example/mcp',
      )
    },
  },
  {
    path: 'a recipe that asks for a header value',
    choose: async (dialog: HTMLElement) => {
      await userEvent.click(within(dialog).getByRole('button', { name: 'Add Gamma MCP' }))
    },
  },
])('a refused read of $path stays in the open dialog', async ({ choose }) => {
  let refuse!: () => void
  const refused = new Promise<void>((resolve) => {
    refuse = resolve
  })
  renderRoute(
    <AddProviderLauncher
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      pending={false}
      newPath="/workspaces/demo/providers/new"
      recipes={[keyed]}
    />,
    {
      entry: '/workspaces/demo/providers',
      action: async () => {
        await refused
        return { error: 'The server did not answer.' }
      },
    },
  )
  await userEvent.click(screen.getByRole('button', { name: 'Add provider' }))
  const dialog = await screen.findByRole('dialog')
  await choose(dialog)
  await userEvent.click(within(dialog).getByRole('button', { name: 'Connect' }))
  await vi.waitFor(() =>
    expect(within(dialog).getByRole('form', { name: 'New source' })).toHaveAttribute(
      'aria-busy',
      'true',
    ),
  )
  await userEvent.keyboard('{Escape}')
  expect(screen.getByRole('dialog')).toBe(dialog)

  await act(async () => refuse())
  // The form says why next to the address, so no toast repeats it.
  await vi.waitFor(() =>
    expect(within(dialog).getByText('The server did not answer.')).toBeInTheDocument(),
  )
  expect(addToast).not.toHaveBeenCalled()
  expect(screen.getByRole('dialog')).toBe(dialog)
})
