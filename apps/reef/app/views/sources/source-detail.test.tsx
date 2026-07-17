import { createMemoryRouter, RouterProvider } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { CatalogEntry } from '@/lib/sources'

import { SourceDetailView } from './source-detail'

const installedEntry: CatalogEntry = {
  description: 'Query GitHub data.',
  inputSpecs: [
    {
      hint: 'Choose the API endpoint.',
      input: { case: 'variable', value: { defaultValue: 'https://api.github.com' } },
      key: 'API_BASE_URL',
      required: true,
    },
    {
      hint: 'Use a token with **read access**.',
      input: {
        case: 'secret',
        value: {
          credential: {
            methods: [
              {
                description: 'Paste an existing token.',
                hint: 'Create one in GitHub settings.',
                label: 'Personal access token',
                method: { case: 'sourceConfig', value: {} },
              },
              {
                description: 'Authorize with GitHub.',
                hint: 'A browser window will open.',
                label: 'OAuth',
                method: { case: 'oauth', value: {} },
              },
            ],
          },
        },
      },
      key: 'GITHUB_TOKEN',
      required: true,
    },
  ],
  installed: true,
  name: 'github',
  origin: 'bundled',
  source: {
    name: 'github',
    origin: 'bundled',
    secrets: [{ key: 'GITHUB_TOKEN', value: '' }],
    variables: [{ key: 'API_BASE_URL', value: 'https://github.example.test' }],
    version: '1.0.0',
  },
  version: '1.0.0',
}

describe('SourceDetailView', () => {
  it('renders the source dialog from route data passed by the adapter', async () => {
    const screen = await renderSourceDetail({
      description: 'Query GitHub data.',
      installed: false,
      name: 'github',
      origin: 'bundled',
      version: '1.0.0',
    })

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Add source' })).toBeVisible()
  })

  it('keeps manifest copy, formatted fields, hints, and masked secrets after installation', async () => {
    const screen = await renderSourceDetail(installedEntry)
    const dialog = screen.getByRole('dialog')

    await expect.element(dialog).toBeVisible()
    await expect.element(screen.getByText('Query GitHub data.')).toBeVisible()
    await expect.element(screen.getByText('Api base url')).toBeVisible()
    await expect.element(screen.getByText('Github token')).toBeVisible()
    await expect.element(screen.getByText('Choose the API endpoint.')).toBeVisible()
    await expect.element(screen.getByText('Use a token with read access.')).toBeVisible()
    await expect
      .element(screen.getByLabelText('Api base url'))
      .toHaveValue('https://github.example.test')
    const secretInput = screen.getByLabelText('Github token')
    await expect.element(secretInput).toHaveValue('••••••••')

    await secretInput.click()
    await expect.element(secretInput).toHaveValue('')
    secretInput.element().blur()
    await expect.element(secretInput).toHaveValue('••••••••')
  })

  it('does not present a credential method as stored state', async () => {
    const screen = await renderSourceDetail(installedEntry)
    const dialog = screen.getByRole('dialog')

    await expect.element(dialog).toBeVisible()
    expect(dialog.element().querySelector('[role="tablist"]')).toBeNull()
    expect(dialog.element().textContent).not.toContain('Personal access token')
    expect(dialog.element().textContent).not.toContain('OAuth')
  })

  it('keeps imported source fields non-editable', async () => {
    const entry: CatalogEntry = {
      ...installedEntry,
      origin: 'imported',
      source: { ...installedEntry.source!, origin: 'imported' },
    }
    const screen = await renderSourceDetail(entry)

    await expect.element(screen.getByText('Imported', { exact: true })).toBeVisible()
    await expect
      .element(
        screen.getByText(
          "Imported sources can't be edited. Please remove and re-import the source spec",
        ),
      )
      .toBeVisible()
    await expect.element(screen.getByLabelText('Api base url')).toBeDisabled()
    await expect.element(screen.getByLabelText('Github token')).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Save changes' }).query()).toBeNull()
  })

  it('submits an imported source canonical name when removing it', async () => {
    const name = 'spotify_web_api_with_fixes_and_improvements_from_sonallux'
    const entry: CatalogEntry = {
      ...installedEntry,
      name,
      origin: 'imported',
      source: { ...installedEntry.source!, name, origin: 'imported' },
    }
    const screen = await renderSourceDetail(entry)

    await screen.getByRole('button', { name: 'Remove' }).click()
    await expect
      .element(screen.getByRole('heading', { name: /Remove spotify web api/ }))
      .toBeVisible()

    const dialogs = document.querySelectorAll<HTMLElement>(
      '[role="dialog"]:not([data-ending-style])',
    )
    const confirmation = dialogs.item(dialogs.length - 1)
    const nameInput = confirmation?.querySelector<HTMLInputElement>('input[name="name"]')
    expect(nameInput?.value).toBe(name)
  })
})

async function renderSourceDetail(entry: CatalogEntry) {
  const router = createMemoryRouter(
    [
      {
        element: (
          <SourceDetailView
            actionData={undefined}
            loaderData={{ entry, loadError: null }}
            sourcesPath="/workspaces/default/sources"
            workspaceId="default"
          />
        ),
        path: '/workspaces/:workspaceId/sources/:sourceName',
      },
    ],
    { initialEntries: [`/workspaces/default/sources/${entry.name}`] },
  )

  return render(<RouterProvider router={router} />)
}
