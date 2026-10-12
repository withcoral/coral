/** @vitest-environment happy-dom */

import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, expect, test } from 'vitest'

import { SourceStep } from './source-step'

const roots: Root[] = []
const routers: ReturnType<typeof createMemoryRouter>[] = []

afterEach(() => {
  for (const root of roots.splice(0)) act(() => root.unmount())
  for (const router of routers.splice(0)) router.dispose()
  document.body.innerHTML = ''
})

/** Renders the custom source form and returns what its next read submits. */
async function renderSource() {
  let submitted: FormData | undefined
  const router = createMemoryRouter(
    [
      {
        path: '*',
        action: async ({ request }) => {
          submitted = await request.formData()
          return { error: 'stop here' }
        },
        Component: () => (
          <SourceStep form={{ csrf: 'test-csrf', requestId: 'test-request' }} pending={false} />
        ),
      },
    ],
    { initialEntries: ['/workspaces/demo/providers/new'] },
  )
  const container = document.createElement('div')
  document.body.append(container)
  const root = createRoot(container)
  roots.push(root)
  routers.push(router)
  await act(async () => root.render(<RouterProvider router={router} />))
  return { submitted: () => submitted }
}

async function chooseFile(file: File) {
  const input = document.querySelector<HTMLInputElement>('input[aria-label="OpenAPI file"]')!
  const transfer = new DataTransfer()
  transfer.items.add(file)
  input.files = transfer.files
  await act(async () => input.dispatchEvent(new Event('change', { bubbles: true })))
}

async function fill(label: string, value: string) {
  const input = document.querySelector<HTMLInputElement>(`input[id$="-${label}-url"]`)!
  await act(async () => {
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!
    setter.call(input, value)
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
}

async function press(name: string) {
  const button = [...document.querySelectorAll('button')].find(
    (candidate) => candidate.textContent === name,
  )!
  await act(async () => {
    button.click()
    await new Promise((resolve) => setTimeout(resolve, 0))
  })
}

// Catches: a chosen OpenAPI file uploading with an MCP read, which Coral never
// reads but the browser still sends, up to the document size limit.
test.each([
  { read: 'Read', mcpUrl: '', source: 'file', sendsDocument: true },
  { read: 'Connect', mcpUrl: 'https://mcp.example.com/mcp', source: 'mcp', sendsDocument: false },
])(
  '$read after choosing a file sends source $source',
  async ({ read, mcpUrl, source, sendsDocument }) => {
    const { submitted } = await renderSource()
    await chooseFile(new File(['openapi: 3.1.0'], 'billing.yaml', { type: 'application/yaml' }))
    if (mcpUrl) await fill('mcp', mcpUrl)
    await press(read)

    const data = submitted()!
    expect(data.get('source')).toBe(source)
    expect(data.has('document')).toBe(sendsDocument)
  },
)
