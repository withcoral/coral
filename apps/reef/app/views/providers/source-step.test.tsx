// @vitest-environment happy-dom

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { expect, test, vi } from 'vitest'

import { SourceStep } from './source-step'
import { AddProviderEntry } from './add-provider'
import type { StepperResult } from './types'
import type { SourceChoice } from '@/lib/provider-recipes/choice'

const refused = (): StepperResult => ({
  error: 'Coral could not read the document or server.',
  credential: null,
})

// Stands in for the next read that Coral refuses.
function Page() {
  const [result, setResult] = useState(refused)
  return (
    <>
      <SourceStep
        form={{ csrf: 'test-csrf', requestId: 'test-request' }}
        pending={false}
        result={result}
      />
      <button type="button" onClick={() => setResult(refused())}>
        Refuse again
      </button>
    </>
  )
}

// A refusal names the address it refused. Changing the address takes the
// correction, so the old refusal goes; the next refusal shows again.
test('a refused source clears its error when the address changes, and a new refusal shows', async () => {
  const user = userEvent.setup()
  const Stub = createRoutesStub([{ path: '/', Component: Page }])
  render(<Stub initialEntries={['/']} />)

  const address = screen.getByLabelText('MCP server URL')
  expect(screen.getByRole('alert')).toHaveTextContent(
    'Coral could not read the document or server.',
  )
  expect(address).toHaveAttribute('aria-invalid', 'true')

  await user.type(address, 'https://mcp.example.test/mcp')
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  expect(address).not.toHaveAttribute('aria-invalid', 'true')

  await user.click(screen.getByRole('button', { name: 'Refuse again' }))
  expect(screen.getByRole('alert')).toHaveTextContent(
    'Coral could not read the document or server.',
  )
  expect(address).toHaveAttribute('aria-invalid', 'true')
})

test('an instance recipe waits for a valid address and reads the filled source URL', async () => {
  const user = userEvent.setup()
  const read = vi.fn(async ({ request }: { request: Request }) => {
    const form = await request.formData()
    expect(form.get('namespace')).toBe('instance')
    return { error: String(form.get('sourceUrl')), credential: null }
  })
  const recipe: SourceChoice = {
    id: 'instance_mcp',
    service: 'instance',
    displayName: 'Instance',
    description: '',
    kind: 'mcp',
    url: '{{input.instance}}/mcp-server/http',
    sourceInputs: [
      {
        name: 'instance',
        label: 'Instance URL',
        description: '',
        type: 'url',
        defaultValue: '',
        choices: [],
      },
    ],
  }
  const Stub = createRoutesStub([
    {
      path: '/',
      action: read,
      Component: () => (
        <SourceStep form={{ csrf: 'csrf', requestId: 'request' }} pending={false} recipe={recipe} />
      ),
    },
  ])
  render(<Stub initialEntries={['/']} />)
  await user.click(screen.getByRole('button', { name: 'Connect' }))
  expect(screen.getByRole('alert')).toHaveTextContent('Enter the source details.')
  expect(read).not.toHaveBeenCalled()
  await user.type(screen.getByLabelText('Instance URL'), 'https://tenant.example.test')
  expect(screen.getByLabelText('MCP server URL')).toHaveValue(
    'https://tenant.example.test/mcp-server/http',
  )
  await user.click(screen.getByRole('button', { name: 'Connect' }))
  expect(await screen.findByRole('alert')).toHaveTextContent(
    'https://tenant.example.test/mcp-server/http',
  )
  expect(read).toHaveBeenCalledOnce()
})

test('choosing an instance recipe in the catalog asks for its source details before reading', async () => {
  const user = userEvent.setup()
  const read = vi.fn(() => ({ error: 'Unexpected read', credential: null }))
  const recipe: SourceChoice = {
    id: 'tenant',
    displayName: 'Tenant service',
    description: '',
    kind: 'mcp',
    url: 'https://{{input.hostname}}/mcp',
    sourceInputs: [
      {
        name: 'hostname',
        label: 'Service hostname',
        description: '',
        type: 'hostname',
        defaultValue: '',
        choices: [],
      },
    ],
  }
  const Stub = createRoutesStub([
    {
      path: '/',
      action: read,
      Component: () => (
        <AddProviderEntry
          form={{ csrf: 'csrf', requestId: 'request' }}
          pending={false}
          newPath="/"
          recipes={[recipe]}
        />
      ),
    },
  ])
  render(<Stub initialEntries={['/']} />)
  await user.click(screen.getByRole('button', { name: 'Add Tenant service MCP' }))
  expect(screen.getByLabelText('Service hostname')).toBeInTheDocument()
  expect(read).not.toHaveBeenCalled()
})

test('a regional source picker has the input label as its accessible name', () => {
  const recipe: SourceChoice = {
    id: 'regional',
    displayName: 'Regional',
    description: '',
    kind: 'mcp',
    url: '{{input.region}}',
    sourceInputs: [
      {
        name: 'region',
        label: 'MCP region',
        description: '',
        type: 'url',
        defaultValue: 'https://us.example.test/mcp',
        choices: ['https://us.example.test/mcp', 'https://eu.example.test/mcp'],
      },
    ],
  }
  const Stub = createRoutesStub([
    {
      path: '/',
      Component: () => (
        <SourceStep form={{ csrf: 'csrf', requestId: 'request' }} pending={false} recipe={recipe} />
      ),
    },
  ])
  render(<Stub initialEntries={['/']} />)
  expect(screen.getByRole('button', { name: 'MCP region' })).toBeInTheDocument()
})
