import { beforeEach, describe, expect, it, vi } from 'vitest'

const { catalogClientForRequest, fetchSchemaFromCoral, fetchTableColumnsFromCoral } = vi.hoisted(
  () => ({
    catalogClientForRequest: vi.fn(),
    fetchSchemaFromCoral: vi.fn(),
    fetchTableColumnsFromCoral: vi.fn(),
  }),
)

vi.mock('@/lib/coral-request.server', () => ({ catalogClientForRequest }))
vi.mock('@/lib/schema-explorer', () => ({ fetchSchemaFromCoral, fetchTableColumnsFromCoral }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'

import { loader as schemaLoader } from './schema'
import { loader as schemaTableLoader } from './schema-table'

describe('schema loaders', () => {
  const catalogClient = { name: 'catalog-client' }

  beforeEach(() => {
    vi.clearAllMocks()
    catalogClientForRequest.mockReturnValue(catalogClient)
  })

  it('lists catalog tables for the workspace route parameter', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/schema')
    const schema = { connectors: [] }
    fetchSchemaFromCoral.mockResolvedValue(schema)

    await expect(
      schemaLoader(authRouteTestArgs(request, { workspaceId: 'analytics' })),
    ).resolves.toEqual({ schema })
    expect(fetchSchemaFromCoral).toHaveBeenCalledWith(
      catalogClient,
      expect.objectContaining({ name: 'analytics' }),
      request.signal,
    )
    expect(catalogClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
  })

  it('lists table columns for the workspace route parameter', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/schema/github/issues')
    fetchTableColumnsFromCoral.mockResolvedValue([])

    await expect(
      schemaTableLoader(
        authRouteTestArgs(request, {
          schemaName: 'github',
          tableName: 'issues',
          workspaceId: 'analytics',
        }),
      ),
    ).resolves.toEqual({ columns: [] })
    expect(fetchTableColumnsFromCoral).toHaveBeenCalledWith(
      catalogClient,
      expect.objectContaining({ name: 'analytics' }),
      'github',
      'issues',
      request.signal,
    )
    expect(catalogClientForRequest).toHaveBeenCalledWith(request, 'test-coral-token')
  })
})
