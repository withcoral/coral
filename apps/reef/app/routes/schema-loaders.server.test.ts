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
    const schema = {
      connectors: [
        {
          catalogName: 'github_v4',
          items: [{ kind: 'tableFunction', name: 'list_for_repo' }],
          name: 'issues',
        },
      ],
    }
    fetchSchemaFromCoral.mockResolvedValue(schema)

    await expect(
      schemaLoader({ params: { workspaceId: 'analytics' }, request } as Parameters<
        typeof schemaLoader
      >[0]),
    ).resolves.toEqual({ schema })
    expect(fetchSchemaFromCoral).toHaveBeenCalledWith(
      catalogClient,
      expect.objectContaining({ name: 'analytics' }),
      request.signal,
    )
  })

  it('lists table columns for the workspace route parameter', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/schema/github/issues')
    fetchTableColumnsFromCoral.mockResolvedValue([])

    await expect(
      schemaTableLoader({
        params: {
          schemaName: 'github',
          tableName: 'issues',
          workspaceId: 'analytics',
        },
        request,
      } as Parameters<typeof schemaTableLoader>[0]),
    ).resolves.toEqual({ columns: [] })
    expect(fetchTableColumnsFromCoral).toHaveBeenCalledWith(
      catalogClient,
      expect.objectContaining({ name: 'analytics' }),
      'github',
      'issues',
      request.signal,
    )
  })
})
