import { create } from '@bufbuild/protobuf'
import { describe, expect, it, vi } from 'vitest'

import { CatalogItemKind } from '@/generated/coral/v1/catalog_pb'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { fetchSchemaFromCoral, fetchTableColumnsFromCoral } from './schema-explorer'
import type { CatalogClient } from './schema-explorer'

describe('fetchSchemaFromCoral', () => {
  it('groups tables and table functions from the unified catalog response', async () => {
    const listCatalog = vi.fn().mockResolvedValue({
      items: [
        {
          item: {
            case: 'tableFunction',
            value: {
              arguments: [
                {
                  name: 'channel',
                  required: true,
                  values: ['general', 'random'],
                },
              ],
              description: 'Lists messages in a channel.',
              name: 'messages',
              resultColumns: [
                {
                  dataType: 'Utf8',
                  description: 'Message text.',
                  name: 'text',
                  nullable: false,
                },
              ],
              schemaName: 'slack',
            },
          },
        },
        {
          item: {
            case: 'table',
            value: {
              description: 'Slack users.',
              name: 'users',
              requiredFilters: [],
              schemaName: 'slack',
            },
          },
        },
      ],
    })
    const workspace = create(WorkspaceSchema, { name: 'analytics' })

    await expect(
      fetchSchemaFromCoral({ listCatalog } as unknown as CatalogClient, workspace),
    ).resolves.toEqual({
      catalogs: [],
      schemas: [
        {
          items: [
            {
              arguments: [
                {
                  name: 'channel',
                  required: true,
                  values: ['general', 'random'],
                },
              ],
              description: 'Lists messages in a channel.',
              kind: 'tableFunction',
              name: 'messages',
              resultColumns: [
                {
                  description: 'Message text.',
                  name: 'text',
                  nullable: false,
                  type: 'Utf8',
                },
              ],
            },
            {
              columns: [],
              columnsLoaded: false,
              description: 'Slack users.',
              kind: 'table',
              name: 'users',
              requiredFilters: [],
            },
          ],
          name: 'slack',
        },
      ],
    })
    expect(listCatalog).toHaveBeenCalledOnce()
    expect(listCatalog.mock.calls[0]?.[0]).toMatchObject({
      kind: CatalogItemKind.UNSPECIFIED,
      workspace,
    })
  })

  it('groups database tables into catalogs and provider schemas', async () => {
    const listCatalog = vi.fn().mockResolvedValue({
      items: [
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'commerce',
              description: '',
              name: 'products',
              requiredFilters: [],
              schemaName: 'public',
            },
          },
        },
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'commerce',
              description: '',
              name: 'sales',
              requiredFilters: [],
              schemaName: 'public',
            },
          },
        },
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'commerce',
              description: '',
              name: 'revenue_by_product',
              requiredFilters: [],
              schemaName: 'analytics',
            },
          },
        },
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'warehouse',
              description: '',
              name: 'products',
              requiredFilters: [],
              schemaName: 'public',
            },
          },
        },
      ],
    })
    const workspace = create(WorkspaceSchema, { name: 'analytics' })

    await expect(
      fetchSchemaFromCoral({ listCatalog } as unknown as CatalogClient, workspace),
    ).resolves.toEqual({
      catalogs: [
        {
          name: 'commerce',
          schemas: [
            {
              items: [
                expect.objectContaining({ kind: 'table', name: 'products' }),
                expect.objectContaining({ kind: 'table', name: 'sales' }),
              ],
              name: 'public',
            },
            {
              items: [expect.objectContaining({ kind: 'table', name: 'revenue_by_product' })],
              name: 'analytics',
            },
          ],
        },
        {
          name: 'warehouse',
          schemas: [
            {
              items: [expect.objectContaining({ kind: 'table', name: 'products' })],
              name: 'public',
            },
          ],
        },
      ],
      schemas: [],
    })
  })
})

describe('fetchTableColumnsFromCoral', () => {
  it('passes the complete database table identity to ListColumns', async () => {
    const listColumns = vi.fn().mockResolvedValue({ columns: [] })
    const workspace = create(WorkspaceSchema, { name: 'analytics' })

    await fetchTableColumnsFromCoral({ listColumns } as unknown as CatalogClient, workspace, {
      catalogName: 'commerce',
      schemaName: 'public',
      tableName: 'products',
    })

    expect(listColumns).toHaveBeenCalledWith(
      expect.objectContaining({
        catalogName: 'commerce',
        schemaName: 'public',
        tableName: 'products',
      }),
      expect.anything(),
    )
  })
})
