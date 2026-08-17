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
      connectors: [
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

  it('keeps matching schema names in different catalogs separate', async () => {
    const listCatalog = vi.fn().mockResolvedValue({
      items: [
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'github_v4',
              name: 'issues',
              schemaName: 'api',
            },
          },
        },
        {
          item: {
            case: 'table',
            value: {
              catalogName: 'linear_v4',
              name: 'issues',
              schemaName: 'api',
            },
          },
        },
      ],
    })
    const workspace = create(WorkspaceSchema, { name: 'analytics' })

    await expect(
      fetchSchemaFromCoral({ listCatalog } as unknown as CatalogClient, workspace),
    ).resolves.toEqual({
      connectors: [
        {
          catalogName: 'github_v4',
          items: [expect.objectContaining({ kind: 'table', name: 'issues' })],
          name: 'api',
        },
        {
          catalogName: 'linear_v4',
          items: [expect.objectContaining({ kind: 'table', name: 'issues' })],
          name: 'api',
        },
      ],
    })
  })
})

describe('fetchTableColumnsFromCoral', () => {
  it('passes the catalog coordinate to ListColumns', async () => {
    const listColumns = vi.fn().mockResolvedValue({ columns: [] })
    const workspace = create(WorkspaceSchema, { name: 'analytics' })

    await expect(
      fetchTableColumnsFromCoral(
        { listColumns } as unknown as CatalogClient,
        workspace,
        'github_v4',
        'api',
        'issues',
      ),
    ).resolves.toEqual([])
    expect(listColumns.mock.calls[0]?.[0]).toMatchObject({
      catalogName: 'github_v4',
      schemaName: 'api',
      tableName: 'issues',
      workspace,
    })
  })
})
