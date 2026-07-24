import { create } from '@bufbuild/protobuf'
import { tableFromIPC } from 'apache-arrow'

import { ExecuteSqlRequestSchema } from '@/generated/coral/v1/query_pb'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { queryClientForRequest } from './coral-request.server'
import {
  ONBOARDING_SAMPLE_QUERY,
  type OnboardingSampleQueryResult,
  type OnboardingSampleQueryRow,
} from './onboarding-query'
import { errorMessage } from './utils'

export async function loadOnboardingSampleQuery(
  request: Request,
  workspaceId: string,
): Promise<OnboardingSampleQueryResult> {
  try {
    const response = await queryClientForRequest(request).executeSql(
      create(ExecuteSqlRequestSchema, {
        sql: ONBOARDING_SAMPLE_QUERY,
        workspace: create(WorkspaceSchema, { name: workspaceId }),
      }),
    )
    const rows = decodeOnboardingSampleQueryRows(response.arrowIpcStream).map((row) => ({
      ...row,
      tables: row.tables.toString(),
    }))
    return { rows, status: 'success' }
  } catch (error) {
    return { message: errorMessage(error), status: 'error' }
  }
}

export function decodeOnboardingSampleQueryRows(
  arrowIpcStream: Uint8Array,
): OnboardingSampleQueryRow[] {
  const table = tableFromIPC(arrowIpcStream)
  const sourceColumn = table.getChild('source')
  const tablesColumn = table.getChild('tables')

  if (!sourceColumn || !tablesColumn) {
    throw new Error('The sample query returned an unexpected result shape.')
  }

  return Array.from({ length: table.numRows }, (_, index) => {
    const source = sourceColumn.get(index)
    const tables = tablesColumn.get(index)

    if (typeof source !== 'string') {
      throw new Error('The sample query returned an invalid source name.')
    }

    return { source, tables: queryCount(tables) }
  })
}

function queryCount(value: unknown): bigint | number | string {
  if (typeof value === 'bigint' || typeof value === 'number' || typeof value === 'string') {
    return value
  }
  if (value !== null && value !== undefined) return String(value)
  throw new Error('The sample query returned an invalid table count.')
}
