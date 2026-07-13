import { tableFromIPC } from 'apache-arrow'

export interface OnboardingSampleQueryRow {
  source: string
  tables: bigint | number | string
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
