import type { MiddlewareRow } from './types'

const scopes = {
  run: 'execution',
  function: 'function call',
  operation: 'operation',
  unknown: 'call',
} as const

/** "audit around coral.providers.linear.listIssues", or "audit around execution". */
export function middlewareLabel(row: Pick<MiddlewareRow, 'middlewareName' | 'target' | 'scope'>) {
  return `${row.middlewareName} around ${row.target || scopes[row.scope]}`
}
