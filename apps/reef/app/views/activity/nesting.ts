import type { ActivityRow } from './types'

/**
 * How many levels each nested call in `rows` sits under the program's own calls. A call sits
 * one level under the call whose body made it only while it follows that call's own rows:
 * the call itself, or the calls nested under it. Logs and emissions between them do not part
 * them. A call that another call parts from its caller, or whose caller `rows` does not hold,
 * sits at the top, so an indent never points at a call that did not make it. Its caller
 * label names the call that did.
 */
export function callDepths(rows: readonly ActivityRow[]): Map<string, number> {
  // The calls each call sits under, nearest first.
  const under = new Map<string, string[]>()
  const depths = new Map<string, number>()
  let previous: string | undefined
  for (const row of rows) {
    if (row.kind !== 'invocation') continue
    const above = previous === undefined ? [] : [previous, ...(under.get(previous) ?? [])]
    const parent = row.parent ? above.indexOf(row.parent.invocation) : -1
    const chain = parent === -1 ? [] : above.slice(parent)
    under.set(row.name, chain)
    if (chain.length) depths.set(row.name, chain.length)
    previous = row.name
  }
  return depths
}
