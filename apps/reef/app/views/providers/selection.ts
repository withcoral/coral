/**
 * How a cross-provider policy selection travels: as keys in the page's
 * selection set, and as one form field per operation in a bulk submission.
 * The route action, the mutation queue, and the page all read this shape.
 */
export type SelectedOperation = { provider: string; operation: string }

/** A stable string for one selected operation, keyed by provider and name. */
export type SelectionKey = string

export const SELECTED_OPERATION_FIELD = 'selectedOperation'

export const selectionKey = (provider: string, operation: string): SelectionKey =>
  JSON.stringify([provider, operation])

export const parseSelectionKey = (key: SelectionKey): SelectedOperation => {
  const [provider, operation] = JSON.parse(key) as [string, string]
  return { provider, operation }
}

export const encodeSelectedOperation = (target: SelectedOperation) => JSON.stringify(target)

/**
 * Every selected operation in a submission, or `null` when any entry is not a
 * provider-and-operation pair. A form with no entries decodes to an empty list.
 */
export function decodeSelectedOperations(data: FormData): SelectedOperation[] | null {
  const targets: SelectedOperation[] = []
  for (const value of data.getAll(SELECTED_OPERATION_FIELD)) {
    if (typeof value !== 'string') return null
    let parsed: unknown
    try {
      parsed = JSON.parse(value)
    } catch {
      return null
    }
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      !('provider' in parsed) ||
      !('operation' in parsed) ||
      typeof parsed.provider !== 'string' ||
      typeof parsed.operation !== 'string'
    )
      return null
    targets.push({ provider: parsed.provider, operation: parsed.operation })
  }
  return targets
}
