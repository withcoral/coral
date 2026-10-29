/**
 * How a cross-provider policy selection travels: as keys in the page's
 * selection set, and as one form field per tool in a bulk submission.
 * The route action, the mutation queue, and the page all read this shape.
 */
export type SelectedTool = { provider: string; tool: string }

/** A stable string for one selected tool, keyed by provider and name. */
export type SelectionKey = string

export const SELECTED_TOOL_FIELD = 'selectedTool'

export const selectionKey = (provider: string, tool: string): SelectionKey =>
  JSON.stringify([provider, tool])

export const parseSelectionKey = (key: SelectionKey): SelectedTool => {
  const [provider, tool] = JSON.parse(key) as [string, string]
  return { provider, tool }
}

export const encodeSelectedTool = (target: SelectedTool) => JSON.stringify(target)

/**
 * Every selected tool in a submission, or `null` when any entry is not a
 * provider-and-tool pair. A form with no entries decodes to an empty list.
 */
export function decodeSelectedTools(data: FormData): SelectedTool[] | null {
  const targets: SelectedTool[] = []
  for (const value of data.getAll(SELECTED_TOOL_FIELD)) {
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
      !('tool' in parsed) ||
      typeof parsed.provider !== 'string' ||
      typeof parsed.tool !== 'string'
    )
      return null
    targets.push({ provider: parsed.provider, tool: parsed.tool })
  }
  return targets
}
