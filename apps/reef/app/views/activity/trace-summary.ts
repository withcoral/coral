import type { ActivityRow, ActivityTrace } from './types'

type ToolRow = Extract<ActivityRow, { kind: 'tool' }>
type CallRow = Extract<ActivityRow, { kind: 'invocation' }>

const byStart = (a: ActivityRow, b: ActivityRow) =>
  Date.parse(a.startedAt) - Date.parse(b.startedAt)
const ended = (row: CallRow) => (row.endedAt ? Date.parse(row.endedAt) : Date.parse(row.startedAt))

/** One function a root saved: its saves in order, the first then each revision, and its path
 * when a later Run called it by one. */
export interface SavedFunction {
  key: string
  path?: string
  saves: ToolRow[]
  /** A Run that succeeded called it after its last save. */
  tested: boolean
}

/** The functions a root saved, in the order it first saved each. */
export function savedFunctionsOf(trace: ActivityTrace): SavedFunction[] {
  const functions = new Map<string, SavedFunction>()
  const rows = Object.values(trace.children)
    .flat()
    .filter(
      (row): row is ToolRow =>
        row.kind === 'tool' && row.tool === 'save_function' && row.outcome === 'succeeded',
    )
    .toSorted(byStart)
  for (const row of rows) {
    const saved = trace.saves?.[row.name]
    const key = saved?.savedFunction ?? row.name
    const entry = functions.get(key) ?? {
      key,
      saves: [],
      tested: false,
      ...(saved?.path && { path: saved.path }),
    }
    entry.saves.push(row)
    // Saves come in order, so the last one decides.
    entry.tested = saved?.tested === true
    functions.set(key, entry)
  }
  return [...functions.values()]
}

/** A provider's calls in a root: its Call Path root, one call to name it by, how many, and how
 * many failed. */
export interface ProviderCalls {
  callRoot: string
  callPath: string
  count: number
  failed: number
}

/** A root's calls by the provider that served them, the busiest first. */
export function callsByProvider(trace: ActivityTrace): ProviderCalls[] {
  const providers = new Map<string, ProviderCalls>()
  for (const row of Object.values(trace.children).flat()) {
    if (row.kind !== 'invocation') continue
    const callRoot = row.callPath.split('.').slice(0, 3).join('.')
    const entry = providers.get(callRoot) ?? {
      callRoot,
      callPath: row.callPath,
      count: 0,
      failed: 0,
    }
    entry.count += 1
    if (row.outcome === 'coral_failure') entry.failed += 1
    providers.set(callRoot, entry)
  }
  return [...providers.values()].toSorted((a, b) => b.count - a.count)
}

/**
 * A Run's calls in order, the time from the first's start to the last's end, and whether any
 * ran at the same time as another.
 */
export function runCalls(trace: ActivityTrace, run: string) {
  const calls = (trace.children[run] ?? [])
    .filter((row): row is CallRow => row.kind === 'invocation')
    .toSorted(byStart)
  if (!calls.length) return { calls, span: 0, parallel: false }
  const span =
    Math.max(...calls.map(ended)) - Math.min(...calls.map((row) => Date.parse(row.startedAt)))
  const parallel = calls.some(
    (row, index) => index > 0 && Date.parse(row.startedAt) < ended(calls[index - 1]!),
  )
  return { calls, span, parallel }
}

/** A problem with one tool that the agent described in its summary. */
export interface ReportedIssue {
  /** The tool's name, as the summary and the Call Path end with it. */
  tool: string
  callPath: string
  /** The provider's resource, workspaces/{workspace}/providers/{provider}. */
  provider: string
  /** The summary's sentence that names the tool and the problem, as the agent wrote it. */
  sentence: string
  /** The calls of that tool in the trace. */
  calls: string[]
}

/** Words that describe something going wrong, as an agent reports it in prose. */
const PROBLEM =
  /\b(but|bug|broken|fails?|failed|wrong|incorrect|mismatch(es|ed)?|matched nothing|returned nothing|empty|doesn't|does not|didn't|did not|ignores?|ignored|instead|unexpected(ly)?)\b/i

/**
 * The tools the agent's summary reports a problem with: a sentence that names a tool of the
 * trace by its own name and describes something going wrong. Coral keeps no structured report
 * from the agent, so this reads its prose, and a sentence that only names a tool reports nothing.
 */
export function reportedIssues(trace: ActivityTrace): ReportedIssue[] {
  if (!trace.feedback) return []
  const sentences = trace.feedback.split(/(?<=[.!?])\s+/)
  const byTool = new Map<string, CallRow[]>()
  for (const row of Object.values(trace.children).flat())
    if (row.kind === 'invocation') {
      const tool = row.callPath.split('.').at(-1)!
      byTool.set(tool, [...(byTool.get(tool) ?? []), row])
    }
  return [...byTool].flatMap(([tool, rows]) => {
    const named = new RegExp(`(^|[^\\w])${tool.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^\\w]|$)`)
    const sentence = sentences.find((text) => named.test(text) && PROBLEM.test(text))
    return sentence
      ? [
          {
            tool,
            callPath: rows[0]!.callPath,
            provider: rows[0]!.provider,
            sentence,
            calls: rows.map((row) => row.name),
          },
        ]
      : []
  })
}
