import type { ActivityRow, ActivityTrace } from './types'

/** What one record did to the agent's context, in bytes. */
export interface RecordFlow {
  /** Bytes that reached the agent: a discovery response, or a program's output. */
  into?: number
  /** Tool result bytes a program read and did not pass on. */
  keptOut?: number
  /** A call's tool result, which its program read in place of the agent. */
  result?: number
}

/**
 * Where a root's bytes went. Coral's Context Impact is signed: a discovery adds its response,
 * and a Run adds its output minus the tool results its calls read. This splits the two apart,
 * so nothing nets what reached the agent against what a program kept from it.
 */
export interface ContextFlow {
  records: Map<string, RecordFlow>
  total: {
    /** Bytes that reached the agent. */
    into: number
    /** Tool result bytes the programs read. */
    providerData: number
    /** What the programs gave back of them. */
    output: number
    /** False when some work has no measurement, or the trace stops short. */
    complete: boolean
    /** Programs still running, whose bytes Coral measures when they end. */
    running: number
    /** Searches, describes and ended programs Coral has no measurement for. */
    unmeasured: number
  }
}

/** The tool interactions whose response reaches the agent's context, as Coral counts them. */
const COUNTED_TOOLS = new Set(['search', 'describe', 'exec'])

const measured = (record: ActivityRow) =>
  'contextImpactBytes' in record && record.contextImpactBytes !== undefined
    ? Number(record.contextImpactBytes)
    : undefined

export function contextFlow(trace: ActivityTrace): ContextFlow {
  const records = new Map<string, RecordFlow>()
  const total = {
    into: 0,
    providerData: 0,
    output: 0,
    complete: trace.complete,
    running: 0,
    unmeasured: 0,
  }
  for (const record of Object.values(trace.children).flat()) {
    const bytes = measured(record)
    if (record.kind === 'invocation') {
      // Coral stores a call's tool result as a negative number.
      if (bytes !== undefined) records.set(record.name, { result: Math.abs(bytes) })
      continue
    }
    // Coral counts a search, a describe and a submission's program; a save adds nothing.
    if (record.kind === 'tool' && !COUNTED_TOOLS.has(record.tool)) continue
    if (record.kind !== 'tool' && record.kind !== 'run') continue
    if (bytes === undefined) {
      total.complete = false
      if (record.kind === 'run' && record.state === 'running') total.running += 1
      else total.unmeasured += 1
      continue
    }
    if (record.kind === 'tool') {
      records.set(record.name, { into: bytes })
      total.into += bytes
      continue
    }
    // A Run's Context Impact is its output minus what its calls read, so the two give its output.
    const read = (trace.children[record.name] ?? []).reduce(
      (sum, child) => sum + (child.kind === 'invocation' ? Math.abs(measured(child) ?? 0) : 0),
      0,
    )
    const output = bytes + read
    // A program that writes more than it reads keeps nothing out.
    records.set(record.name, { into: output, keptOut: Math.max(0, read - output) })
    total.into += output
    total.providerData += read
    total.output += output
  }
  return { records, total }
}
