// How Activity names a record wherever it lists one: its label, its kind icon, and the
// status it reports. The list, the waterfall and the detail share these.
import type { IconName } from '@/wax/components/icon'
import type * as Table from '@/wax/components/table'
import { toolOwner } from '@/lib/call-root'
import { CallTool, type ProviderMark } from './call-tool'
import { invocationOutcomeLabels, resultUnknown, taskOutcomeLabel } from './labels'
import type { ActivityRow, RunState } from './types'

export function rowLabel(row: ActivityRow): string {
  switch (row.kind) {
    case 'task':
      return row.intent
    case 'tool':
      return row.intent
    case 'run':
      return row.submission?.intent || row.intent || row.actor || 'exec'
    case 'invocation':
      return row.callPath || row.tool
    case 'output':
      return row.label
  }
}
/** A record's name. A call names its tool as every list of calls does. */
export function RecordLabel({ row, wrap }: { row: ActivityRow; wrap?: boolean }) {
  return row.kind === 'invocation' ? (
    <CallTool callPath={rowLabel(row)} wrap={wrap} />
  ) : (
    rowLabel(row)
  )
}
const toolIcons = new Map<string, IconName>([
  ['search', 'Search'],
  ['describe', 'BookOpen'],
  ['exec', 'Play'],
  ['save_function', 'Save'],
  ['delete_function', 'Trash'],
])
/** A record's kind icon. A call whose provider the workspace still has shows that provider's
 * logo in its name instead. */
export function recordIcon(row: ActivityRow, providers: readonly ProviderMark[]) {
  return row.kind === 'invocation' && toolOwner(row.callPath, providers) ? undefined : rowIcon(row)
}
export function rowIcon(row: ActivityRow): { name: IconName; label: string } {
  switch (row.kind) {
    case 'task':
      return { name: 'Flag', label: 'task' }
    case 'tool':
      return {
        name: toolIcons.get(row.tool) ?? 'CircleQuestionMark',
        label: row.tool,
      }
    case 'run':
      return { name: 'Play', label: 'exec' }
    case 'invocation':
      return { name: 'Zap', label: 'tool' }
    case 'output':
      return row.eventKind === 'program.logged'
        ? { name: 'Terminal', label: 'log' }
        : { name: 'Radio', label: 'output' }
  }
}
export type RowStatus = NonNullable<Table.RowProps['status']>
const runTones = {
  running: 'neutral',
  succeeded: 'success',
  failed: 'error',
  timed_out: 'error',
  canceled: 'neutral',
  interrupted: 'warning',
  unknown: 'warning',
} satisfies Record<RunState, RowStatus['tone']>
export function rowStatus(row: ActivityRow): RowStatus | undefined {
  switch (row.kind) {
    case 'task':
      return {
        tone:
          row.outcome === 'success' ? 'success' : row.outcome === 'failure' ? 'error' : 'neutral',
        label: taskOutcomeLabel(row.outcome),
      }
    case 'tool':
      if (row.outcome === 'unknown') return { tone: 'warning', label: resultUnknown }
      if (row.acceptedRun) return { tone: 'neutral', label: 'accepted · run unavailable' }
      return {
        tone:
          row.outcome === 'succeeded'
            ? 'success'
            : row.outcome === 'rejected'
              ? 'error'
              : 'neutral',
        label: row.outcome,
      }
    case 'run':
      return {
        tone: runTones[row.state],
        label: row.state.replaceAll('_', ' '),
      }
    case 'invocation':
      if (row.state !== 'ended')
        return {
          tone: row.requiresAction || row.state === 'unknown' ? 'warning' : 'neutral',
          label: row.state,
        }
      // A complete response stays neutral whether the Program received it or was rejected with it
      // as a Tool Error: the row holds no response to tell, so it says only that one came.
      return {
        tone:
          row.outcome === 'coral_failure'
            ? 'error'
            : row.outcome === 'indeterminate' || row.outcome === 'unknown'
              ? 'warning'
              : 'neutral',
        label: invocationOutcomeLabels[row.outcome],
      }
    case 'output':
      // Its icon and label name it; a state would say nothing more.
      return undefined
  }
}
