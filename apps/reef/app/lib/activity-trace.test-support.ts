// A fake of Coral as the Activity loader reads it for a trace, for unit tests.

import { activityPage, workspace } from '@/views/activity/fixtures'
import {
  MAX_EXPANDED_RECORDS,
  type ActivityBranch,
  type ActivityPage,
  type ActivityRow,
} from '@/views/activity/types'
import { ACTIVITY_CHILD_WINDOW } from './activity-live-types'

export type Row<Kind extends ActivityRow['kind']> = Extract<ActivityRow, { kind: Kind }>
export const at = (second: number) => new Date(Date.UTC(2026, 8, 18, 14, 0, second)).toISOString()

export const task: Row<'task'> = {
  kind: 'task',
  name: `${workspace}/tasks/invoices`,
  workspace,
  agent: '',
  actor: 'you',
  intent: 'Find invoices',
  outcome: 'success',
  startedAt: at(0),
  endedAt: at(50),
}
export const runRow = (id: string, state: Row<'run'>['state'] = 'succeeded'): Row<'run'> => ({
  kind: 'run',
  name: `${workspace}/runs/${id}`,
  workspace,
  agent: '',
  actor: 'you',
  task: task.name,
  state,
  startedAt: at(1),
  endedAt: state === 'running' ? null : at(9),
  durationNanos: '8000000000',
  requiresAction: false,
})
export const call = (run: Row<'run'>, index: number, ended = true): Row<'invocation'> => ({
  ...run,
  kind: 'invocation',
  name: `${workspace}/invocations/${run.name.split('/').at(-1)}-${index}`,
  run: run.name,
  state: ended ? 'ended' : 'pending',
  outcome: ended ? 'upstream_outcome' : 'unknown',
  toolKind: 'provider',
  dispatchedAt: at(2),
  waitingNanos: '0',
  provider: `${workspace}/providers/linear`,
  tool: 'listIssues',
  callPath: 'coral.providers.linear.listIssues',
})
/** A call of a private tool, whose body makes calls. `inside` is the call that made it. */
export const toolCall = (
  run: Row<'run'>,
  id: string,
  inside?: Row<'invocation'>,
  ended = true,
): Row<'invocation'> => ({
  ...call(run, 0, ended),
  name: `${workspace}/invocations/${id}`,
  toolKind: 'private',
  provider: '',
  tool: id,
  callPath: `coral.private.${id}`,
  parent: inside && {
    invocation: inside.name,
    callPath: inside.callPath,
    toolKind: inside.toolKind,
  },
})
export const save: Row<'tool'> = {
  kind: 'tool',
  name: `${task.name}/interactions/save`,
  workspace,
  agent: '',
  task: task.name,
  tool: 'save_private_tool',
  intent: 'Save the report',
  outcome: 'succeeded',
  startedAt: at(20),
  completedAt: at(21),
  acceptedRun: '',
}

/**
 * Coral as the Activity loader reads it for a trace: bounded windows by ordinal.
 */
export function coral(
  children: Record<string, ActivityRow[] | 'unavailable'>,
  root: ActivityRow | ActivityRow[] = task,
  /** The feedback of a Task at the root, as its own detail states it. */
  feedback?: string,
) {
  const calls: URLSearchParams[] = []
  const window = (parent: string, offset: number): ActivityBranch | undefined => {
    const rows = children[parent]
    if (!rows) return undefined
    if (rows === 'unavailable') return { rows: [], next: '', unavailable: true }
    // An offset at the end reads the last window.
    const from = offset >= rows.length ? Math.max(0, rows.length - ACTIVITY_CHILD_WINDOW) : offset
    return {
      offset: from,
      total: String(rows.length),
      rows: rows
        .slice(from, from + ACTIVITY_CHILD_WINDOW)
        .map((row, index) => ({ ...row, position: from + index })),
      next: '',
      unavailable: false,
    }
  }
  const load = (query: URLSearchParams): ActivityPage => {
    calls.push(query)
    // As the loader does, it refuses a request that expands more records than it allows.
    if (query.getAll('expand').length > MAX_EXPANDED_RECORDS)
      throw new Error('Expand at most 20 records at a time')
    const branches: ActivityPage['branches'] = {}
    const shown = () => Object.values(branches).flatMap((branch) => branch.rows)
    // As the loader does, a tool call opens only once a window shows the call itself.
    for (const parent of query.getAll('expand')) {
      if (parent.includes('/invocations/') && !shown().some((row) => row.name === parent)) continue
      const branch = window(parent, Number(query.get(`offset.${parent}`) ?? 0))
      if (branch) branches[parent] = branch
    }
    // As the loader does, the record the query selects comes with its detail.
    const selected = [root].flat().find((row) => row.name === query.get('record'))
    return activityPage({
      rows: [root].flat(),
      branches,
      expanded: query.getAll('expand'),
      ...(selected?.kind === 'task' && {
        selected: selected.name,
        selectedRow: selected,
        detail: { kind: 'task', task: selected, feedback },
      }),
    })
  }
  return { load, calls, children }
}
