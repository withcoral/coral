// Traces for unit tests and stories: a root with every child already read.

import { absorbPage, openTrace, type ActivityTraceStore } from '@/lib/activity-trace'
import { activityPage, workspace } from './fixtures'
import type { ActivityPage, ActivityRow } from './types'

type Row<Kind extends ActivityRow['kind']> = Extract<ActivityRow, { kind: Kind }>

/** The instant `ms` milliseconds into the fixture's first second. */
export const T0 = Date.UTC(2026, 8, 18, 14, 0, 0)
export const instant = (ms: number) => new Date(T0 + ms).toISOString()

export function traceTask(overrides: Partial<Row<'task'>> = {}): Row<'task'> {
  return {
    kind: 'task',
    name: `${workspace}/tasks/invoices`,
    workspace,
    agent: '',
    actor: 'you',
    intent: 'Find overdue invoices',
    outcome: 'success',
    startedAt: instant(0),
    endedAt: instant(60_000),
    ...overrides,
  }
}

/** A Run of `task` from `start` to `end` milliseconds. A null end is a Run that runs. */
export function traceRun(
  id: string,
  start: number,
  end: number | null,
  overrides: Partial<Row<'run'>> = {},
): Row<'run'> {
  return {
    kind: 'run',
    name: `${workspace}/runs/${id}`,
    workspace,
    agent: '',
    actor: 'you',
    task: `${workspace}/tasks/invoices`,
    intent: `Run ${id}`,
    state: end === null ? 'running' : 'succeeded',
    startedAt: instant(start),
    endedAt: end === null ? null : instant(end),
    durationNanos: String(((end ?? start) - start) * 1_000_000),
    requiresAction: false,
    ...overrides,
  }
}

/**
 * A tool call of `run`, admitted at `start`, sent at `sent` and ended at `end`
 * milliseconds. A null `sent` was never sent; a null `end` has not ended.
 */
export function traceCall(
  run: Row<'run'>,
  id: string,
  [start, sent, end]: [number, number | null, number | null],
  overrides: Partial<Row<'invocation'>> = {},
): Row<'invocation'> {
  return {
    kind: 'invocation',
    name: `${workspace}/invocations/${id}`,
    workspace,
    agent: run.agent,
    actor: run.actor,
    task: run.task,
    run: run.name,
    state: end === null ? (sent === null ? 'pending' : 'dispatching') : 'ended',
    outcome: end === null ? 'unknown' : 'upstream_outcome',
    startedAt: instant(start),
    dispatchedAt: sent === null ? null : instant(sent),
    endedAt: end === null ? null : instant(end),
    durationNanos: String(((end ?? start) - start) * 1_000_000),
    waitingNanos: String(((sent ?? end ?? start) - start) * 1_000_000),
    requiresAction: false,
    toolKind: 'provider',
    provider: `${workspace}/providers/linear`,
    tool: 'listIssues',
    callPath: 'coral.providers.linear.listIssues',
    ...overrides,
  }
}

/** An Event of `run` at `ms`: a log, an emission or a lifecycle Event. */
export function traceEvent(
  run: Row<'run'>,
  sequence: number,
  ms: number,
  eventKind = 'program.logged',
  contextImpactBytes?: string,
): Row<'output'> {
  return {
    kind: 'output',
    name: `${run.name}/events/${sequence}`,
    workspace,
    agent: run.agent,
    task: run.task,
    run: run.name,
    label:
      eventKind === 'program.logged'
        ? `log #${sequence}`
        : eventKind === 'program.emitted'
          ? `emission #${sequence}`
          : eventKind,
    startedAt: instant(ms),
    eventKind,
    contextImpactBytes,
  }
}

/** A Task tool call that is no Run, such as a search. A null end has no recorded completion. */
export function traceTool(
  id: string,
  tool: string,
  start: number,
  end: number | null,
  overrides: Partial<Row<'tool'>> = {},
): Row<'tool'> {
  return {
    kind: 'tool',
    name: `${workspace}/tasks/invoices/interactions/${id}`,
    workspace,
    agent: '',
    task: `${workspace}/tasks/invoices`,
    tool,
    intent: `${tool} ${id}`,
    outcome: end === null ? 'unknown' : 'succeeded',
    startedAt: instant(start),
    completedAt: end === null ? null : instant(end),
    acceptedRun: '',
    ...overrides,
  }
}

/** The windows of a root with all of `children` in them, as one page holds them. */
export function tracePage(
  root: ActivityRow,
  children: Record<string, ActivityRow[]>,
  overrides: Partial<ActivityPage> = {},
): ActivityPage {
  return activityPage({
    rows: [root],
    expanded: Object.keys(children),
    branches: Object.fromEntries(
      Object.entries(children).map(([parent, rows]) => [
        parent,
        {
          offset: 0,
          total: String(rows.length),
          rows: rows.map((row, position) => ({ ...row, position })),
          next: '',
          unavailable: false,
        },
      ]),
    ),
    ...overrides,
  })
}

/** A trace that has read all of `children`. */
export function traceStore(
  root: ActivityRow,
  children: Record<string, ActivityRow[]>,
): ActivityTraceStore {
  return absorbPage(openTrace(root.name), tracePage(root, children))
}
