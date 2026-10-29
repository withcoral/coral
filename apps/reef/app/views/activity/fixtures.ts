// Activity data for unit tests: one agent Run in the Finance workspace that
// waits on one approval of `mail.send`.

import type { ActivityPage, ActivityRow, RequestDetail, RunDetail } from './types'

export const workspace = 'workspaces/finance'

/** A running agent Run that waits on one approval. */
export const run: Extract<ActivityRow, { kind: 'run' }> = {
  kind: 'run',
  name: `${workspace}/runs/one`,
  workspace,
  agent: `${workspace}/agents/one`,
  actor: 'opaque-principal',
  actorLabel: 'Collections agent',
  workspaceLabel: 'Finance',
  intent: 'Send invoice reminders',
  task: '',
  state: 'running',
  startedAt: '2026-09-22T09:00:00Z',
  endedAt: null,
  durationNanos: '1',
  requiresAction: true,
  actionPage: {
    total: '1',
    next: '',
    actions: [
      {
        kind: 'approval',
        name: `${workspace}/approvals/one`,
        callPath: 'mail.send',
        providerLabel: 'Mail',
        deadline: '2030-09-22T09:15:00Z',
      },
    ],
  },
}

/** The pending action `run` waits on, as the pending list and the Run row carry it. */
export const approval = run.actionPage!.actions[0]

/** The exact approval request behind `approval`, as its review reads it. */
export const approvalRequest: Extract<RequestDetail, { kind: 'approval' }> = {
  kind: 'approval',
  name: approval.name,
  run: run.name,
  invocation: `${workspace}/invocations/one`,
  provider: `${workspace}/providers/mail`,
  callPath: approval.callPath,
  arguments: '{"to":"exact-recipient"}',
  state: 'pending',
  actionable: true,
  deadline: approval.deadline,
  approver: '',
  decidedAt: null,
  comment: null,
}

/** The Run that `approvalRequest` belongs to, as its review reads it. */
export const approvalContext: RunDetail = {
  kind: 'run',
  run,
  source: 'return await coral.providers.mail.send({ to: "exact-recipient" })',
  linkedFunctions: [],
  terminal: { kind: 'unavailable' },
}

/** An Activity page with no records and nothing selected, with `overrides` applied. */
export function activityPage(overrides: Partial<ActivityPage> = {}): ActivityPage {
  return {
    rows: [],
    branches: {},
    expanded: [],
    hasBefore: false,
    hasAfter: false,
    anchorUnavailable: false,
    listUnavailable: false,
    agent: null,
    detail: null,
    unavailable: false,
    selected: null,
    selectedRow: null,
    listPath: '/workspaces/finance/activity',
    ...overrides,
  }
}

/** Your personal history, with `approval` pending above it. */
export const pendingPage = activityPage({
  rows: [
    {
      ...run,
      name: `${workspace}/runs/personal`,
      actor: 'you',
      agent: '',
      requiresAction: false,
      actionPage: undefined,
    },
  ],
  pending: {
    actions: [{ action: approval, run }],
    actionCount: '1',
    next: '',
    unavailable: false,
  },
})

const at = '2026-09-18T14:02:00.000Z'

/** An open Task in `workspace`, labelled by its intent. */
export function taskRow(id: string, intent = id): Extract<ActivityRow, { kind: 'task' }> {
  return {
    kind: 'task',
    name: `${workspace}/tasks/${id}`,
    workspace,
    agent: '',
    actor: 'you',
    intent,
    outcome: 'open',
    startedAt: at,
    endedAt: null,
  }
}

/** A finished personal Run in `parent`, labelled by its actor. */
export function runRow(
  id: string,
  parent: string,
  overrides: Partial<Extract<ActivityRow, { kind: 'run' }>> = {},
): Extract<ActivityRow, { kind: 'run' }> {
  return {
    kind: 'run',
    name: `${workspace}/runs/${id}`,
    workspace,
    agent: '',
    actor: id,
    task: parent,
    state: 'succeeded',
    startedAt: at,
    endedAt: at,
    durationNanos: '1',
    requiresAction: false,
    ...overrides,
  }
}

/** An ended tool call that Run `owner` made, labelled by its call path. */
export function invocationRow(
  owner: string,
  id: string,
  overrides: Partial<Extract<ActivityRow, { kind: 'invocation' }>> = {},
): Extract<ActivityRow, { kind: 'invocation' }> {
  return {
    kind: 'invocation',
    name: `${workspace}/invocations/${id}`,
    workspace,
    agent: '',
    run: owner,
    task: '',
    actor: 'you',
    state: 'ended',
    startedAt: at,
    endedAt: at,
    durationNanos: '1',
    requiresAction: false,
    provider: `${workspace}/providers/mail`,
    tool: 'send',
    callPath: 'mail.send',
    outcome: 'upstream_outcome',
    dispatchedAt: at,
    waitingNanos: '0',
    ...overrides,
  }
}

/** A tool call in Task `parent`, labelled by its intent. */
export function toolRow(
  parent: string,
  id: string,
  overrides: Partial<Extract<ActivityRow, { kind: 'tool' }>> = {},
): Extract<ActivityRow, { kind: 'tool' }> {
  return {
    kind: 'tool',
    name: `${parent}/interactions/${id}`,
    workspace,
    agent: '',
    task: parent,
    tool: 'search',
    intent: id,
    outcome: 'succeeded',
    startedAt: at,
    completedAt: at,
    acceptedRun: '',
    ...overrides,
  }
}
