// Activity data for unit tests: one agent Run in the Finance workspace that
// waits on one approval of `mail.send`.

import type { ActivityPage, ActivityRow, RequestDetail } from './types'

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
