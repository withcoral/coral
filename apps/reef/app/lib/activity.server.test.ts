import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { createElement } from 'react'
import { renderToString } from 'react-dom/server'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { Activity } from '@/views/activity/activity'
import { activityNavigation } from '@/views/activity/navigation'
import { stored } from '@/views/activity/discovery/fixtures'
import type { ActivityPage } from '@/views/activity/types'
import {
  ActivityRunState,
  ActivityInvocationState,
  ActivityInvocationOutcome,
} from '@/generated/coral/management/v1/activity_pb'
import { AccountConnectionState } from '@/generated/coral/management/v1/account_pb'
import { fakeManagement, type ManagementHandlers } from './management.test-support'
import { ACTIVITY_CHILD_WINDOW } from './activity-live-types'
import { loadActivity, loadRunActivity } from './activity.server'

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const scope = { workspace }
const task = `${workspace}/tasks/00000000-0000-4000-8000-000000000002`
const interaction = `${task}/interactions/00000000-0000-4000-8000-000000000003`
const run = `${workspace}/runs/00000000-0000-4000-8000-000000000004`
const invocation = `${workspace}/invocations/00000000-0000-4000-8000-000000000005`
const approval = `${workspace}/approvals/00000000-0000-4000-8000-000000000006`
const agent = `${workspace}/agents/00000000-0000-4000-8000-000000000007`
const connection = `${workspace}/accountConnections/00000000-0000-4000-8000-000000000008`
const goal = {
  name: task,
  workspace,
  intent: 'Find invoices',
  outcome: 'open',
  actor: 'user',
  startedAtUnixNanos: 1_000_000_000n,
}
/** Renders Activity inside a data router, as the app does. Row decisions need one. */
function renderActivity(location: string, page: ActivityPage) {
  const router = createMemoryRouter([{ path: '*', element: createElement(Activity, { page }) }], {
    initialEntries: [location],
  })
  const html = renderToString(createElement(RouterProvider, { router }))
  router.dispose()
  return html
}
const execution = {
  name: run,
  workspace,
  task,
  actor: 'user',
  state: ActivityRunState.RUNNING,
  intent: 'Send reminders',
  actorLabel: 'Collections agent',
  workspaceLabel: 'Finance',
  actionPage: {
    total: 1n,
    nextAction: '',
    actions: [
      {
        request: { request: { case: 'approval' as const, value: approval } },
        callPath: 'mail.send',
        providerLabel: 'Mail',
        deadlineAtUnixNanos: 902_000_000_000n,
      },
    ],
  },
  startedAtUnixNanos: 2_000_000_000n,
  requiresAction: true,
}
const operation = {
  name: invocation,
  workspace,
  run,
  task,
  operation: 'send',
  callPath: 'mail.send',
  state: ActivityInvocationState.PENDING,
  requiresAction: true,
}
// Each builder serves one Coral read. A test composes the reads its load makes;
// any other read fails as unimplemented.
type Handlers = ManagementHandlers
type AgentOption = { agentRun?: boolean }
const agentOf = ({ agentRun }: AgentOption) => (agentRun ? agent : '')
const refuse = (message: string, code = Code.NotFound) => {
  throw new ConnectError(message, code)
}

// Short names for the reads, as tests refer to them.
const READS: Record<string, string> = {
  browseRequiredActions: 'actions',
  browseActivityRoots: 'roots',
  inspectTask: 'task',
  inspectTaskInteraction: 'tool',
  browseRunEntries: 'entries',
  inspectRun: 'run',
  inspectRunEvent: 'event',
  inspectInvocation: 'operation',
  inspectApproval: 'approval',
  getAccountConnectionRequest: 'connection',
}

/** An in-memory Coral serving `parts`; `calls` records each read in order. */
function coral(...parts: Handlers[]) {
  type Method = (input: unknown, context: unknown) => unknown
  const calls: { method: string; input: unknown }[] = []
  const handlers: Record<string, Record<string, Method>> = {}
  for (const part of parts)
    for (const [client, methods] of Object.entries(part))
      for (const [name, method] of Object.entries(methods as Record<string, Method>))
        (handlers[client] ??= {})[name] = (input, context) => {
          calls.push({ method: READS[name] ?? name, input })
          return method(input, context)
        }
  return { calls, api: fakeManagement(handlers as Handlers) }
}

/** The reads every list load makes: pending actions and roots. */
function list(
  options: AgentOption & {
    resolved?: boolean
    personalAction?: boolean
    listConcealed?: boolean
    neighbors?: boolean
    contextImpact?: boolean
    partialContextImpact?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      browseRequiredActions: () => ({
        actions: options.resolved
          ? []
          : [
              {
                action: execution.actionPage.actions[0],
                run: {
                  ...execution,
                  actionPage: undefined,
                  agent: options.personalAction ? '' : agent,
                },
              },
            ],
        actionCount: options.resolved ? 0n : 1n,
      }),
      browseActivityRoots: (input) => {
        if (options.listConcealed && input.scope?.agent) refuse('history unavailable')
        if (options.agentRun && input.scope?.agent !== agent) return { roots: [] }
        const goals = [goal, ...(options.neighbors ? [{ ...goal, name: `${task}-neighbor` }] : [])]
        return {
          roots: goals.map((value) => ({
            record: {
              case: 'task' as const,
              value: {
                ...value,
                agent: agentOf(options),
                contextImpactBytes: options.contextImpact ? -141800n : undefined,
                contextImpactPartial: options.partialContextImpact ?? false,
              },
            },
          })),
        }
      },
    },
  }
}

function lost() {
  return refuse('history unavailable')
}

/** Pending actions and every exact read report lost authority. */
function lostAuthority(): Handlers {
  return {
    accounts: { getAccountConnectionRequest: lost },
    activity: {
      browseRequiredActions: lost,
      inspectTask: lost,
      inspectTaskInteraction: lost,
      browseRunEntries: lost,
      inspectRun: lost,
      inspectRunEvent: lost,
      inspectInvocation: lost,
      inspectApproval: lost,
    },
  }
}

function taskDetail(
  options: AgentOption & {
    feedback?: string
    taskUnavailable?: boolean
    contextImpact?: boolean
    outOfWindow?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      inspectTask: (input) => {
        if (options.taskUnavailable && input.name === task) refuse('task unavailable')
        if (options.agentRun && input.scope && input.scope.agent !== agent)
          refuse('outside Agent scope')
        return {
          feedback: options.feedback,
          task: {
            ...goal,
            name: input.name,
            agent: agentOf(options),
            contextImpactBytes: options.contextImpact ? -141800n : undefined,
          },
          offset: options.outOfWindow ? 100n : 0n,
          total: options.outOfWindow ? 300n : 1n,
          interactions: options.outOfWindow
            ? [
                {
                  name: `${input.name}/interactions/visible`,
                  kind: 'search',
                  intent: 'Visible search',
                },
              ]
            : [
                {
                  name: interaction,
                  kind: 'submit',
                  intent: 'Send reminders',
                  outcome: 'succeeded',
                  run: {
                    ...execution,
                    agent: agentOf(options),
                    contextImpactBytes: options.contextImpact ? -148000n : undefined,
                  },
                  acceptedRun: run,
                  agent: agentOf(options),
                },
              ],
          nextAfter: 'next-interaction',
        }
      },
    },
  }
}

function interactionDetail(
  options: AgentOption & { response?: string; describe?: string; types?: string[] } = {},
): Handlers {
  return {
    activity: {
      inspectTaskInteraction: () => ({
        interaction: {
          name: interaction,
          agent: agentOf(options),
          kind: options.describe ? 'describe' : 'search',
          intent: 'Find operations',
          outcome: 'succeeded',
        },
        evidence: {
          case: 'discovery',
          value: {
            context: options.describe
              ? {
                  case: 'describe',
                  value: { callPath: options.describe, types: options.types ?? [] },
                }
              : { case: 'search', value: { query: 'invoices', limit: 10 } },
            responseJson: options.response ?? '{"results":[]}',
          },
        },
      }),
    },
  }
}

const githubProvider = '00000000-0000-4000-8000-000000000009'
/** Every Operation that the stored discovery fixtures name. */
const fixtureOperations = [
  'pulls.checkIfMerged',
  'pulls.deleteReviewComment',
  'pulls.merge',
  'pulls.getMergeAsyncResult',
  'pulls.updateBranch',
  'issues.create',
]
/** The current catalog that discovery links resolve against. */
function catalog(
  options: {
    namespaces?: string[]
    operations?: string[]
    functions?: string[]
    concealed?: boolean
  } = {},
): Handlers {
  return {
    providers: {
      listProviders: () =>
        options.concealed
          ? refuse('providers', Code.PermissionDenied)
          : {
              providers: (options.namespaces ?? []).map((namespace) => ({
                name: `${workspace}/providers/${githubProvider}`,
                namespace,
              })),
            },
      listOperations: () => ({
        operations: (options.operations ?? fixtureOperations).map((relativePath) => ({
          relativePath,
        })),
      }),
    },
    savedFunctions: {
      listSavedFunctions: () => ({
        savedFunctions: (options.functions ?? []).map((path) => ({ path })),
      }),
    },
  }
}

function runEntries(
  options: AgentOption & {
    runUnavailable?: Code
    runAssociation?:
      | 'missing-run'
      | 'missing-task'
      | 'other-task'
      | 'missing-submission'
      | 'missing-position'
    submissionPosition?: bigint
    endedEvent?: boolean
    contextImpact?: boolean
    completedEntries?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      browseRunEntries: (input) => {
        if (options.runUnavailable) refuse('run unavailable', options.runUnavailable)
        if (options.agentRun && input.scope && !input.scope.agent) refuse('outside personal scope')
        return {
          run:
            options.runAssociation === 'missing-run'
              ? undefined
              : {
                  ...execution,
                  agent: agentOf(options),
                  task:
                    options.runAssociation === 'missing-task'
                      ? ''
                      : options.runAssociation === 'other-task'
                        ? `${task}-other`
                        : task,
                },
          submission:
            options.runAssociation === 'missing-submission'
              ? undefined
              : {
                  name: interaction,
                  kind: 'submit',
                  intent: 'Send invoice reminders',
                  outcome: 'succeeded',
                  acceptedRun: run,
                  position:
                    options.runAssociation === 'missing-position'
                      ? undefined
                      : (options.submissionPosition ?? 0n),
                },
          entries: options.endedEvent
            ? [
                {
                  record: {
                    case: 'event',
                    value: {
                      sequence: 4n,
                      kind: 'run.ended',
                      contextImpactBytes: 4n,
                      occurredAtUnixNanos: 3_000_000_000n,
                    },
                  },
                },
              ]
            : [
                {
                  record: {
                    case: 'invocation',
                    value: {
                      ...operation,
                      contextImpactBytes: options.contextImpact ? -150000n : undefined,
                    },
                  },
                },
                {
                  record: {
                    case: 'event',
                    value: {
                      sequence: 3n,
                      kind: options.contextImpact ? 'run.ended' : 'program.emitted',
                      contextImpactBytes: options.contextImpact ? 2000n : undefined,
                      occurredAtUnixNanos: 3_000_000_000n,
                    },
                  },
                },
              ],
          nextAfter: options.endedEvent ? 4n : 3n,
          hasMore: !options.endedEvent && !options.completedEntries,
          hasFinalValue: options.endedEvent || (options.completedEntries ?? false),
        }
      },
    },
  }
}

function runDetail(
  options: AgentOption & {
    runDetailUnavailable?: boolean
    resolved?: boolean
    retainedConnection?: boolean
    noFinalValue?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      inspectRun: (input) => {
        if (options.runDetailUnavailable) refuse('run unavailable')
        return {
          run: {
            ...execution,
            agent: agentOf(options),
            actionPage: {
              actions: options.resolved || input.actionAfter ? [] : execution.actionPage.actions,
              total: options.resolved ? 0n : 1n,
              nextAction: options.resolved || input.actionAfter ? '' : 'action-cursor',
            },
          },
          accountConnection:
            options.retainedConnection && input.accountConnection === connection ? connection : '',
          submittedSource: 'return secretSource;',
          linkedFunctions: [
            {
              savedFunction: 'saved-before-deletion',
              path: 'old',
              source: 'export default () => 41',
            },
          ],
          terminal: {
            case: 'succeeded',
            value: { finalValueJson: options.noFinalValue ? undefined : 'null' },
          },
          events: [
            {
              sequence: 3n,
              payload: { case: 'programEmitted', value: { valueJson: '"unselected-event"' } },
            },
          ],
        }
      },
    },
  }
}

function runEvent(options: AgentOption & { endedEvent?: boolean } = {}): Handlers {
  return {
    activity: {
      inspectRunEvent: (input) => ({
        contextImpactBytes: options.endedEvent ? 4n : undefined,
        run: { ...execution, agent: agentOf(options) },
        event: {
          sequence: input.sequence,
          occurredAtUnixNanos: 3_000_000_000n,
          payload: options.endedEvent
            ? { case: 'ended', value: { state: ActivityRunState.SUCCEEDED } }
            : { case: 'programEmitted', value: { valueJson: '{"invoices":24}' } },
        },
      }),
    },
  }
}

function invocationDetail(options: AgentOption & { resolved?: boolean } = {}): Handlers {
  return {
    activity: {
      inspectInvocation: () => ({
        invocation: {
          ...operation,
          agent: agentOf(options),
          state: options.resolved ? ActivityInvocationState.ENDED : ActivityInvocationState.PENDING,
          outcome: options.resolved ? ActivityInvocationOutcome.UPSTREAM_OUTCOME : undefined,
        },
        argumentsJson: '{"to":"private@example.com"}',
        outcome: options.resolved
          ? { case: 'upstreamOutcomeJson', value: '{"data":null,"metadata":{"status":404}}' }
          : undefined,
        requests: [{ request: { case: 'approval', value: approval } }],
      }),
    },
  }
}

function approvalDetail(
  options: { hiddenRequest?: boolean; resolved?: boolean; wrongApprovalInvocation?: boolean } = {},
): Handlers {
  return {
    activity: {
      inspectApproval: () => {
        if (options.hiddenRequest) refuse('unavailable')
        return {
          name: approval,
          run,
          invocation: options.wrongApprovalInvocation ? `${invocation}-other` : invocation,
          state: options.resolved ? 'approved' : 'pending',
          actionable: !options.resolved,
          argumentsJson: '{"to":"private@example.com"}',
          approver: options.resolved ? 'reviewer' : '',
        }
      },
    },
  }
}

function connectionRequest(options: { hiddenRequest?: boolean } = {}): Handlers {
  return {
    accounts: {
      getAccountConnectionRequest: () => {
        if (options.hiddenRequest) refuse('unavailable')
        return {
          name: connection,
          provider: `${workspace}/providers/mail`,
          state: AccountConnectionState.CONNECTED,
        }
      },
    },
  }
}

/** The reads that open an operation for review inside its Task and Run. */
function reviewReads(
  options: Parameters<typeof runDetail>[0] & Parameters<typeof approvalDetail>[0] = {},
) {
  return [
    taskDetail(options),
    runEntries(options),
    runDetail(options),
    invocationDetail(options),
    approvalDetail(options),
  ]
}

/** Methods that read exact detail or a child window, as opposed to the list summary. */
const DETAIL_OR_CHILD = [
  'task',
  'tool',
  'entries',
  'run',
  'event',
  'operation',
  'approval',
  'connection',
]
function searchHits(page: ActivityPage) {
  const view =
    page.detail?.kind === 'tool' && page.detail.evidence.kind === 'discovery'
      ? page.detail.evidence.view
      : undefined
  if (view?.kind !== 'search') throw new Error('expected a search page in the detail')
  return view.page.hits
}
function loaded(calls: { method: string }[], ...methods: string[]) {
  return calls.filter((call) => methods.includes(call.method)).length
}
function request(values: Record<string, string | string[] | undefined> = {}) {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(values))
    for (const item of Array.isArray(value) ? value : value ? [value] : []) query.append(key, item)
  return new Request(`http://reef/${workspace}/activity?${query}`)
}

/** The drawer's reveal link, from the navigation interface the view uses. */
function revealLink(page: ActivityPage, location: string): URL {
  const { pathname, search } = new URL(location, 'http://reef')
  const href = activityNavigation(page, pathname, search).revealHref(page.selectedRow!)
  return new URL(href, 'http://reef')
}

describe('hierarchical activity through the private server interface', () => {
  it('selects and loads the exact operation approval once, but never chooses another action during observation', async () => {
    const { api, calls } = coral(list(), ...reviewReads())
    const input = request({ record: invocation })
    const initial = await loadActivity(input, api, { scope })
    expect(initial.initialRequest).toBe(approval)
    expect(initial.detail).toMatchObject({ request: { name: approval } })
    expect(calls.filter(({ method }) => method === 'run')).toHaveLength(1)
    expect(calls.filter(({ method }) => method === 'approval')).toHaveLength(1)
    const update = await loadActivity(input, api, {
      scope,
      reading: { roots: [], followRoot: true, branches: {} },
    })
    expect(update.initialRequest).toBeUndefined()
    expect(update.detail).toMatchObject({ kind: 'invocation', request: null })
    expect(calls.filter(({ method }) => method === 'approval')).toHaveLength(1)
  })
  it.each([run, `${run}/final`])(
    'does not turn %s inspection into an automatic approval selection',
    async (record) => {
      const { api, calls } = coral(list(), taskDetail(), runDetail())
      const value = await loadActivity(request({ record }), api, { scope })
      expect(value.initialRequest).toBeUndefined()
      expect(calls.some(({ method }) => method === 'approval')).toBe(false)
    },
  )
  it('does not show a sibling operation approval as the selected operation', async () => {
    const { api } = coral(
      list(),
      taskDetail(),
      runEntries(),
      invocationDetail(),
      approvalDetail({ wrongApprovalInvocation: true }),
    )
    const page = await loadActivity(request({ record: invocation, request: approval }), api, {
      scope,
    })
    expect(page.selected).toBe(invocation)
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      request: null,
      requestUnavailable: true,
    })
  })
  it('conceals an operation approval whose Run is unavailable instead of reviewing it without context', async () => {
    const { api } = coral(list(), ...reviewReads({ runDetailUnavailable: true }))
    const page = await loadActivity(request({ record: invocation, request: approval }), api, {
      scope,
    })
    expect(page.selected).toBe(invocation)
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      request: null,
      requestUnavailable: true,
    })
    expect(page.detail).not.toHaveProperty('context')
  })
  it('respects an explicitly empty request selection', async () => {
    const { api } = coral(list(), taskDetail(), runDetail())
    const input = new URL(request({ record: run }).url)
    input.searchParams.set('request', '')
    const value = await loadActivity(new Request(input), api, { scope })
    expect(value.initialRequest).toBeUndefined()
    expect(value.detail).toMatchObject({ kind: 'run', request: null })
  })
  it.each([undefined, `${task}-neighbor`])(
    'opens pending Review in its real Task and Run hierarchy from a list at anchor %s',
    async (anchor) => {
      const { api } = coral(list({ personalAction: true, neighbors: true }), ...reviewReads())
      const input = request({ tree: '1', anchor })
      const initial = await loadActivity(input, api, { scope })
      const location = new URL(input.url)
      // The pending list's Review link.
      const [pending] = initial.pending!.actions
      const href = activityNavigation(initial, location.pathname, location.search).revealHref(
        pending.run,
        { request: pending.action.name },
      )
      const selected = await loadActivity(new Request(new URL(href, 'http://reef')), api, { scope })
      expect(selected.selected).toBe(invocation)
      expect(selected.selectedRow).toMatchObject({ kind: 'invocation', name: invocation })
      expect(selected.detail).toMatchObject({ kind: 'invocation', request: { name: approval } })
      expect(selected.branches[task]?.rows ?? []).toContainEqual(
        expect.objectContaining({ kind: 'run', name: run }),
      )
      expect(selected.branches[run]?.rows ?? []).toContainEqual(
        expect.objectContaining({ kind: 'invocation', name: invocation }),
      )
      const rendered = renderActivity(href, selected)
      expect(rendered).not.toContain('Outside this workspace or page:')
    },
  )
  it('reads the original resolved account request only after verifying its retained Run association', async () => {
    const { api, calls } = coral(
      list({ resolved: true }),
      taskDetail(),
      runDetail({ resolved: true, retainedConnection: true }),
      connectionRequest(),
    )
    const page = await loadActivity(request({ record: run, request: connection }), api, { scope })
    expect(page.pending?.actions).toEqual([])
    expect(page.detail).toMatchObject({
      kind: 'run',
      request: { kind: 'account_connection', name: connection, state: 'connected' },
    })
    expect(calls.find((call) => call.method === 'run')?.input).toMatchObject({
      name: run,
      accountConnection: connection,
    })
    expect(calls.find((call) => call.method === 'connection')?.input).toMatchObject({
      name: connection,
    })
  })
  it('does not inspect an unrelated historical connection or replace unavailable exact evidence', async () => {
    for (const retainedConnection of [false, true]) {
      const { api, calls } = coral(
        list({ resolved: true }),
        taskDetail(),
        runDetail({ resolved: true, retainedConnection }),
        connectionRequest({ hiddenRequest: true }),
      )
      const page = await loadActivity(request({ record: run, request: connection }), api, { scope })
      expect(page.detail).toMatchObject({
        request: null,
        requestUnavailable: true,
      })
      expect(calls.filter((call) => call.method === 'connection')).toHaveLength(
        retainedConnection ? 1 : 0,
      )
    }
  })
  it('continues the selected Run action window without discarding an independently authorized review', async () => {
    const { api, calls } = coral(list(), ...reviewReads())
    const page = await loadActivity(
      request({ record: run, request: approval, actionsAfter: 'action-cursor' }),
      api,
      { scope },
    )
    expect(calls.find((call) => call.method === 'run')?.input).toMatchObject({
      name: run,
      actionAfter: 'action-cursor',
    })
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      context: { run: { actionPage: { actions: [], total: '1', next: '' } } },
      request: { name: approval, arguments: '{"to":"private@example.com"}' },
    })
  })
  it.each(['', '.data'])(
    'keeps action discovery and exact review on document and client navigation requests (%s)',
    async (suffix) => {
      const { api, calls } = coral(list(), ...reviewReads())
      const input = new Request(`http://reef/activity${suffix}?pendingAfter=pending-cursor`)
      const summary = await loadActivity(input, api, { scope })
      expect(summary.listPath).toBe(`/${workspace}/activity`)
      expect(summary.pending).toMatchObject({
        actionCount: '1',
        next: '',
        unavailable: false,
      })
      expect(summary.rows[0]).toMatchObject({ name: task, kind: 'task' })
      expect(summary.pending?.actions).toMatchObject([
        {
          action: {
            kind: 'approval',
            name: approval,
            callPath: 'mail.send',
            providerLabel: 'Mail',
            deadline: '1970-01-01T00:15:02.000Z',
          },
          run: {
            name: run,
            agent,
            intent: 'Send reminders',
            actorLabel: 'Collections agent',
            workspaceLabel: 'Finance',
          },
        },
      ])
      expect(loaded(calls, 'actions')).toBe(1)
      expect(loaded(calls, ...DETAIL_OR_CHILD)).toBe(0)
      expect(JSON.stringify(summary)).not.toMatch(/secretSource|private@example|arguments/)
      await loadActivity(input, api, {
        scope,
        reading: { roots: [task], followRoot: false, branches: {} },
      })
      expect(calls.at(-1)).toMatchObject({
        method: 'roots',
        input: { retained: { names: [task] } },
      })
      expect(calls.filter((call) => call.method === 'actions').at(-1)?.input).toMatchObject({
        after: 'pending-cursor',
      })
      const url = new URL(input.url)
      url.searchParams.set('record', run)
      url.searchParams.set('request', approval)
      const review = await loadActivity(new Request(url), api, { scope })
      expect(review.detail).toMatchObject({
        kind: 'invocation',
        request: { actionable: true, arguments: '{"to":"private@example.com"}' },
      })
      const resolved = await loadActivity(
        new Request(url),
        coral(list({ resolved: true }), ...reviewReads({ resolved: true })).api,
        { scope },
      )
      expect(resolved.pending?.actions).toEqual([])
      expect(resolved.rows[0].name).toBe(task)
      expect(resolved.detail).toMatchObject({
        kind: 'invocation',
        request: { actionable: false, state: 'approved', approver: 'reviewer' },
      })
      const concealed = await loadActivity(
        new Request(url),
        coral(list(), ...reviewReads({ hiddenRequest: true })).api,
        { scope },
      )
      expect(concealed.detail).toMatchObject({ request: null, requestUnavailable: true })
    },
  )
  it.each([undefined, '<script>private completion 🪸</script>'])(
    'keeps optional completion feedback only in exact detail (%s)',
    async (feedback) => {
      const { api } = coral(list(), taskDetail({ feedback }))
      const page = await loadActivity(request({ record: task, expand: [task] }), api, { scope })
      expect(page.detail).toMatchObject({ kind: 'task', feedback })
      expect(JSON.stringify([page.rows, page.branches, page.selectedRow])).not.toContain(
        'private completion',
      )
      expect(page.branches[task].rows).toHaveLength(1)
    },
  )
  it.each(['automatic', 'explicit', 'already expanded'] as const)(
    'keeps the terminal Event selected inside the expanded branch with %s navigation',
    async (mode) => {
      const name = `${run}/events/4`
      const { api } = coral(
        list(),
        taskDetail(),
        runEntries({ endedEvent: true }),
        runEvent({ endedEvent: true }),
      )
      const input = request({
        record: name,
        tree: mode === 'automatic' ? undefined : '1',
        expand: mode === 'already expanded' ? [task, run] : undefined,
      })
      let page = await loadActivity(input, api, { scope })
      let location = new URL(input.url)
      if (mode === 'explicit') {
        location = revealLink(page, location.pathname + location.search)
        page = await loadActivity(new Request(location), api, { scope })
      }
      expect(page.selected).toBe(name)
      expect(page.selectedRow).toMatchObject({
        name,
        label: 'execution ended',
        contextImpactBytes: '4',
      })
      expect(page.branches[run].rows).toMatchObject([
        { name, label: 'execution ended', contextImpactBytes: '4' },
      ])
      expect(page.branches[run].rows).toHaveLength(1)
      const markup = renderActivity(location.pathname + location.search, page)
      expect(markup).not.toContain('Outside this workspace or page:')
      expect(markup.match(/aria-selected="true"/g)).toHaveLength(1)
    },
  )
  it('keeps a terminal Event anchor after closing detail, but restores the final-value shortcut when paging away', async () => {
    const name = `${run}/events/4`
    const { api } = coral(list(), taskDetail(), runEntries({ endedEvent: true }))
    const query = { expand: [task, run], [`anchor.${run}`]: name }
    const anchored = await loadActivity(request(query), api, { scope })
    expect(anchored.selected).toBeNull()
    expect(anchored.branches[run].rows).toMatchObject([
      { name, label: 'execution ended', contextImpactBytes: '4' },
    ])
    const paged = await loadActivity(request({ ...query, [`cursor.${run}`]: '3' }), api, { scope })
    expect(paged.branches[run].rows).toMatchObject([
      { name: `${run}/final`, label: 'final value', contextImpactBytes: '4' },
    ])
  })
  it('preserves complete signed totals without summing only loaded children or coercing unavailable to zero', async () => {
    const { api } = coral(
      list({ contextImpact: true }),
      taskDetail({ contextImpact: true }),
      runEntries({ contextImpact: true }),
    )
    const collapsed = await loadActivity(request(), api, { scope })
    const expanded = await loadActivity(request({ expand: [task, run] }), api, {
      scope,
    })
    expect(collapsed.rows[0]).toMatchObject({
      contextImpactBytes: '-141800',
      contextImpactPartial: false,
    })
    expect(expanded.rows[0]).toMatchObject({ contextImpactBytes: '-141800' })
    expect(expanded.branches[task].rows[0]).toMatchObject({ contextImpactBytes: '-148000' })
    expect(expanded.branches[run].rows).toMatchObject([
      { contextImpactBytes: '-150000' },
      { contextImpactBytes: '2000', label: 'final value', name: `${run}/final` },
    ])
    const unavailable = await loadActivity(request(), coral(list()).api, {
      scope,
    })
    expect(unavailable.rows[0].contextImpactBytes).toBeUndefined()
    const running = await loadActivity(
      request(),
      coral(list({ contextImpact: true, partialContextImpact: true })).api,
      { scope },
    )
    expect(running.rows[0]).toMatchObject({
      contextImpactBytes: '-141800',
      contextImpactPartial: true,
    })
  })
  it('loads one bounded mixed root window, with no child or exact evidence for collapsed rows', async () => {
    const { api, calls } = coral(list())
    const page = await loadActivity(request({ after: task, load: 'page' }), api, { scope })
    expect(page.rows).toMatchObject([{ kind: 'task', name: task }])
    expect(page.branches).toEqual({})
    expect(loaded(calls, ...DETAIL_OR_CHILD)).toBe(0)
    expect(calls.find((call) => call.method === 'roots')?.input).toMatchObject({ after: task })
    expect(JSON.stringify(page)).not.toMatch(/secretSource|private@example|unselected-event/)
  })
  it('reopens the list at the reading anchor with its anchored child windows instead of loading a shifted first page', async () => {
    const { api, calls } = coral(list(), ...reviewReads())
    const initial = await loadActivity(request({ expand: [task, run], tree: '1' }), api, { scope })
    const nav = activityNavigation(initial, `/${workspace}/activity`, '', {
      anchor: task,
      branches: {
        [task]: { offset: 125, anchor: interaction, preceding: 25 },
        [run]: { offset: 300, anchor: `${run}/events/320`, preceding: 20 },
      },
    })
    calls.length = 0
    const page = await loadActivity(
      new Request(new URL(nav.recordHref(invocation), 'http://reef')),
      api,
      { scope },
    )
    expect(calls.find((call) => call.method === 'roots')?.input).toMatchObject({
      scope: { workspace },
      before: '',
      after: '',
      anchor: task,
    })
    expect(calls.find((call) => call.method === 'task')?.input).toMatchObject({
      viewport: { offset: 125n, anchor: interaction, preceding: 25 },
      pageSize: ACTIVITY_CHILD_WINDOW,
    })
    expect(calls.find((call) => call.method === 'entries')?.input).toMatchObject({
      viewport: { offset: 300n, anchor: `${run}/events/320`, preceding: 20 },
      pageSize: ACTIVITY_CHILD_WINDOW,
    })
    expect(page.rows.map((row) => row.name)).toEqual([task])
    expect(page).not.toHaveProperty('total')
    expect(page.selectedRow?.name).toBe(invocation)
    expect(page.detail?.kind).toBe('invocation')
  })
  it.each([
    { before: invocation },
    { after: `${run}/events/1` },
    { expand: task, [`offset.${task}`]: '1', [`preceding.${task}`]: '100' },
    { expand: task, [`offset.${task}`]: '-1', [`preceding.${task}`]: '0' },
  ])('rejects unbounded or malformed navigation coordinates: %j', async (query) => {
    await expect(
      loadActivity(request(query), coral(list(), taskDetail()).api, {
        scope,
      }),
    ).rejects.toMatchObject({ status: 400 })
  })
  it('expands accepted exec exactly once and loads only source-free child summaries', async () => {
    const { api, calls } = coral(list(), taskDetail(), runEntries())
    const page = await loadActivity(request({ expand: [task, run], tree: '1' }), api, { scope })
    expect(page.branches[task].rows).toMatchObject([
      {
        kind: 'run',
        name: run,
        submission: { tool: 'exec', outcome: 'succeeded' },
      },
    ])
    expect(page.branches[run].rows).toMatchObject([
      { kind: 'invocation', name: invocation },
      { kind: 'output', name: `${run}/events/3` },
    ])
    expect(page.branches[run].next).toBe('3')
    expect(loaded(calls, 'task')).toBe(1)
    expect(loaded(calls, 'entries')).toBe(1)
    expect(loaded(calls, 'tool', 'run', 'event', 'operation', 'approval')).toBe(0)
    expect(JSON.stringify(page)).not.toMatch(/secretSource|private@example|invoices.*24/)
  })
  it.each([0n, 200n])(
    'pins an independently authorized expanded Run at ordinal %s outside the Task viewport',
    async (position) => {
      const { api, calls } = coral(
        list(),
        taskDetail({ outOfWindow: true }),
        runEntries({ submissionPosition: position }),
      )
      const page = await loadActivity(
        request({ expand: [task, run], [`offset.${task}`]: '100', tree: '1' }),
        api,
        { scope },
      )
      expect(page.branches[task]).toMatchObject({ offset: 100, total: '300', unavailable: false })
      expect(page.branches[task].rows.map((row) => [row.name, row.position])).toEqual(
        position === 0n
          ? [
              [run, 0],
              [`${task}/interactions/visible`, 100],
            ]
          : [
              [`${task}/interactions/visible`, 100],
              [run, 200],
            ],
      )
      expect(page.branches[task].rows.find((row) => row.name === run)).toMatchObject({
        kind: 'run',
        task,
        submission: { name: interaction, tool: 'exec', intent: 'Send invoice reminders' },
      })
      expect(page.branches[run].rows).toMatchObject([
        { name: invocation },
        { name: `${run}/events/3` },
      ])
      expect(loaded(calls, 'tool', 'run', 'event', 'operation', 'approval')).toBe(0)
      expect(calls.find((call) => call.method === 'task')?.input).toMatchObject({
        viewport: { offset: 100n },
      })
      expect(calls.find((call) => call.method === 'entries')?.input).toMatchObject({ name: run })
      expect(page.detail).toBeNull()
      expect(JSON.stringify(page)).not.toMatch(/secretSource|private@example/)
    },
  )
  it.each([
    { runUnavailable: Code.NotFound },
    { runUnavailable: Code.PermissionDenied },
    { runAssociation: 'missing-run' },
    { runAssociation: 'missing-task' },
    { runAssociation: 'other-task' },
    { runAssociation: 'missing-submission' },
    { runAssociation: 'missing-position' },
  ] as const)(
    'does not pin stale or unrelated Run data without an authorized association: %j',
    async (options) => {
      const { api, calls } = coral(list(), taskDetail({ outOfWindow: true }), runEntries(options))
      const page = await loadActivity(request({ expand: [task, run], tree: '1' }), api, { scope })
      expect(calls.filter((call) => call.method === 'entries')).toHaveLength(1)
      expect(page.branches[task].rows.map((row) => row.name)).toEqual([
        `${task}/interactions/visible`,
      ])
      expect(page.branches[run]).toBeUndefined()
      expect(page.rows.map((row) => row.name)).toEqual([task])
      expect(page.selectedRow).toBeNull()
      expect(page.detail).toBeNull()
      expect(JSON.stringify(page.branches)).not.toMatch(/Send invoice reminders|mail.send/)
    },
  )
  it.each([
    { taskUnavailable: true, neighbors: false },
    { taskUnavailable: false, neighbors: false },
    { taskUnavailable: true, neighbors: true },
    { taskUnavailable: false, neighbors: true },
  ])(
    'does not use another Task or an expanded Run reference to bypass an unavailable or collapsed Task: %j',
    async (options) => {
      const { api, calls } = coral(
        list({ neighbors: options.neighbors }),
        taskDetail({ outOfWindow: true, taskUnavailable: options.taskUnavailable }),
        runEntries(),
      )
      const page = await loadActivity(
        request({
          expand: [
            ...(options.taskUnavailable ? [task] : []),
            ...(options.neighbors ? [`${task}-neighbor`] : []),
            run,
          ],
          tree: '1',
        }),
        api,
        { scope },
      )
      expect(page.branches[task]).toEqual(
        options.taskUnavailable ? { rows: [], next: '', unavailable: true } : undefined,
      )
      expect(page.branches[run]).toBeUndefined()
      expect(calls.filter((call) => call.method === 'entries')).toHaveLength(
        options.neighbors ? 1 : 0,
      )
      expect(
        Object.values(page.branches)
          .flatMap((branch) => branch.rows)
          .map((row) => row.name),
      ).toEqual(options.neighbors ? [`${task}-neighbor/interactions/visible`] : [])
      expect(page.detail).toBeNull()
      expect(page.selectedRow).toBeNull()
    },
  )
  it('does not fetch arbitrary or collapsed branches supplied in the URL', async () => {
    const { api, calls } = coral(list())
    await loadActivity(request({ expand: run, tree: '1' }), api, { scope })
    expect(loaded(calls, ...DETAIL_OR_CHILD)).toBe(0)
  })
  it('keeps exact operation detail selected outside the list and opens only its own approval', async () => {
    const { api, calls } = coral(list(), runDetail(), invocationDetail(), approvalDetail())
    const page = await loadActivity(request({ record: invocation, tree: '1' }), api, { scope })
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      arguments: '{"to":"private@example.com"}',
      request: { name: approval, invocation },
    })
    expect(page.selected).toBe(invocation)
    expect(calls.filter((call) => call.method === 'approval')).toHaveLength(1)
  })
  it('opens a selected historical search page without repeating discovery or loading execution source', async () => {
    const { api, calls } = coral(list(), interactionDetail())
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(page.detail).toMatchObject({
      kind: 'tool',
      evidence: {
        kind: 'discovery',
        context: { kind: 'search', query: 'invoices' },
        response: '{"results":[]}',
        // Reef cannot read this response, so it shows the stored text.
        view: { kind: 'raw' },
      },
    })
    expect(loaded(calls, 'tool')).toBe(1)
    expect(loaded(calls, 'task', 'entries', 'run', 'event', 'operation')).toBe(0)
  })
  it('links an Operation hit of a historical search page to the current operation', async () => {
    const { api } = coral(
      list(),
      interactionDetail({ response: stored.complete }),
      catalog({ namespaces: ['github'] }),
    )
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(searchHits(page).map((hit) => hit.currentHref)).toEqual(
      ['checkIfMerged', 'deleteReviewComment', 'merge', 'getMergeAsyncResult', 'updateBranch'].map(
        (path) => `/${workspace}/providers/${githubProvider}/ops/pulls.${path}`,
      ),
    )
  })
  it.each([
    ['its provider is gone', catalog({ namespaces: ['linear'] })],
    // The provider stays, but an update removed the operation.
    ['its operation is gone', catalog({ namespaces: ['github'], operations: ['issues.create'] })],
    ['you cannot list providers', catalog({ concealed: true })],
  ])('leaves an Operation hit without a link when %s', async (_, current) => {
    const { api } = coral(list(), interactionDetail({ response: stored.complete }), current)
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(searchHits(page).map((hit) => hit.currentHref)).toEqual(Array(5).fill(null))
  })
  it.each([
    {
      name: 'links your own Saved Function hit to the current function',
      functions: ['github.openIssuesByLabel'],
      href: `/${workspace}/functions/github.openIssuesByLabel`,
      reads: 1,
    },
    { name: 'leaves a deleted Saved Function without a link', functions: [], href: null, reads: 1 },
    {
      // Reef opens only your own functions, and the Agent owns the function that its search found.
      name: "leaves an Agent's Saved Function hit without a link",
      functions: ['github.openIssuesByLabel'],
      agentRun: true,
      href: null,
      reads: 0,
    },
  ])('$name', async ({ functions, agentRun, href, reads }) => {
    const { api, calls } = coral(
      list(),
      interactionDetail({ response: stored.mixed, agentRun }),
      catalog({ namespaces: ['github'], functions }),
    )
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    const [savedFunction, merge] = searchHits(page)
    expect(savedFunction).toMatchObject({ kind: 'saved_function', currentHref: href })
    expect(merge.currentHref).toBe(`/${workspace}/providers/${githubProvider}/ops/pulls.merge`)
    expect(loaded(calls, 'listSavedFunctions')).toBe(reads)
  })
  it('links a historical description to the current operation', async () => {
    const { api } = coral(
      list(),
      interactionDetail({
        describe: 'coral.providers.github.issues.create',
        types: ['Repository'],
        response: stored.describeOperation,
      }),
      catalog({ namespaces: ['github'] }),
    )
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(page.detail).toMatchObject({
      evidence: {
        context: {
          kind: 'describe',
          callPath: 'coral.providers.github.issues.create',
          types: ['Repository'],
        },
        view: {
          kind: 'describe',
          description: {
            title: 'Create an issue',
            currentHref: `/${workspace}/providers/${githubProvider}/ops/issues.create`,
          },
        },
      },
    })
  })
  it('direct output links reveal and seek both the task and execution ancestors', async () => {
    const { api, calls } = coral(list(), taskDetail(), runEntries(), runEvent())
    const page = await loadActivity(request({ record: `${run}/events/3` }), api, { scope })
    expect(page.expanded).toEqual([task, run])
    expect(page.branches[task].rows[0].name).toBe(run)
    expect(page.branches[run].rows).toContainEqual(
      expect.objectContaining({ name: `${run}/events/3` }),
    )
    expect(calls.find((call) => call.method === 'task')?.input).toMatchObject({ anchor: run })
    expect(calls.find((call) => call.method === 'entries')?.input).toMatchObject({
      anchor: `${run}/events/3`,
    })
  })
  it('retains effective direct-link anchors after the observation route normalizes tree state', async () => {
    const page = await loadActivity(
      request({
        record: `${run}/events/3`,
        tree: '1',
        expand: [task, run],
        [`anchor.${task}`]: run,
        [`anchor.${run}`]: `${run}/events/3`,
      }),
      coral(list(), taskDetail(), runEntries(), runEvent()).api,
      { scope },
    )
    expect(page.anchors).toEqual({ [task]: run, [run]: `${run}/events/3` })
  })
  it('reads one exact emission without preloading other events or source', async () => {
    const { api, calls } = coral(list(), runEvent())
    const page = await loadActivity(request({ record: `${run}/events/3`, tree: '1' }), api, {
      scope,
    })
    expect(page.detail).toMatchObject({
      kind: 'output',
      event: { kind: 'program.emitted', value: '{"invoices":24}' },
    })
    expect(loaded(calls, 'event')).toBe(1)
    expect(loaded(calls, 'task', 'entries', 'run', 'operation')).toBe(0)
    expect(JSON.stringify(page)).not.toContain('secretSource')
  })
  it('keeps the final value and original intent on the expanded exec row', async () => {
    const page = await loadActivity(
      request({ expand: [task, run], tree: '1' }),
      coral(list(), taskDetail(), runEntries({ completedEntries: true })).api,
      { scope },
    )
    expect(page.branches[task].rows).toMatchObject([
      {
        kind: 'run',
        name: run,
        hasFinalValue: true,
        submission: { intent: 'Send reminders' },
      },
    ])
    expect(page.branches[run].rows.every((row) => !row.name.endsWith('/final'))).toBe(true)
    expect(page.detail).toBeNull()
    expect(JSON.stringify(page)).not.toContain('secretSource')
  })
  it('retains accepted source snapshots and distinguishes explicit null from no final value', async () => {
    const present = await loadActivity(
      request({ record: `${run}/final`, tree: '1' }),
      coral(list(), runDetail()).api,
      { scope },
    )
    expect(present.detail).toMatchObject({
      kind: 'run',
      terminal: { kind: 'succeeded', finalValue: 'null' },
      linkedFunctions: [{ source: 'export default () => 41' }],
    })
    expect(present.selected).toBe(run)
    expect(present.selectedRow).toMatchObject({ kind: 'run', name: run, hasFinalValue: true })
    expect(JSON.stringify(present)).not.toContain('unselected-event')
    const absent = await loadActivity(
      request({ record: `${run}/final`, tree: '1' }),
      coral(list(), runDetail({ noFinalValue: true })).api,
      { scope },
    )
    expect(absent.unavailable).toBe(true)
    expect(absent.detail).toBeNull()
  })
  it('preserves upstream 404 as an upstream outcome, not a Coral failure', async () => {
    const page = await loadActivity(
      request({ record: invocation, tree: '1' }),
      coral(
        list({ resolved: true }),
        runDetail({ resolved: true }),
        invocationDetail({ resolved: true }),
        approvalDetail({ resolved: true }),
      ).api,
      { scope },
    )
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      result: {
        kind: 'upstream_outcome',
        value: '{"data":null,"metadata":{"status":404}}',
      },
    })
  })
  it('loads only the selected retained approval and shows its recorded reviewer after resolution', async () => {
    const { api, calls } = coral(
      list({ resolved: true }),
      runDetail({ resolved: true }),
      invocationDetail({ resolved: true }),
      approvalDetail({ resolved: true }),
    )
    const page = await loadActivity(
      request({ record: invocation, request: approval, tree: '1' }),
      api,
      { scope },
    )
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      request: { state: 'approved', approver: 'reviewer' },
    })
    expect(calls.filter((call) => call.method === 'approval')).toHaveLength(1)
  })
  it('conceals an unavailable request without discarding authorized operation evidence', async () => {
    const page = await loadActivity(
      request({ record: invocation, request: approval, tree: '1' }),
      coral(list(), invocationDetail(), approvalDetail({ hiddenRequest: true })).api,
      { scope },
    )
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      request: null,
      requestUnavailable: true,
    })
  })
  it('independently authorizes exact detail when the list scope is unavailable', async () => {
    const page = await loadActivity(
      request({ record: invocation, tree: '1' }),
      coral(list({ listConcealed: true }), runDetail(), invocationDetail(), approvalDetail()).api,
      { scope: { workspace, agent } },
    )
    expect(page.listUnavailable).toBe(true)
    expect(page.rows).toEqual([])
    expect(page.detail?.kind).toBe('invocation')
  })
  it('conceals lost detail authority instead of substituting cached or current content', async () => {
    const before = await loadActivity(
      request({ record: invocation }),
      coral(list(), ...reviewReads()).api,
      { scope },
    )
    expect(before.detail?.kind).toBe('invocation')
    const after = await loadActivity(
      request({ record: invocation }),
      coral(list(), lostAuthority()).api,
      { scope },
    )
    expect(after.detail).toBeNull()
    expect(after.selectedRow).toBeNull()
    expect(after.unavailable).toBe(true)
    expect(JSON.stringify(after)).not.toContain('private@example.com')
  })
  it('does not mix owner-visible Agent work into personal scope', async () => {
    const page = await loadActivity(
      request({ anchor: task, expand: [task, run] }),
      coral(list({ agentRun: true })).api,
      { scope },
    )
    expect(page.rows).toEqual([])
    expect(page.branches).toEqual({})
  })
  it('preserves the stable Run path over unrelated query selection', async () => {
    const { api, calls } = coral(list(), runDetail())
    const page = await loadRunActivity(request({ record: task, tree: '1' }), api, {
      workspace: workspace.split('/')[1],
      run: run.split('/').at(-1)!,
    })
    expect(page.selected).toBe(run)
    expect(page.detail?.kind).toBe('run')
    expect(calls.some((call) => call.method === 'operation')).toBe(false)
  })
  it('allows exact child selection on a stable Run link but conceals an operation from another Run', async () => {
    const { api } = coral(list(), ...reviewReads())
    const own = await loadRunActivity(request({ record: invocation, request: approval }), api, {
      workspace: workspace.split('/')[1],
      run: run.split('/').at(-1)!,
    })
    expect(own.selected).toBe(invocation)
    expect(own.detail).toMatchObject({ kind: 'invocation', request: { name: approval } })
    const other = await loadRunActivity(request({ record: invocation, request: approval }), api, {
      workspace: workspace.split('/')[1],
      run: '00000000-0000-4000-8000-000000000099',
    })
    expect(other).toMatchObject({ detail: null, selectedRow: null, unavailable: true })
    expect(JSON.stringify(other)).not.toContain('private@example.com')
  })
  it('reveals an authorized Agent Run from its stable link in Agent history, not personal history', async () => {
    const { api } = coral(
      list({ agentRun: true, neighbors: true }),
      taskDetail({ agentRun: true }),
      runEntries({ agentRun: true }),
      runDetail({ agentRun: true }),
    )
    const path = `/${run}?tree=1`
    const page = await loadRunActivity(new Request(`http://reef${path}`), api, {
      workspace: workspace.split('/')[1],
      run: run.split('/').at(-1)!,
    })
    const target = revealLink(page, path)
    expect(target.pathname).toBe(`/${agent}`)
    const revealed = await loadActivity(new Request(target), api, {
      scope: { workspace, agent },
    })
    expect(revealed.listUnavailable).toBe(false)
    expect(target.searchParams.has('scope')).toBe(false)
    expect(target.searchParams.get('anchor')).toBe(task)
    expect(revealed.rows).toMatchObject([
      { kind: 'task', name: task },
      { name: `${task}-neighbor` },
    ])
    expect(revealed.rows).toHaveLength(2)
    expect(revealed.branches[task].rows).toMatchObject([{ kind: 'run', name: run }])
    expect(revealed.selected).toBe(run)
    expect(revealed.detail?.kind).toBe('run')
  })
  it.each([task, interaction, invocation, `${run}/events/3`])(
    'reveals independently authorized Agent record %s in its owning history',
    async (record) => {
      const { api } = coral(
        list({ agentRun: true }),
        ...reviewReads({ agentRun: true }),
        interactionDetail({ agentRun: true }),
        runEvent({ agentRun: true }),
      )
      const input = request({ record, tree: '1' })
      const page = await loadActivity(input, api, { scope })
      const target = revealLink(page, new URL(input.url).pathname + new URL(input.url).search)
      expect(target.pathname).toBe(`/${agent}`)
      const revealed = await loadActivity(new Request(target), api, {
        scope: { workspace, agent },
      })
      expect(revealed.listUnavailable).toBe(false)
      expect(revealed.selected).toBe(record)
      expect(revealed.detail).not.toBeNull()
    },
  )
  it('reveals personal detail from an unavailable Agent list without carrying its attribution', async () => {
    const { api } = coral(list({ listConcealed: true }), taskDetail(), runEntries(), runDetail())
    const input = request({ record: run, tree: '1' })
    const page = await loadActivity(input, api, {
      scope: { workspace, agent },
    })
    expect(page.listUnavailable).toBe(true)
    const target = revealLink(page, `/${agent}${new URL(input.url).search}`)
    expect(target.pathname).toBe(`/${workspace}/activity`)
    const revealed = await loadActivity(new Request(target), api, { scope })
    expect(revealed.listUnavailable).toBe(false)
    expect(revealed.rows).toMatchObject([{ name: task }])
    expect(revealed.branches[task].rows).toMatchObject([{ name: run }])
    expect(revealed.selected).toBe(run)
  })
  it.each([
    { record: 'bad' },
    { anchor: invocation },
    { expand: invocation },
    { expand: [task, run], [`cursor.${run}`]: '-1' },
    { expand: [task, run], [`cursor.${run}`]: '18446744073709551616' },
    { expand: Array.from({ length: 21 }, (_, i) => `${workspace}/tasks/${i}`) },
  ])('rejects malformed or unbounded active tree state: %j', async (query) => {
    await expect(
      loadActivity(request(query), coral(list(), taskDetail()).api, { scope }),
    ).rejects.toMatchObject({ status: 400 })
  })
})

describe('links from before continuous loading', () => {
  const neighbor = `${task}-neighbor`
  const empty: Handlers = { activity: { browseActivityRoots: () => ({ roots: [] }) } }
  it.each([
    { link: { before: neighbor }, served: list({ neighbors: true }), anchor: task },
    { link: { after: neighbor }, served: list({ neighbors: true }), anchor: task },
    // Nothing is left on that side, so the link opens the newest records.
    { link: { after: neighbor }, served: empty, anchor: null },
  ])('redirects $link to the anchor of the page it named', async ({ link, served, anchor }) => {
    const { api, calls } = coral(served)
    const response: unknown = await loadActivity(
      request({ ...link, record: run, expand: task, tree: '1' }),
      api,
      { scope },
    ).catch((error: unknown) => error)
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).status).toBe(302)
    const location = new URL((response as Response).headers.get('Location')!, 'http://reef')
    expect(location.pathname).toBe(`/${workspace}/activity`)
    expect(Object.fromEntries(location.searchParams)).toEqual({
      record: run,
      expand: task,
      tree: '1',
      ...(anchor ? { anchor } : {}),
    })
    // Only the page read runs; the destination loads the rest.
    expect(calls).toEqual([
      { method: 'roots', input: expect.objectContaining({ ...link, anchor: '', pageSize: 25 }) },
    ])
  })
  it('loads the page itself for the reader that continues the list', async () => {
    const { api, calls } = coral(list({ neighbors: true }))
    const page = await loadActivity(request({ after: neighbor, load: 'page' }), api, { scope })
    expect(page.rows.map((row) => row.name)).toEqual([task, neighbor])
    expect(calls.find((call) => call.method === 'roots')?.input).toMatchObject({ after: neighbor })
  })
})
