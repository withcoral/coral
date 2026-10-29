import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { createElement } from 'react'
import { renderToString } from 'react-dom/server'
import { createMemoryRouter, RouterProvider } from 'react-router'
import { Activity } from '@/views/activity/activity'
import { activityNavigation } from '@/views/activity/navigation'
import { stored } from '@/views/activity/discovery/fixtures'
import type { ActivityPage, LiveLatest } from '@/views/activity/types'
import {
  ActivityRunState,
  ActivityInvocationState,
  ActivityInvocationOutcome,
  ActivityLogLevel,
} from '@/generated/coral/management/v1/activity_pb'
import { AccountConnectionState } from '@/generated/coral/management/v1/account_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
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
const tool = {
  name: invocation,
  workspace,
  run,
  task,
  tool: 'send',
  callPath: 'mail.send',
  state: ActivityInvocationState.PENDING,
  requiresAction: true,
}
// A source location as Coral sends it on a tool, an emission or a log.
type StoredLocation = {
  document:
    | { case: 'program'; value: Record<string, never> }
    | { case: 'savedFunction'; value: string }
    | { case: undefined; value?: undefined }
  line: number
  column: number
}
const linkedSnapshot = `${workspace}/savedFunctions/00000000-0000-4000-8000-000000000009`
const inSnapshot: StoredLocation = {
  document: { case: 'savedFunction', value: linkedSnapshot },
  line: 2,
  column: 11,
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
  inspectInvocation: 'invocation',
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
    /** Refuse the Task read, with this code or NotFound. */
    taskUnavailable?: boolean | Code
    contextImpact?: boolean
    outOfWindow?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      inspectTask: (input) => {
        if (options.taskUnavailable && input.name === task)
          refuse(
            'task unavailable',
            options.taskUnavailable === true ? Code.NotFound : options.taskUnavailable,
          )
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
          intent: 'Find tools',
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
/** Every Tool that the stored discovery fixtures name. */
const fixtureTools = [
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
    tools?: string[]
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
                callRoot: `coral.providers.${namespace}`,
              })),
            },
      listTools: () => ({
        tools: (options.tools ?? fixtureTools).map((relativePath) => ({
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
    location?: StoredLocation
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
                      ...tool,
                      contextImpactBytes: options.contextImpact ? -150000n : undefined,
                      sourceLocation: options.location,
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
                      sourceLocation: options.location,
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
    runDetailUnavailable?: boolean | Code
    resolved?: boolean
    retainedConnection?: boolean
    noFinalValue?: boolean
  } = {},
): Handlers {
  return {
    activity: {
      inspectRun: (input) => {
        if (options.runDetailUnavailable)
          refuse(
            'run unavailable',
            options.runDetailUnavailable === true ? Code.NotFound : options.runDetailUnavailable,
          )
        return {
          run: {
            ...execution,
            agent: agentOf(options),
            actionPage: {
              actions: options.resolved || input.actionAfter ? [] : execution.actionPage.actions,
              total: options.resolved ? 0n : 1n,
              // One page holds up to 100 pending actions, so one action has no next page.
              nextAction: '',
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

function runEvent(
  options: AgentOption & {
    endedEvent?: boolean
    logged?: boolean
    location?: StoredLocation
    eventUnavailable?: Code
  } = {},
): Handlers {
  return {
    activity: {
      inspectRunEvent: (input) => {
        if (options.eventUnavailable) refuse('event unavailable', options.eventUnavailable)
        return {
          contextImpactBytes: options.endedEvent ? 4n : undefined,
          run: { ...execution, agent: agentOf(options) },
          event: {
            sequence: input.sequence,
            occurredAtUnixNanos: 3_000_000_000n,
            payload: options.endedEvent
              ? { case: 'ended', value: { state: ActivityRunState.SUCCEEDED } }
              : options.logged
                ? {
                    case: 'programLogged',
                    value: { level: ActivityLogLevel.INFO, message: 'sent 24 reminders' },
                  }
                : { case: 'programEmitted', value: { valueJson: '{"invoices":24}' } },
            sourceLocation: options.location,
          },
        }
      },
    },
  }
}

function invocationDetail(
  options: AgentOption & {
    resolved?: boolean
    /** No pending approval: selecting it never triggers the initial-request lookup. */
    ordinary?: boolean
    /** Retains a shared account connection request instead of an approval. */
    connection?: boolean
    location?: StoredLocation
    invocationUnavailable?: Code
  } = {},
): Handlers {
  return {
    activity: {
      inspectInvocation: () => {
        if (options.invocationUnavailable) refuse('tool unavailable', options.invocationUnavailable)
        return {
          invocation: {
            ...tool,
            agent: agentOf(options),
            state: options.resolved
              ? ActivityInvocationState.ENDED
              : ActivityInvocationState.PENDING,
            outcome: options.resolved ? ActivityInvocationOutcome.UPSTREAM_OUTCOME : undefined,
            requiresAction: !options.ordinary,
            sourceLocation: options.location,
          },
          argumentsJson: '{"to":"private@example.com"}',
          outcome: options.resolved
            ? {
                case: 'upstreamOutcomeJson',
                value: '{"status":404,"data":null,"metadata":{"kind":"http"}}',
              }
            : undefined,
          requests: options.connection
            ? [{ request: { case: 'accountConnection', value: connection } }]
            : options.ordinary
              ? []
              : [{ request: { case: 'approval', value: approval } }],
        }
      },
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

/** The reads that open a tool for review inside its Task and Run. */
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
  'invocation',
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
  it('selects and loads the exact tool approval once, but never chooses another action during observation', async () => {
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
  it('does not show a sibling tool approval as the selected tool', async () => {
    const { api } = coral(
      list(),
      taskDetail(),
      runEntries(),
      invocationDetail(),
      approvalDetail({ wrongApprovalInvocation: true }),
      runDetail(),
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
  it('conceals a tool approval whose Run is unavailable instead of reviewing it without context', async () => {
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
  it('opens an exact review beside the Run with one read of its first page of actions', async () => {
    const { api, calls } = coral(list(), ...reviewReads())
    const page = await loadActivity(request({ record: run, request: approval }), api, { scope })
    // Without a "More actions" cursor, the review reads the Run once, at its first page.
    expect(calls.filter((call) => call.method === 'run').map((call) => call.input)).toEqual([
      expect.objectContaining({ name: run, actionAfter: '' }),
    ])
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      context: { run: { actionPage: { total: '1', next: '' } } },
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
        runDetail(),
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
    // The approval review also reads its Task once for the intent; the branch read is last.
    expect(calls.findLast((call) => call.method === 'task')?.input).toMatchObject({
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
    expect(loaded(calls, 'tool', 'run', 'event', 'invocation', 'approval')).toBe(0)
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
      expect(loaded(calls, 'tool', 'run', 'event', 'invocation', 'approval')).toBe(0)
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
  it.each([
    ['from its Run', run, false, goal.intent],
    ['from its tool', invocation, false, goal.intent],
    // An unavailable Task leaves the intent out, not the review.
    ['from its tool, with its Task unavailable', invocation, true, undefined],
  ])(
    'reviews an approval selected %s with the intent of the Task that owns the Run',
    async (_label, record, taskUnavailable, taskIntent) => {
      const { api, calls } = coral(
        list(),
        runDetail(),
        invocationDetail(),
        approvalDetail(),
        taskDetail({ taskUnavailable }),
      )
      const page = await loadActivity(request({ record, request: approval, tree: '1' }), api, {
        scope,
      })
      expect(page.detail).toMatchObject({
        kind: 'invocation',
        requestUnavailable: false,
        request: { name: approval },
      })
      const review = page.detail?.kind === 'invocation' ? page.detail.request : null
      expect(review?.kind === 'approval' && review.taskIntent).toBe(taskIntent ?? undefined)
      expect(calls.find((call) => call.method === 'task')?.input).toMatchObject({ name: task })
    },
  )
  // Catches: an optional Task read that fails the whole approval on a passing fault, or that
  // hides an expired session, a refused request or a cancelled load behind a readable review.
  it.each([
    { label: 'unavailable', code: Code.Unavailable, readable: true },
    { label: 'failing inside Coral', code: Code.Internal, readable: true },
    { label: 'refused for your session', code: Code.Unauthenticated, readable: false },
    { label: 'refused as invalid', code: Code.InvalidArgument, readable: false },
    // An aborted load reaches Reef as Canceled.
    { label: 'cancelled with the load', code: Code.Canceled, readable: false },
  ])(
    'with its Task read $label, keeps the approval review readable: $readable',
    async ({ code, readable }) => {
      const { api } = coral(
        list(),
        runDetail(),
        invocationDetail(),
        approvalDetail(),
        taskDetail({ taskUnavailable: code }),
      )
      const load = loadActivity(
        request({ record: invocation, request: approval, tree: '1' }),
        api,
        { scope },
      )
      if (!readable) {
        await expect(load).rejects.toMatchObject({ code })
        return
      }
      const page = await load
      expect(page.detail).toMatchObject({
        kind: 'invocation',
        requestUnavailable: false,
        request: { kind: 'approval', name: approval, arguments: expect.any(String) },
      })
      const review = page.detail?.kind === 'invocation' ? page.detail.request : null
      expect(review && 'taskIntent' in review).toBe(false)
    },
  )
  it('keeps exact tool detail selected outside the list and opens only its own approval', async () => {
    const { api, calls } = coral(
      list(),
      runDetail(),
      invocationDetail(),
      approvalDetail(),
      taskDetail(),
    )
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
    expect(loaded(calls, 'task', 'entries', 'run', 'event', 'invocation')).toBe(0)
  })
  it('links a Tool hit of a historical search page to the current tool', async () => {
    const { api } = coral(
      list(),
      interactionDetail({ response: stored.complete }),
      catalog({ namespaces: ['github'] }),
    )
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(searchHits(page).map((hit) => hit.currentHref)).toEqual(
      ['checkIfMerged', 'deleteReviewComment', 'merge', 'getMergeAsyncResult', 'updateBranch'].map(
        (path) => `/${workspace}/providers/${githubProvider}/tools/pulls.${path}`,
      ),
    )
  })
  it.each([
    ['its provider is current', ['github'], 'pulls.checkIfMerged'],
    ['its provider is gone', ['linear'], 'coral.providers.github.pulls.checkIfMerged'],
  ])('titles an untitled Tool hit by its path when %s', async (_, namespaces, title) => {
    const response = JSON.parse(stored.complete) as { hits: { title: string | null }[] }
    response.hits[0]!.title = null
    const { api } = coral(
      list(),
      interactionDetail({ response: JSON.stringify(response) }),
      catalog({ namespaces }),
    )
    const page = await loadActivity(request({ record: interaction, tree: '1' }), api, { scope })
    expect(searchHits(page)[0]?.title).toBe(title)
  })
  it.each([
    ['its provider is gone', catalog({ namespaces: ['linear'] })],
    // The provider stays, but an update removed the tool.
    ['its tool is gone', catalog({ namespaces: ['github'], tools: ['issues.create'] })],
    ['you cannot list providers', catalog({ concealed: true })],
  ])('leaves a Tool hit without a link when %s', async (_, current) => {
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
    expect(merge.currentHref).toBe(`/${workspace}/providers/${githubProvider}/tools/pulls.merge`)
    expect(loaded(calls, 'listSavedFunctions')).toBe(reads)
  })
  it('links a historical description to the current tool', async () => {
    const { api } = coral(
      list(),
      interactionDetail({
        describe: 'coral.providers.github.issues.create',
        types: ['Repository'],
        response: stored.describeTool,
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
            currentHref: `/${workspace}/providers/${githubProvider}/tools/issues.create`,
          },
        },
      },
    })
  })
  it('direct output links reveal and seek both the task and execution ancestors', async () => {
    const { api, calls } = coral(list(), taskDetail(), runEntries(), runEvent(), runDetail())
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
      coral(list(), taskDetail(), runEntries(), runEvent(), runDetail()).api,
      { scope },
    )
    expect(page.anchors).toEqual({ [task]: run, [run]: `${run}/events/3` })
  })
  it('reads one exact emission, with its parent Run’s authorized accepted-source context', async () => {
    const { api, calls } = coral(list(), runEvent(), runDetail())
    const page = await loadActivity(request({ record: `${run}/events/3`, tree: '1' }), api, {
      scope,
    })
    expect(page.detail).toMatchObject({
      kind: 'output',
      event: { kind: 'program.emitted', value: '{"invoices":24}' },
      context: { kind: 'run', source: 'return secretSource;' },
    })
    expect(loaded(calls, 'event')).toBe(1)
    expect(loaded(calls, 'run')).toBe(1)
    expect(loaded(calls, 'task', 'entries', 'invocation')).toBe(0)
  })
  // Catches: a location Coral stored lost, or mistranslated, on one of the reads that carry it.
  it.each<[string, Handlers[], Parameters<typeof request>[0], (page: ActivityPage) => unknown[]]>([
    [
      'a selected tool',
      [invocationDetail({ resolved: true, ordinary: true, location: inSnapshot }), runDetail()],
      { record: invocation },
      (page) => [page.selectedRow, page.detail?.kind === 'invocation' && page.detail.invocation],
    ],
    [
      'a selected emission',
      [runEvent({ location: inSnapshot }), runDetail()],
      { record: `${run}/events/3` },
      (page) => [page.selectedRow],
    ],
    [
      'an expanded Run’s tool and emission',
      [taskDetail(), runEntries({ location: inSnapshot })],
      { expand: [task, run] },
      (page) => page.branches[run]!.rows,
    ],
  ])('keeps the source location Coral stored for %s', async (_, reads, query, records) => {
    const { api } = coral(list(), ...reads)
    const page = await loadActivity(request({ ...query, tree: '1' }), api, { scope })
    const shown = records(page)
    expect(shown.length).toBeGreaterThan(0)
    for (const record of shown)
      expect(record).toMatchObject({
        sourceLocation: {
          document: { kind: 'saved_function', savedFunction: linkedSnapshot },
          line: 2,
          column: 11,
        },
      })
  })
  // Catches: an unknown document kind or a record without a location gaining a guessed one.
  it.each<[string, StoredLocation | undefined, unknown]>([
    [
      'the accepted program',
      { document: { case: 'program', value: {} }, line: 3, column: 9 },
      { document: { kind: 'program' }, line: 3, column: 9 },
    ],
    [
      'a document kind this client does not know',
      { document: { case: undefined }, line: 3, column: 9 },
      undefined,
    ],
    ['no location', undefined, undefined],
  ])('reads a tool located in %s', async (_, location, expected) => {
    const { api } = coral(
      list(),
      invocationDetail({ resolved: true, ordinary: true, location }),
      runDetail(),
    )
    const page = await loadActivity(request({ record: invocation, tree: '1' }), api, { scope })
    expect(page.selectedRow?.kind === 'invocation' && page.selectedRow.sourceLocation).toEqual(
      expected,
    )
  })
  // Catches: an optional Run read that takes the selected record down on a passing fault, or
  // that hides an expired session, a refused request or a cancelled load behind the record.
  const selections = [
    {
      selected: 'a tool',
      reads: (code?: Code) => [
        invocationDetail({ resolved: true, ordinary: true, invocationUnavailable: code }),
      ],
      record: invocation,
      shown: { kind: 'invocation', invocation: { name: invocation } },
    },
    {
      selected: 'a log',
      reads: (code?: Code) => [runEvent({ logged: true, eventUnavailable: code })],
      record: `${run}/events/3`,
      shown: { kind: 'output', event: { kind: 'program.logged', message: 'sent 24 reminders' } },
    },
    {
      selected: 'an emission',
      reads: (code?: Code) => [runEvent({ eventUnavailable: code })],
      record: `${run}/events/3`,
      shown: { kind: 'output', event: { kind: 'program.emitted', value: '{"invoices":24}' } },
    },
  ]
  const outcomes = [
    { label: 'unavailable', code: Code.Unavailable, readable: true },
    { label: 'failing inside Coral', code: Code.Internal, readable: true },
    { label: 'not found', code: Code.NotFound, readable: true },
    { label: 'refused to you', code: Code.PermissionDenied, readable: true },
    { label: 'refused for your session', code: Code.Unauthenticated, readable: false },
    { label: 'refused as invalid', code: Code.InvalidArgument, readable: false },
    // An aborted load reaches Reef as Canceled.
    { label: 'cancelled with the load', code: Code.Canceled, readable: false },
  ]
  const withRunRead = (readable: boolean) =>
    selections.flatMap((selection) =>
      outcomes
        .filter((outcome) => outcome.readable === readable)
        .map((outcome) => ({ ...selection, ...outcome })),
    )
  it.each(withRunRead(true))(
    'keeps $selected readable without its source when its Run read is $label',
    async ({ reads, record, shown, code }) => {
      const { api } = coral(list(), ...reads(), runDetail({ runDetailUnavailable: code }))
      const page = await loadActivity(request({ record, tree: '1' }), api, { scope })
      expect(page.unavailable).toBe(false)
      expect(page.detail).toMatchObject(shown)
      expect(page.detail).not.toHaveProperty('context')
    },
  )
  it.each(withRunRead(false))(
    'fails the load of $selected when its Run read is $label',
    async ({ reads, record, code }) => {
      const { api } = coral(list(), ...reads(), runDetail({ runDetailUnavailable: code }))
      await expect(
        loadActivity(request({ record, tree: '1' }), api, { scope }),
      ).rejects.toMatchObject({ code })
    },
  )
  // Catches: the Run's fallback swallowing a failure of the selected record's own read.
  it.each(
    selections.flatMap((selection) =>
      [Code.Unavailable, Code.Internal, Code.Unauthenticated].map((code) => ({
        ...selection,
        code,
      })),
    ),
  )(
    'fails the load when $selected itself fails to load with code $code',
    async ({ reads, record, code }) => {
      const { api } = coral(list(), ...reads(code), runDetail())
      await expect(
        loadActivity(request({ record, tree: '1' }), api, { scope }),
      ).rejects.toMatchObject({ code })
    },
  )
  it('authorizes an ordinary tool selection’s parent Run source the same way, not only an approval review', async () => {
    const { api, calls } = coral(
      list(),
      invocationDetail({ resolved: true, ordinary: true }),
      runDetail(),
    )
    const page = await loadActivity(request({ record: invocation, tree: '1' }), api, { scope })
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      context: { kind: 'run', source: 'return secretSource;' },
    })
    expect(loaded(calls, 'run')).toBe(1)
  })
  // Catches: a request in the query skipping the Run's source, so the drawer lost it.
  it.each<[string, Handlers[], string, Partial<ActivityPage['detail']>]>([
    [
      'an unknown request',
      [invocationDetail({ resolved: true, ordinary: true })],
      'bogus',
      { requestUnavailable: true },
    ],
    [
      'a concealed approval',
      [invocationDetail(), approvalDetail({ hiddenRequest: true })],
      approval,
      { requestUnavailable: true },
    ],
    [
      'an approval for another tool',
      [invocationDetail(), approvalDetail({ wrongApprovalInvocation: true })],
      approval,
      { requestUnavailable: true },
    ],
    [
      'an account connection',
      [invocationDetail({ connection: true }), connectionRequest()],
      connection,
      { request: { kind: 'account_connection' } },
    ],
  ] as never)(
    'keeps the parent Run’s source for a tool selected with %s',
    async (_, handlers, selected, expected) => {
      const { api, calls } = coral(list(), runDetail(), ...handlers)
      const page = await loadActivity(
        request({ record: invocation, request: selected, tree: '1' }),
        api,
        { scope },
      )
      expect(page.unavailable).toBe(false)
      expect(page.detail).toMatchObject({
        kind: 'invocation',
        invocation: { name: invocation },
        context: { kind: 'run', source: 'return secretSource;' },
        ...expected,
      })
      expect(loaded(calls, 'run')).toBeGreaterThanOrEqual(1)
    },
  )
  it('does not re-read the parent Run when the selection is already under approval review', async () => {
    const { api, calls } = coral(
      list(),
      runDetail(),
      invocationDetail(),
      approvalDetail(),
      taskDetail(),
    )
    await loadActivity(request({ record: invocation, tree: '1' }), api, { scope })
    expect(loaded(calls, 'run')).toBe(1)
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
        taskDetail(),
      ).api,
      { scope },
    )
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      result: {
        kind: 'upstream_outcome',
        value: '{"status":404,"data":null,"metadata":{"kind":"http"}}',
      },
    })
  })
  it('loads only the selected retained approval and shows its recorded reviewer after resolution', async () => {
    const { api, calls } = coral(
      list({ resolved: true }),
      runDetail({ resolved: true }),
      invocationDetail({ resolved: true }),
      approvalDetail({ resolved: true }),
      taskDetail(),
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
  it('conceals an unavailable request without discarding authorized tool evidence', async () => {
    const page = await loadActivity(
      request({ record: invocation, request: approval, tree: '1' }),
      coral(list(), invocationDetail(), approvalDetail({ hiddenRequest: true }), runDetail()).api,
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
      coral(
        list({ listConcealed: true }),
        runDetail(),
        invocationDetail(),
        approvalDetail(),
        taskDetail(),
      ).api,
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
    expect(calls.some((call) => call.method === 'invocation')).toBe(false)
  })
  it('allows exact child selection on a stable Run link but conceals a tool from another Run', async () => {
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
  // A Run identifier is four or five words, or the UUID of a Run from before
  // Coral named Runs with words. The stable link accepts both shapes.
  it.each([
    'height-word-database-library',
    'small-title-work-image-power',
    '00000000-0000-4000-8000-000000000099',
  ])('asks Coral about the Run named %s', async (id) => {
    const { api, calls } = coral(list(), runDetail())
    await loadRunActivity(request({ record: task, tree: '1' }), api, {
      workspace: workspace.split('/')[1],
      run: id,
    })
    const asked = calls.some((call) =>
      JSON.stringify(call.input, (_key, value: unknown) =>
        typeof value === 'bigint' ? value.toString() : value,
      ).includes(id),
    )
    expect(asked).toBe(true)
  })
  it.each([
    'height-word-database',
    'height-word-database-library-power-title',
    'Height-word-database-library',
    'height-word-database-librar1',
    'abcdefghijklmnopqrstu-word-database-library',
    '00000000-0000-4000-8000-00000000009',
    '../runs/height-word-database-library',
  ])('rejects the Run name %s before asking Coral', async (id) => {
    const { api, calls } = coral(list(), runDetail())
    expect(() =>
      loadRunActivity(request({ record: task, tree: '1' }), api, {
        workspace: workspace.split('/')[1],
        run: id,
      }),
    ).toThrow(expect.objectContaining({ status: 404 }))
    expect(calls).toEqual([])
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

/** A tool of `run` that waits on a request, as Coral lists and inspects it. */
interface Wait {
  id: string
  startedAt: bigint
  request: { kind: 'approval' | 'accountConnection'; id: string }
  /** The caller can decide the approval, or the connection is still pending. */
  actionable?: boolean
  /** Coral still reports the tool waiting. */
  waiting?: boolean
  /** Its approval was approved; the tool no longer waits. */
  decided?: boolean
  /** The 100-child page of the Run's children that lists it. */
  page?: number
}
const waitName = (id: string) => `${workspace}/invocations/${id}`
const requestName = (wait: Wait) =>
  `${workspace}/${wait.request.kind === 'approval' ? 'approvals' : 'accountConnections'}/${wait.request.id}`
const waiting = (wait?: Wait) => wait?.waiting !== false && !wait?.decided
/** The action summary Coral lists for the request `name`. */
const summary = (name: string) => ({
  request: {
    request: name.includes('/approvals/')
      ? { case: 'approval' as const, value: name }
      : { case: 'accountConnection' as const, value: name },
  },
  callPath: 'mail.send',
  providerLabel: 'Mail',
  deadlineAtUnixNanos: 902_000_000_000n,
})
const approvalWait = (id: string, startedAt: bigint, extra: Partial<Wait> = {}): Wait => ({
  id,
  startedAt,
  request: { kind: 'approval', id: `approval-${id}` },
  ...extra,
})
const connectionWait = (id: string, startedAt: bigint, extra: Partial<Wait> = {}): Wait => ({
  id,
  startedAt,
  request: { kind: 'accountConnection', id: `connection-${id}` },
  ...extra,
})
/** An emission of `run`, selected while live mode follows the Run. */
const followed = (extra: Record<string, string | undefined> = {}) =>
  request({ record: `${run}/events/3`, tree: '1', live: '1', ...extra })
/** The action that reviews `wait` on its tool. */
const target = (wait: Wait) => ({ run, record: waitName(wait.id), request: requestName(wait) })

/**
 * A Run whose tools wait on `waits`, listed in the order given. Its current requests
 * come in two action pages: the first holds the last wait, the second all the others.
 */
function waitingRun(waits: Wait[]): Handlers {
  return waitingRuns({ [run]: waits })
}
/** Several Runs, each read by its name, as `waitingRun` reads one. */
function waitingRuns(runs: Record<string, Wait[]>): Handlers {
  const all = Object.values(runs).flat()
  const ownerOf = (wait?: Wait) =>
    Object.keys(runs).find((name) => wait && runs[name]!.includes(wait)) ?? run
  const byName = (name: string) => all.find((wait) => requestName(wait) === name)
  const of = (name: string) => {
    const waits = runs[name] ?? []
    return {
      waits,
      current: [...new Set(waits.filter(waiting).map(requestName))],
      pages: Math.max(1, ...waits.map((wait) => (wait.page ?? 0) + 1)),
    }
  }
  return {
    accounts: {
      getAccountConnectionRequest: (input) => ({
        name: input.name,
        provider: `${workspace}/providers/mail`,
        state:
          byName(input.name)?.actionable === false
            ? AccountConnectionState.DECLINED
            : AccountConnectionState.PENDING,
      }),
    },
    activity: {
      inspectRun: (input) => {
        const { current } = of(input.name)
        return {
          run: {
            ...execution,
            name: input.name,
            actionPage: {
              actions: (input.actionAfter ? current.slice(0, -1) : current.slice(-1)).map(summary),
              total: BigInt(current.length),
              nextAction: input.actionAfter || current.length < 2 ? '' : 'older-actions',
            },
            requiresAction: current.length > 0,
          },
          submittedSource: 'return secretSource;',
          terminal: { case: 'succeeded', value: { finalValueJson: 'null' } },
        }
      },
      browseRunEntries: (input) => {
        const { waits, pages } = of(input.name)
        const page = Number(input.after) / 100
        return {
          run: { ...execution, name: input.name },
          offset: input.after,
          total: BigInt(pages * 100),
          entries: [
            {
              record: {
                case: 'event',
                value: { sequence: input.after + 1n, kind: 'program.logged' },
              },
            },
            ...waits
              .filter((wait) => (wait.page ?? 0) === page)
              .map((wait) => ({
                record: {
                  case: 'invocation' as const,
                  value: {
                    ...tool,
                    name: waitName(wait.id),
                    startedAtUnixNanos: wait.startedAt,
                    requiresAction: waiting(wait),
                  },
                },
              })),
          ],
          nextAfter: input.after + 100n,
          hasMore: page + 1 < pages,
        }
      },
      inspectInvocation: (input) => {
        const wait = all.find((item) => waitName(item.id) === input.name)
        return {
          invocation: {
            ...tool,
            run: ownerOf(wait),
            name: input.name,
            startedAtUnixNanos: wait?.startedAt,
            requiresAction: waiting(wait),
          },
          requests: wait ? [{ request: summary(requestName(wait)).request.request }] : [],
        }
      },
      inspectApproval: (input) => {
        const wait = byName(input.name)
        return {
          name: input.name,
          run: ownerOf(wait),
          invocation: wait ? waitName(wait.id) : '',
          state: wait?.decided ? 'approved' : 'pending',
          actionable: !wait?.decided && wait?.actionable !== false,
        }
      },
    },
  }
}

describe("Live's oldest action in the followed Run", () => {
  it('times every approval on every action page by its tool, then identity', async () => {
    // `b` alone is on the first action page; its equal-time tie `a` and the newer `c`
    // are on the second, and far past the first history page.
    const a = approvalWait('a', 5n, { page: 3 })
    const b = approvalWait('b', 5n)
    const c = approvalWait('c', 1_000n)
    const { api, calls } = coral(list(), runEvent(), waitingRun([c, a, b]))
    const page = await loadActivity(followed(), api, { scope })
    expect(page.liveAction).toEqual(target(a))
    // Each approval and its tool are read once; the Run's history is not.
    expect(loaded(calls, 'entries')).toBe(0)
    expect(loaded(calls, 'approval')).toBe(3)
    expect(loaded(calls, 'invocation')).toBe(3)
  })
  it.each(['latest', 'task'])('reads it while live mode follows the %s scope', async (live) => {
    const oldest = approvalWait('a', 1n)
    const { api } = coral(list(), runEvent(), waitingRun([approvalWait('b', 9n), oldest]))
    const page = await loadActivity(followed({ live }), api, { scope })
    expect(page.liveAction).toEqual(target(oldest))
  })
  it('reads the first action page when the review shows a later one', async () => {
    const oldest = approvalWait('a', 1n)
    const newer = approvalWait('b', 9n)
    // The first page holds `a`; the reader's `actionsAfter` page holds only `b`.
    const { api } = coral(list(), runEvent(), waitingRun([newer, oldest]))
    const page = await loadActivity(followed({ actionsAfter: 'older-actions' }), api, { scope })
    expect(page.liveAction).toEqual(target(oldest))
  })
  it('reads no tool of an approval you cannot decide, and no history', async () => {
    const waits = ['a', 'b', 'c'].map((id, index) =>
      approvalWait(id, BigInt(index), { actionable: false, page: 9 }),
    )
    const { api, calls } = coral(list(), runEvent(), waitingRun(waits))
    const page = await loadActivity(followed(), api, { scope })
    expect(page.liveAction).toBeUndefined()
    expect(loaded(calls, 'entries', 'invocation')).toBe(0)
  })
  it.each([
    { name: 'an earlier timestamp', early: 1n, approvalTime: 5n },
    { name: 'an equal timestamp with a lower identity', early: 10n, approvalTime: 10n },
  ])('times a shared connection across child pages by $name', async ({ early, approvalTime }) => {
    const first = connectionWait('z', 10n)
    const oldest = connectionWait('a', early, { page: 1, request: first.request })
    const other = approvalWait('m', approvalTime)
    const { api } = coral(list(), runEvent(), waitingRun([first, oldest, other]))
    const page = await loadActivity(followed(), api, { scope })
    expect(page.liveAction).toEqual({ ...target(oldest), record: run })
  })
  it.each([
    { name: 'an older connection wins', connected: 3n, approved: 4n, chosen: 'x' },
    { name: 'an older approval wins', connected: 5n, approved: 4n, chosen: 'y' },
  ])(
    'times a connection by the first tool that waits on it: $name',
    async ({ connected, approved, chosen }) => {
      const x = connectionWait('x', connected, { page: 1 })
      const y = approvalWait('y', approved)
      const { api, calls } = coral(list(), runEvent(), waitingRun([x, y]))
      const page = await loadActivity(followed(), api, { scope })
      expect(page.liveAction).toEqual(chosen === 'x' ? { ...target(x), record: run } : target(y))
      // Connection ordering reads every child page, including shared requests.
      expect(loaded(calls, 'entries')).toBe(2)
    },
  )
  it.each([
    {
      name: 'an approval you cannot decide',
      waits: [approvalWait('a', 1n, { actionable: false }), approvalWait('b', 2n)],
      chosen: 'b',
    },
    {
      name: 'a tool that no longer waits',
      waits: [approvalWait('a', 1n, { waiting: false }), approvalWait('b', 2n)],
      chosen: 'b',
    },
    {
      name: 'a connection that is no longer pending',
      waits: [connectionWait('a', 1n, { actionable: false }), connectionWait('b', 2n)],
      chosen: 'b',
    },
    {
      name: 'every request you cannot act on',
      waits: [
        approvalWait('a', 1n, { actionable: false }),
        connectionWait('b', 2n, { actionable: false }),
      ],
      chosen: undefined,
    },
  ])('skips $name', async ({ waits, chosen }) => {
    const { api } = coral(list(), runEvent(), waitingRun(waits))
    const page = await loadActivity(followed(), api, { scope })
    const wait = waits.find((item) => item.id === chosen)
    // An account connection is reviewed on its Run, an approval on its tool.
    expect(page.liveAction).toEqual(
      wait && {
        ...target(wait),
        record: wait.request.kind === 'approval' ? waitName(wait.id) : run,
      },
    )
  })
  it.each([
    { name: 'live mode is off', query: { live: '' }, waits: [approvalWait('a', 1n)] },
    { name: 'live names no scope', query: { live: 'on' }, waits: [approvalWait('a', 1n)] },
    { name: 'the Run waits on nothing', query: {}, waits: [] },
  ])('reads no children when $name', async ({ query, waits }) => {
    const { api, calls } = coral(list(), runEvent(), waitingRun(waits))
    const page = await loadActivity(followed(query), api, { scope })
    expect(page.liveAction).toBeUndefined()
    expect(loaded(calls, 'entries', 'invocation', 'approval', 'connection')).toBe(0)
  })
  it('reads the next action once the reviewed approval is decided, with live mode off', async () => {
    const decided = approvalWait('a', 1n, { decided: true })
    const next = approvalWait('b', 2n)
    const { api } = coral(list(), taskDetail(), waitingRun([decided, next]))
    const query = { record: waitName('a'), request: requestName(decided), tree: '1' }
    const page = await loadActivity(request(query), api, { scope })
    expect(page.detail).toMatchObject({ kind: 'invocation', request: { state: 'approved' } })
    expect(page.liveAction).toEqual(target(next))
    // A live snapshot of that review never reads it: only the read after the decision advances.
    const reading = { followRoot: false, roots: [], branches: {} }
    expect((await loadActivity(request(query), api, { scope, reading })).liveAction).toBeUndefined()
    // A pending review reads nothing more.
    const pending = coral(list(), taskDetail(), waitingRun([approvalWait('a', 1n), next]))
    const query2 = { ...query, request: `${workspace}/approvals/approval-a` }
    expect((await loadActivity(request(query2), pending.api, { scope })).liveAction).toBeUndefined()
    expect(loaded(pending.calls, 'entries')).toBe(0)
  })
})

const runOf = (id: string) => `${workspace}/runs/${id}`
interface TaskRun {
  id: string
  ended?: boolean
  waits?: Wait[]
  /** A Search that started no Run. */
  tool?: boolean
  /**
   * When its tool call started, in seconds after its Task, to the nanosecond. Its
   * position by default. A standalone Run's is when Coral accepted it, after `recent`.
   */
  at?: number
}
/** `count` standalone Runs that ended, named from `prefix`. */
const endedRoots = (count: number, prefix = 'gone'): LiveRoot[] =>
  Array.from({ length: count }, (_, index) => ({
    kind: 'run',
    run: { id: `${prefix}-${index}`, ended: true },
  }))
/** `count` Runs that ended, before the newer Runs the window holds. */
const endedRuns = (count: number) =>
  Array.from({ length: count }, (_, index) => ({ id: `old-${index}`, ended: true }))
const nameOf = (call: { input: unknown }) => (call.input as { name?: string }).name ?? ''
const actionOf = (id: string, wait: Wait) => ({
  run: runOf(id),
  record: wait.request.kind === 'approval' ? waitName(wait.id) : runOf(id),
  request: requestName(wait),
})

/** A listed root of `liveWorld`: a Task with its Runs in order, or a standalone Run. */
type LiveRoot =
  | {
      kind: 'task'
      id: string
      runs: TaskRun[]
      /** The Task's row says a Run of it runs. */
      partial?: boolean
      /** It started over an hour ago. */
      old?: boolean
      /** The list does not hold it. */
      unlisted?: boolean
    }
  | {
      kind: 'run'
      run: TaskRun
      /** The list's scope does not hold it, such as another Agent's Run. */
      unlisted?: boolean
    }
const taskName = (id: string) => `${workspace}/tasks/${id}`
/** Whether `item` runs and waits on you. */
const asks = (item: TaskRun) => !item.ended && (item.waits ?? []).some(waiting)
/**
 * Coral holding `roots`, newest first. Your queue lists each request once, under the
 * first Run that waits on it, or under `queued[request]` when given.
 */
function liveWorld(roots: LiveRoot[], queued: Record<string, string> = {}) {
  const tasks = roots.flatMap((root) => (root.kind === 'task' ? [root] : []))
  const all = roots.flatMap((root) =>
    root.kind === 'task'
      ? root.runs.map((item) => ({ item, task: taskName(root.id) }))
      : [{ item: root.run, task: '' }],
  )
  const recent = BigInt(Date.now()) * 1_000_000n
  /** `seconds` after `recent`, to the nanosecond. */
  const after = (seconds: number) => recent + BigInt(Math.round(seconds * 1e9))
  const runView = ({ item, task: owner }: (typeof all)[number]) => ({
    ...execution,
    ...(!owner && item.at !== undefined && { startedAtUnixNanos: after(item.at) }),
    name: runOf(item.id),
    task: owner,
    state: item.ended ? ActivityRunState.SUCCEEDED : ActivityRunState.RUNNING,
    requiresAction: asks(item),
    actionPage: undefined,
  })
  const queue = new Map<string, (typeof all)[number]>()
  for (const entry of all)
    if (asks(entry.item))
      for (const wait of (entry.item.waits ?? []).filter(waiting))
        if (!queue.has(requestName(wait))) queue.set(requestName(wait), entry)
  for (const [name, waiter] of Object.entries(queued))
    queue.set(
      name,
      all.find(({ item }) => runOf(item.id) === waiter)!,
    )
  const reads = waitingRuns(
    Object.fromEntries(all.map(({ item }) => [runOf(item.id), item.waits ?? []])),
  )
  /** When the record at `position` of the Task `root` started. */
  const childAt = (root: (typeof tasks)[number], position: number) =>
    root.old ? 1_000_000_000n : after(root.runs[position]!.at ?? position)
  const taskValue = (root: (typeof tasks)[number]) => ({
    ...goal,
    name: taskName(root.id),
    startedAtUnixNanos: root.old ? 1_000_000_000n : recent,
    contextImpactBytes: 1n,
    contextImpactPartial: root.partial ?? false,
  })
  return {
    ...reads,
    workspaces: {
      getWorkspace: (input: { name: string }) => ({
        name: input.name,
        state: WorkspaceState.ACTIVE,
      }),
    },
    activity: {
      ...reads.activity,
      browseRequiredActions: () => ({
        actions: [...queue].map(([name, entry]) => ({
          action: summary(name),
          run: runView(entry),
        })),
        actionCount: BigInt(queue.size),
      }),
      // A page of the listed roots after `after`, as Coral pages them.
      browseActivityRoots: (input: {
        after?: string
        pageSize?: number
        retained?: { names: string[] }
      }) => {
        const listed = roots.flatMap(
          (root): { name: string; record: { case: 'task' | 'run'; value: object } }[] =>
            root.unlisted
              ? []
              : root.kind === 'run'
                ? [
                    {
                      name: runOf(root.run.id),
                      record: { case: 'run', value: runView({ item: root.run, task: '' }) },
                    },
                  ]
                : [{ name: taskName(root.id), record: { case: 'task', value: taskValue(root) } }],
        )
        // As Coral does, a read of the records you watch says whether newer ones exist.
        if (input.retained) {
          const first = listed.findIndex(({ name }) => input.retained!.names.includes(name))
          return {
            roots: listed
              .filter(({ name }) => input.retained!.names.includes(name))
              .map(({ record }) => ({ record })),
            hasBefore: first > 0,
            hasAfter: false,
          }
        }
        const from = input.after ? listed.findIndex(({ name }) => name === input.after) + 1 : 0
        const size = input.pageSize || 50
        return {
          roots: listed.slice(from, from + size).map(({ record }) => ({ record })),
          hasAfter: from + size < listed.length,
        }
      },
      inspectTask: (input: { name: string; pageSize: number; viewport?: { offset: bigint } }) => {
        const root = tasks.find((item) => taskName(item.id) === input.name)!
        const total = root.runs.length
        const offset = Number(input.viewport?.offset ?? 0)
        // An offset at or past the end reads the last window, as Coral does.
        const from = offset >= total ? Math.max(0, total - input.pageSize) : offset
        return {
          task: taskValue(root),
          offset: BigInt(from),
          total: BigInt(total),
          interactions: root.runs.slice(from, from + input.pageSize).map((item, index) => ({
            name: `${input.name}/interactions/${item.id}`,
            position: BigInt(from + index),
            kind: item.tool ? 'search' : 'submit',
            intent: item.id,
            outcome: 'succeeded',
            startedAtUnixNanos: childAt(root, from + index),
            ...(!item.tool && {
              acceptedRun: runOf(item.id),
              run: runView({ item, task: input.name }),
            }),
          })),
          nextAfter: '',
        }
      },
    },
  } as Handlers
}
/** Page live mode on, with `values` in the address. */
const onLatest = (values: Record<string, string | string[]> = {}) =>
  request({ tree: '1', live: 'latest', ...values })
/** `liveLatest`, with each Run it names by its name only. */
const liveNames = (value: ActivityPage['liveLatest']) =>
  value && {
    ...value,
    ...(value.running && {
      running: Object.fromEntries(
        Object.entries(value.running).map(([key, row]) => [key, row.name]),
      ),
    }),
  }
const actionIn = (owner: string, id: string, wait: Wait) => ({
  task: owner && taskName(owner),
  action: actionOf(id, wait),
})

/** The windows of the Task `owner` that `calls` read in order: `head` for its first record. */
const windowsOf = (calls: { method: string; input: unknown }[], owner: string) =>
  calls
    .filter((call) => call.method === 'task' && nameOf(call) === owner)
    .map((call) => {
      const offset = (call.input as { viewport?: { offset: bigint } }).viewport?.offset
      return offset === undefined ? 'head' : String(offset)
    })
/** Each Task whose children `calls` read, and how many children each read asked for. */
const childReads = (calls: { method: string; input: unknown }[]) =>
  calls
    .filter((call) => call.method === 'task')
    .map((call) => [nameOf(call), (call.input as { pageSize: number }).pageSize])
/** `world`, where Reef cannot read the window of the Task `owner` from `offset`. */
function refusing(world: Handlers, owner: string, offset: number): Handlers {
  const read = world.activity!.inspectTask as (input: unknown) => never
  return {
    activity: {
      inspectTask: (input: { name: string; viewport?: { offset: bigint } }) =>
        input.name === owner && input.viewport?.offset === BigInt(offset)
          ? refuse('task unavailable')
          : read(input),
    },
  }
}

describe("Page live's projection past the list", () => {
  const early = approvalWait('early', 2n)
  const late = approvalWait('late', 9n)
  // A Task with no Run that runs, newer than everything else.
  const idle: LiveRoot = { kind: 'task', id: 'idle', runs: [{ id: 'idle-run', ended: true }] }
  it.each<{ name: string; roots: LiveRoot[]; chosen: ReturnType<typeof actionIn> }>([
    {
      name: 'the tool that started waiting first, across Tasks',
      roots: [
        idle,
        { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
        { kind: 'task', id: 'o', partial: true, runs: [{ id: 'a', waits: [early] }] },
      ],
      chosen: actionIn('o', 'a', early),
    },
    {
      name: 'a standalone Run that waits first',
      roots: [
        { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
        { kind: 'run', run: { id: 's', waits: [early] } },
      ],
      chosen: actionIn('', 's', early),
    },
    {
      name: 'only a Task under an hour old',
      roots: [
        { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
        { kind: 'task', id: 'o', partial: true, old: true, runs: [{ id: 'a', waits: [early] }] },
      ],
      chosen: actionIn('n', 'b', late),
    },
  ])('names $name', async ({ roots, chosen }) => {
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.action).toEqual(chosen)
  })
  // Your queue lists a shared connection once, under `x` of a Task over an hour old.
  // `y`, of the Task `n`, waits on it too, before 150 Runs that ended and then `later`.
  const shared = connectionWait('x', 5n)
  const also = { ...connectionWait('y', 3n), request: shared.request }
  const sharing = (later: TaskRun[]): LiveRoot[] => [
    idle,
    {
      kind: 'task',
      id: 'n',
      partial: true,
      runs: [{ id: 'y', waits: [also] }, ...endedRuns(150), ...later],
    },
    { kind: 'task', id: 'o', partial: true, old: true, runs: [{ id: 'x', waits: [shared] }] },
  ]
  const queued = { [requestName(shared)]: runOf('x') }
  it('names a Run that waits on a connection your queue lists under another Task, reading each window once', async () => {
    // `z` runs, submitted last.
    const { api, calls } = coral(liveWorld(sharing([{ id: 'z' }]), queued))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.action).toEqual(actionIn('n', 'y', also))
    expect(liveNames(page.liveLatest)?.running).toEqual({ [taskName('n')]: runOf('z') })
    // Its first record, then its 152 Runs from the end, 100 at a time, each once.
    expect(windowsOf(calls, taskName('n'))).toEqual(['head', '52', '0'])
  })
  it.each<{
    name: string
    roots: LiveRoot[]
    queued?: Record<string, string>
    refused: [task: string, offset: number]
    named: ReturnType<typeof liveNames>
    windows: Record<string, string[]>
  }>([
    {
      name: 'a window of a Task it looks in for its newest Run that runs',
      roots: [
        { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b' }] },
        { kind: 'task', id: 'm', partial: true, runs: [{ id: 'c' }] },
      ],
      refused: [taskName('m'), 0],
      named: { running: { [taskName('n')]: runOf('b') } },
      windows: { [taskName('n')]: ['head', '0'], [taskName('m')]: ['head', '0'] },
    },
    {
      name: 'a window of a Task it reads in full while your queue holds a connection',
      roots: sharing([]),
      queued,
      refused: [taskName('n'), 0],
      named: {},
      windows: { [taskName('n')]: ['head', '51', '0'] },
    },
    {
      name: 'an older window of a Task, after it found its newest Run that runs',
      roots: sharing([{ id: 'z' }]),
      queued,
      refused: [taskName('n'), 0],
      named: { running: { [taskName('n')]: runOf('z') } },
      windows: { [taskName('n')]: ['head', '52', '0'] },
    },
  ])('never says it read every record when Reef cannot read $name', async (item) => {
    const world = liveWorld(item.roots, item.queued)
    const { api, calls } = coral(world, refusing(world, ...item.refused))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(liveNames(page.liveLatest)).toEqual(item.named)
    // Each window once: a window Reef could not read is not read again.
    for (const [owner, windows] of Object.entries(item.windows))
      expect(windowsOf(calls, owner), owner).toEqual(windows)
  })
  it('names an older Run that runs before the loaded window of an older Task', async () => {
    const roots: LiveRoot[] = [
      idle,
      { kind: 'task', id: 'o', partial: true, runs: [{ id: 'a' }, ...endedRuns(250)] },
    ]
    const o = taskName('o')
    const window = { expand: o, [`offset.${o}`]: '152', [`preceding.${o}`]: '0' }
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(window), api, { scope })
    expect(page.branches[o]?.rows.some((row) => row.name === runOf('a'))).toBe(false)
    expect(liveNames(page.liveLatest)).toEqual({ running: { [o]: runOf('a') }, complete: true })
  })
  it('reads past a Task whose row says a Run runs when none does', async () => {
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', ended: true }] },
      { kind: 'task', id: 'o', partial: true, runs: [{ id: 'a' }] },
    ]
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(liveNames(page.liveLatest)).toEqual({
      running: { [taskName('o')]: runOf('a') },
      complete: true,
    })
  })
  it('names the newest Run that runs in every Task whose row says one runs, whatever is newer', async () => {
    const roots: LiveRoot[] = [
      { kind: 'run', run: { id: 's' } },
      { kind: 'task', id: 'n', runs: [{ id: 'quiet', ended: true }] },
      { kind: 'task', id: 'm', partial: true, runs: [{ id: 'c' }, { id: 'd' }] },
      { kind: 'task', id: 'o', partial: true, runs: [{ id: 'a' }] },
    ]
    const { api, calls } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(liveNames(page.liveLatest)).toEqual({
      running: {
        [runOf('s')]: runOf('s'),
        [taskName('m')]: runOf('d'),
        [taskName('o')]: runOf('a'),
      },
      complete: true,
    })
    // A Task whose row says no Run of it runs is never read.
    expect(calls.filter((call) => nameOf(call) === taskName('n'))).toEqual([])
  })
  it('reads the list, not the Task you have a Search of open, when the list does not hold it', async () => {
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'gone', unlisted: true, runs: [{ id: 'g' }] },
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b' }] },
    ]
    const taskTool: Handlers = {
      activity: {
        inspectTaskInteraction: () => ({
          interaction: {
            name: `${taskName('gone')}/interactions/search`,
            kind: 'search',
            intent: 'find',
            outcome: 'succeeded',
          },
        }),
      },
    }
    const { api, calls } = coral(liveWorld(roots), taskTool)
    const record = `${taskName('gone')}/interactions/search`
    const page = await loadActivity(onLatest({ record }), api, { scope })
    expect(liveNames(page.liveLatest)).toEqual({
      running: { [taskName('n')]: runOf('b') },
      complete: true,
    })
    expect(calls.filter((call) => nameOf(call) === taskName('gone'))).toEqual([])
  })
  // The list loads its newest 25 records. Live mode reads every older page too.
  it('names a Run that runs in a Task past the loaded list', async () => {
    const roots: LiveRoot[] = [
      ...endedRoots(130),
      { kind: 'task', id: 'far', partial: true, runs: [{ id: 'a' }] },
    ]
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.rows.some((row) => row.name === taskName('far'))).toBe(false)
    expect(liveNames(page.liveLatest)).toEqual({
      running: { [taskName('far')]: runOf('a') },
      complete: true,
    })
  })
  it('names a standalone Run that runs past the loaded list, whatever its age', async () => {
    // The fixtures' Runs started in 1970.
    const roots: LiveRoot[] = [...endedRoots(130), { kind: 'run', run: { id: 'far' } }]
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.rows.some((row) => row.name === runOf('far'))).toBe(false)
    expect(liveNames(page.liveLatest)).toEqual({
      running: { [runOf('far')]: runOf('far') },
      complete: true,
    })
  })
  it('names a standalone Run that waits past the loaded list', async () => {
    const roots: LiveRoot[] = [
      ...endedRoots(30),
      { kind: 'run', run: { id: 'far', waits: [early] } },
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
    ]
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.action).toEqual(actionIn('', 'far', early))
  })
  it('never names a Run from your queue that the list does not hold', async () => {
    const roots: LiveRoot[] = [
      { kind: 'run', run: { id: 'other', waits: [early] }, unlisted: true },
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
    ]
    const { api, calls } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.action).toEqual(actionIn('n', 'b', late))
    expect(calls.filter((call) => nameOf(call) === runOf('other'))).toEqual([])
  })
  it('keeps the loaded list as it pages, and reads each older page once', async () => {
    const world = liveWorld(endedRoots(230))
    const off = await loadActivity(request({ tree: '1' }), coral(world).api, { scope })
    const { api, calls } = coral(world)
    const page = await loadActivity(onLatest(), api, { scope })
    // The list and its pages are as without live mode.
    expect(page.rows).toEqual(off.rows)
    expect(page.hasAfter).toBe(off.hasAfter)
    // Each older read starts after the last record the one before it read, once.
    const afters = calls
      .filter((call) => call.method === 'roots')
      .map((call) => (call.input as { after?: string }).after ?? '')
      .slice(1)
    expect(afters[0]).toBe(page.rows.at(-1)?.name)
    expect(new Set(afters).size).toBe(afters.length)
    // Nothing runs anywhere: it says so, so no row of the list can lag a Run's end. The
    // newest record is the newest standalone Run, which ended.
    expect(page.liveLatest).toEqual({
      latest: { root: runOf('gone-0'), record: runOf('gone-0'), run: true },
      complete: true,
    })
  })
  // A snapshot of only the records you watch, `w`: Coral says when newer ones exist.
  const watched: LiveRoot = { kind: 'task', id: 'w', runs: [{ id: 'w1', ended: true }] }
  it.each<{ name: string; roots: LiveRoot[]; named: ReturnType<typeof liveNames> }>([
    {
      name: 'a newer Task',
      roots: [{ kind: 'task', id: 'fresh', runs: [] }, watched],
      named: {
        newest: taskName('fresh'),
        latest: { root: taskName('fresh'), record: taskName('fresh'), run: false },
        complete: true,
      },
    },
    {
      name: 'every standalone Run that runs, newer or not',
      roots: [
        { kind: 'run', run: { id: 'runs' } },
        { kind: 'run', run: { id: 'over', ended: true } },
        watched,
      ],
      named: { running: { [runOf('runs')]: runOf('runs') }, newest: runOf('runs'), complete: true },
    },
    {
      name: 'the newest record, when nothing runs',
      roots: [watched, { kind: 'run', run: { id: 'over', ended: true } }],
      named: {
        latest: { root: taskName('w'), record: runOf('w1'), run: true },
        complete: true,
      },
    },
  ])('names $name when the snapshot holds only the records you watch', async (item) => {
    const { api } = coral(liveWorld(item.roots))
    const reading = { roots: [taskName('w')], followRoot: false, branches: {} }
    const page = await loadActivity(onLatest(), api, { scope, reading })
    expect(page.rows.map((row) => row.name)).toEqual([taskName('w')])
    expect(liveNames(page.liveLatest)).toEqual(item.named)
  })
  it('keeps what it read when Reef cannot read an older page', async () => {
    const roots: LiveRoot[] = [
      { kind: 'run', run: { id: 'near' } },
      ...endedRoots(30),
      { kind: 'run', run: { id: 'far' } },
    ]
    const world = liveWorld(roots)
    const browse = world.activity!.browseActivityRoots as (input: { after?: string }) => never
    const refused: Handlers = {
      activity: {
        browseActivityRoots: (input: { after?: string }) =>
          input.after ? refuse('roots unavailable') : browse(input),
      },
    }
    const off = await loadActivity(request({ tree: '1' }), coral(world).api, { scope })
    const { api } = coral(world, refused)
    const page = await loadActivity(onLatest(), api, { scope })
    // The list stays as without live mode; only what lies past it is unknown.
    expect(page.listUnavailable).toBe(false)
    expect(page.rows).toEqual(off.rows)
    // It names the Run it read, and never says it read every record.
    expect(liveNames(page.liveLatest)).toEqual({ running: { [runOf('near')]: runOf('near') } })
  })
  it.each<{ name: string; query: Record<string, string> }>([
    { name: 'live mode follows a Task', query: { live: 'task' } },
    { name: 'live mode is off', query: { live: '' } },
    { name: 'you have a Run that runs open', query: { record: `${runOf('b')}/events/3` } },
  ])('reads nothing when $name', async ({ query }) => {
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
    ]
    const { api, calls } = coral(liveWorld(roots), runEvent())
    const page = await loadActivity(onLatest(query), api, { scope })
    expect(page.liveLatest).toBeUndefined()
    expect(calls.filter((call) => [runOf('b'), taskName('n')].includes(nameOf(call)))).toEqual(
      query.record ? [expect.objectContaining({ method: 'run' })] : [],
    )
  })
  // The list orders Tasks by when they started, not by their newest record. With nothing
  // that runs, live mode's newest record is the one that started last across them.
  // Catches: live mode returning to a newer Task's older record once another Task's Run ended.
  const search = (owner: string, id: string) => `${taskName(owner)}/interactions/${id}`
  it.each<{ name: string; roots: LiveRoot[]; latest: NonNullable<LiveLatest['latest']> }>([
    {
      name: "an older Task's Run that started after a newer Task's Search",
      roots: [
        { kind: 'task', id: 'newer', runs: [{ id: 's', tool: true, ended: true, at: 1 }] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 2 }] },
      ],
      latest: { root: taskName('older'), record: runOf('r'), run: true },
    },
    {
      name: "a newer Task's Search that started after an older Task's Run",
      roots: [
        { kind: 'task', id: 'newer', runs: [{ id: 's', tool: true, ended: true, at: 3 }] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 2 }] },
      ],
      latest: { root: taskName('newer'), record: search('newer', 's'), run: false },
    },
    {
      name: 'the newer Task, at an equal time',
      roots: [
        { kind: 'task', id: 'newer', runs: [{ id: 's', tool: true, ended: true, at: 2 }] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 2 }] },
      ],
      latest: { root: taskName('newer'), record: search('newer', 's'), run: false },
    },
    {
      // Both started in the same millisecond, 800 ns apart.
      name: "an older Task's Run that started in the same millisecond, but later",
      roots: [
        { kind: 'task', id: 'newer', runs: [{ id: 's', tool: true, ended: true, at: 1.0000001 }] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 1.0000009 }] },
      ],
      latest: { root: taskName('older'), record: runOf('r'), run: true },
    },
    {
      // A Task's Run by when its submitting call started, which Coral orders the Task's
      // records by, a standalone Run by when Coral accepted it, which orders the list.
      name: "a standalone Run accepted after the call that submitted a Task's Run",
      roots: [
        { kind: 'run', run: { id: 'alone', ended: true, at: 2.5 } },
        { kind: 'task', id: 'work', runs: [{ id: 'r', ended: true, at: 2 }] },
      ],
      latest: { root: runOf('alone'), record: runOf('alone'), run: true },
    },
    {
      name: 'a Task with no record, newer than a record that started with it',
      roots: [
        { kind: 'task', id: 'fresh', runs: [] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 0 }] },
      ],
      latest: { root: taskName('fresh'), record: taskName('fresh'), run: false },
    },
    {
      name: 'a standalone Run of any age, past a Task over an hour old',
      roots: [
        { kind: 'task', id: 'stale', old: true, runs: [{ id: 'x', ended: true }] },
        { kind: 'run', run: { id: 'alone', ended: true } },
      ],
      latest: { root: runOf('alone'), record: runOf('alone'), run: true },
    },
    {
      name: 'a Search of a Task past the loaded list',
      roots: [
        ...endedRoots(130),
        { kind: 'task', id: 'far', runs: [{ id: 'f', tool: true, ended: true }] },
      ],
      latest: { root: taskName('far'), record: search('far', 'f'), run: false },
    },
  ])('names as the newest record $name', async ({ roots, latest }) => {
    const { api } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.latest).toEqual(latest)
  })
  // `b`, whose last Run started last, then 150 idle recent Tasks.
  const busy: LiveRoot[] = [
    {
      kind: 'task',
      id: 'b',
      runs: [
        { id: 'b1', ended: true, at: 1 },
        { id: 'b2', ended: true, at: 3 },
      ],
    },
    ...Array.from({ length: 150 }, (_, index): LiveRoot => ({
      kind: 'task',
      id: `idle${index}`,
      runs: [{ id: `i${index}`, ended: true, at: 1 }],
    })),
  ]
  const recentTasks = busy.map((root) => taskName((root as { id: string }).id))
  // `b` first, in the loaded list, or last, two pages past it.
  // Catches: finding the newest record only among the Tasks the list loaded, or reading
  // a Task's children more than once in a load.
  it.each<[string, LiveRoot[]]>([
    ['in the loaded list', busy],
    ['past the loaded list', [...busy.slice(1), busy[0]!]],
  ])('reads the newest child of each recent Task once, with `b` %s', async (_, roots) => {
    const { api, calls } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.latest).toEqual({ root: taskName('b'), record: runOf('b2'), run: true })
    expect(childReads(calls).toSorted()).toEqual(recentTasks.map((name) => [name, 1]).toSorted())
  })
  // Catches: reading again the newest child a window of this load already holds.
  it('reads no child of a Task when a window of this load holds its newest one', async () => {
    const world = liveWorld(busy)
    const expand = { expand: taskName('b') }
    const off = coral(world)
    await loadActivity(request({ tree: '1', ...expand }), off.api, { scope })
    const { api, calls } = coral(world)
    const page = await loadActivity(onLatest(expand), api, { scope })
    expect(page.liveLatest?.latest).toEqual({ root: taskName('b'), record: runOf('b2'), run: true })
    const ofB = (reads: unknown[][]) => reads.filter(([name]) => name === taskName('b'))
    expect(ofB(childReads(calls))).toEqual(ofB(childReads(off.calls)))
    expect(childReads(calls).filter(([name]) => name !== taskName('b'))).toHaveLength(150)
  })
  // Catches: reading children for a newest record that a Run that runs makes moot.
  it('reads no newest record while a Run runs', async () => {
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'idle', runs: [{ id: 'i', ended: true }] },
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b' }] },
    ]
    const { api, calls } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.latest).toBeUndefined()
    expect(windowsOf(calls, taskName('idle'))).toEqual([])
  })
  // Catches: reading again the newest child that Live's own read of a Task's Runs holds.
  it('reuses a read of this snapshot that holds the newest child of a Task', async () => {
    // The Task's row lags its Run's end, so live mode reads its Runs from the end.
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', ended: true }] },
    ]
    const { api, calls } = coral(liveWorld(roots))
    const page = await loadActivity(onLatest(), api, { scope })
    expect(page.liveLatest?.latest).toEqual({ root: taskName('n'), record: runOf('b'), run: true })
    expect(windowsOf(calls, taskName('n'))).toEqual(['head', '0'])
  })
  // `older` holds the newest record, a Run that started after `newer`'s Search. Any Task
  // Reef cannot read could hold a newer one.
  // Catches: naming a newest record past a Task Reef could not read.
  it.each<[string]>([['older'], ['newer']])(
    'names no newest record when Reef cannot read the newest child of `%s`',
    async (refused) => {
      const roots: LiveRoot[] = [
        { kind: 'task', id: 'newer', runs: [{ id: 's', tool: true, ended: true, at: 1 }] },
        { kind: 'task', id: 'older', runs: [{ id: 'r', ended: true, at: 2 }] },
      ]
      const world = liveWorld(roots)
      const { api } = coral(world, refusing(world, taskName(refused), Number.MAX_SAFE_INTEGER))
      const page = await loadActivity(onLatest(), api, { scope })
      expect(page.liveLatest).toEqual({ complete: true })
    },
  )
  it('reads no action in an archived workspace', async () => {
    const roots: LiveRoot[] = [
      { kind: 'task', id: 'n', partial: true, runs: [{ id: 'b', waits: [late] }] },
    ]
    const archived: Handlers = {
      workspaces: {
        getWorkspace: (input: { name: string }) => ({
          name: input.name,
          state: WorkspaceState.ARCHIVED,
        }),
      },
    }
    const { api, calls } = coral(liveWorld(roots), archived)
    const page = await loadActivity(onLatest(), api, { scope })
    expect(liveNames(page.liveLatest)).toEqual({
      running: { [taskName('n')]: runOf('b') },
      complete: true,
    })
    // It reads no request: only the pending panel reads your queue.
    expect(calls.filter((call) => ['run', 'approval'].includes(call.method))).toEqual([])
    expect(loaded(calls, 'actions')).toBe(1)
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

// Pending actions as Coral pages them, for the selected Run.
const approvalAt = (index: number) => ({
  request: {
    request: { case: 'approval' as const, value: `${workspace}/approvals/action-${index}` },
  },
  callPath: `coral.providers.mail.send${index}`,
  providerLabel: 'Mail',
  deadlineAtUnixNanos: 902_000_000_000n,
})
const connectionAt = (index: number) => ({
  request: {
    request: {
      case: 'accountConnection' as const,
      value: `${workspace}/accountConnections/action-${index}`,
    },
  },
  callPath: '',
  providerLabel: 'Linear',
  deadlineAtUnixNanos: 902_000_000_000n,
})
const range = (from: number, to: number) =>
  Array.from({ length: to - from }, (_, offset) => from + offset)
/** Coral's action pages for the Run, keyed by the cursor that reads each; `fail` refuses one. */
function pagedRun(
  pages: Record<string, { actions: ReturnType<typeof approvalAt>[]; next: string }>,
  total: bigint,
  fail?: string,
): Handlers {
  return {
    activity: {
      inspectRun: (input) => {
        if (input.actionAfter === fail) refuse('action page unavailable', Code.Unavailable)
        const page = pages[input.actionAfter ?? '']
        return {
          run: {
            ...execution,
            actionPage: { actions: page.actions, total, nextAction: page.next },
          },
          submittedSource: 'return null;',
          linkedFunctions: [],
          events: [],
        }
      },
    },
  }
}
const names = (page: ActivityPage) =>
  page.detail?.kind === 'run' ? page.detail.run.actionPage!.actions.map((item) => item.name) : []
/** The action cursor and Event window of each Run read. */
const runReads = (calls: { method: string; input: unknown }[]) =>
  calls
    .filter((call) => call.method === 'run')
    .map((call) => {
      const { actionAfter, pageSize } = call.input as { actionAfter: string; pageSize: number }
      return { actionAfter, pageSize }
    })
describe('the pending actions of the selected Run', () => {
  // Two full pages of 100, approvals and connections mixed, then a short one.
  const pages = {
    '': { actions: range(0, 100).map(approvalAt), next: 'cursor-1' },
    'cursor-1': {
      actions: range(100, 200).map((index) =>
        index % 3 ? approvalAt(index) : connectionAt(index),
      ),
      next: 'cursor-2',
    },
    'cursor-2': { actions: [approvalAt(200), approvalAt(201)], next: '' },
  } as Record<string, { actions: ReturnType<typeof approvalAt>[]; next: string }>

  // Catches: a load that walks every page of a Run's actions.
  it.each([
    ['no action', 0],
    ['one action', 1],
    ['five actions', 5],
    ['six actions', 6],
    ['many actions', 202],
  ])('reads only the first page for %s', async (_, total) => {
    const first = total > 100 ? pages[''] : { actions: range(0, total).map(approvalAt), next: '' }
    const { api, calls } = coral(list(), taskDetail(), pagedRun({ '': first }, BigInt(total)))
    const page = await loadActivity(request({ record: run }), api, { scope })
    // One read, at the first page, with the smallest Event window Coral accepts.
    expect(runReads(calls)).toEqual([{ actionAfter: '', pageSize: 1 }])
    expect(names(page)).toEqual(first.actions.map((item) => item.request.request.value))
    expect(page.detail).toMatchObject({
      kind: 'run',
      run: { actionPage: { total: String(total), next: first.next } },
    })
    const actionPage = page.detail?.kind === 'run' ? page.detail.run.actionPage! : undefined
    expect(actionPage?.incomplete).toBeFalsy()
    expect(actionPage?.after).toBeUndefined()
  })

  // Catches: a "More actions" link that reads every later page, or none.
  it('reads the one later page that an explicit "More actions" link asks for', async () => {
    const { api, calls } = coral(list(), taskDetail(), pagedRun(pages, 202n))
    const page = await loadActivity(request({ record: run, actionsAfter: 'cursor-1' }), api, {
      scope,
    })
    expect(runReads(calls)).toEqual([
      { actionAfter: '', pageSize: 1 },
      { actionAfter: 'cursor-1', pageSize: 1 },
    ])
    expect(names(page)).toEqual(pages['cursor-1'].actions.map((item) => item.request.request.value))
    expect(page.detail).toMatchObject({
      kind: 'run',
      run: { actionPage: { total: '202', next: 'cursor-2', after: 'cursor-1' } },
    })
    // A later page does not open its first connection for you.
    expect(page.initialRequest).toBeUndefined()
  })

  // Catches: a failed later page that empties the list, or claims it is whole.
  it('keeps the first page and says the list is incomplete when the later page fails', async () => {
    const { api, calls } = coral(list(), taskDetail(), pagedRun(pages, 202n, 'cursor-1'))
    const page = await loadActivity(request({ record: run, actionsAfter: 'cursor-1' }), api, {
      scope,
    })
    expect(runReads(calls)).toHaveLength(2)
    expect(names(page)).toEqual(pages[''].actions.map((item) => item.request.request.value))
    expect(page.detail).toMatchObject({
      kind: 'run',
      run: { actionPage: { total: '202', next: 'cursor-1', incomplete: true } },
    })
  })
})

describe('a request reviewed from a later page of the Run actions', () => {
  // The selected approval and connection wait on the Run's second page of actions.
  const laterPages = {
    '': { actions: range(0, 100).map(approvalAt), next: 'cursor-1' },
    'cursor-1': {
      actions: [
        {
          request: { request: { case: 'approval' as const, value: approval } },
          callPath: 'coral.providers.mail.send',
          providerLabel: 'Mail',
          deadlineAtUnixNanos: 902_000_000_000n,
        },
        {
          request: { request: { case: 'accountConnection' as const, value: connection } },
          callPath: '',
          providerLabel: 'Linear',
          deadlineAtUnixNanos: 902_000_000_000n,
        },
      ],
      next: '',
    },
  } as Record<string, { actions: ReturnType<typeof approvalAt>[]; next: string }>
  const later = laterPages['cursor-1'].actions.map((item) => item.request.request.value)

  // Catches: a review that drops the page it was opened from, so its Run lists the first page
  // under it and a connection loses its provider's name.
  it.each([
    ['its Run', run],
    ['its tool', invocation],
  ])('keeps the page an approval opened from %s', async (_label, record) => {
    const { api, calls } = coral(
      list(),
      taskDetail(),
      runEntries(),
      invocationDetail(),
      approvalDetail(),
      pagedRun(laterPages, 102n),
    )
    const page = await loadActivity(
      request({ record, request: approval, actionsAfter: 'cursor-1', tree: '1' }),
      api,
      { scope },
    )
    // The Run and the first page once, then the one page the link names.
    expect(runReads(calls)).toEqual([
      { actionAfter: '', pageSize: 1 },
      { actionAfter: 'cursor-1', pageSize: 1 },
    ])
    expect(calls.filter((call) => call.method === 'approval').map((call) => call.input)).toEqual([
      expect.objectContaining({ name: approval }),
    ])
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      requestUnavailable: false,
      request: { kind: 'approval', name: approval, run },
      context: {
        run: { actionPage: { total: '102', next: '', after: 'cursor-1' } },
      },
    })
    const context = page.detail?.kind === 'invocation' ? page.detail.context : undefined
    expect(context?.run.actionPage?.actions.map((item) => item.name)).toEqual(later)
  })

  it('keeps the page a connection opened from, with its provider', async () => {
    const { api, calls } = coral(
      list(),
      taskDetail(),
      runEntries(),
      connectionRequest(),
      pagedRun(laterPages, 102n),
    )
    const page = await loadActivity(
      request({ record: run, request: connection, actionsAfter: 'cursor-1' }),
      api,
      { scope },
    )
    expect(runReads(calls)).toEqual([
      { actionAfter: '', pageSize: 1 },
      { actionAfter: 'cursor-1', pageSize: 1 },
    ])
    expect(calls.filter((call) => call.method === 'connection').map((call) => call.input)).toEqual([
      expect.objectContaining({ name: connection }),
    ])
    expect(page.detail).toMatchObject({
      kind: 'run',
      requestUnavailable: false,
      request: { kind: 'account_connection', name: connection },
      run: { actionPage: { after: 'cursor-1' } },
    })
    const actions = page.detail?.kind === 'run' ? page.detail.run.actionPage!.actions : []
    expect(actions.find((item) => item.name === connection)).toMatchObject({
      kind: 'account_connection',
      providerLabel: 'Linear',
    })
    // A later page never swaps in another request for the one the link names.
    expect(page.initialRequest).toBeUndefined()
  })

  // Catches: a failed page of context that takes the exact review down with it.
  it('keeps the review readable on the first page when its later page fails', async () => {
    const { api } = coral(
      list(),
      taskDetail(),
      runEntries(),
      invocationDetail(),
      approvalDetail(),
      pagedRun(laterPages, 102n, 'cursor-1'),
    )
    const page = await loadActivity(
      request({ record: run, request: approval, actionsAfter: 'cursor-1' }),
      api,
      { scope },
    )
    expect(page.detail).toMatchObject({
      kind: 'invocation',
      request: { kind: 'approval', name: approval },
      context: { run: { actionPage: { next: 'cursor-1', incomplete: true } } },
    })
  })
})
