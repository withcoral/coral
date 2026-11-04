import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  ActivityInvocationOutcome,
  ActivityInvocationState,
  ActivityRunState,
} from '@/generated/coral/management/v1/activity_pb'
import { fakeManagement, type ManagementHandlers } from './management.test-support'
import { loadActivity, loadRunActivity } from './activity.server'

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const scope = { workspace }
const task = `${workspace}/tasks/plain-amber-river-stone`
const run = `${workspace}/runs/quiet-blue-harbor-light`
const standalone = `${workspace}/runs/brave-green-meadow-path`
const call = `${workspace}/invocations/swift-red-falcon-wing`
const s = (seconds: number) => BigInt(seconds * 1_000_000_000)

const goal = {
  name: task,
  workspace,
  intent: 'Find overdue invoices',
  outcome: 'open',
  actor: 'user',
  startedAtUnixNanos: s(10),
}
const runOf = (name: string, owner: string) => ({
  name,
  workspace,
  task: owner,
  actor: 'user',
  intent: owner ? 'Send reminders' : 'Export paid invoices',
  state: ActivityRunState.SUCCEEDED,
  startedAtUnixNanos: s(owner ? 12 : 5),
  endedAtUnixNanos: s(20),
})
const invocation = {
  name: call,
  workspace,
  run,
  task,
  actor: 'user',
  tool: 'send',
  callPath: 'coral.providers.mail.send',
  state: ActivityInvocationState.ENDED,
  startedAtUnixNanos: s(13),
  endedAtUnixNanos: s(14),
}

/**
 * An in-memory Coral with two roots: the Task (newest), whose one Run made one call, and an
 * older standalone Run. `refused` names records whose reads Coral refuses; `reads` lists what
 * the trace read.
 */
function coral({ refused = [] as string[], code = Code.NotFound, callFailed = false } = {}) {
  // A failed call names its outcome; its own read gives the error and what it waited for.
  const served = callFailed
    ? { ...invocation, outcome: ActivityInvocationOutcome.CORAL_FAILURE }
    : invocation
  const reads: string[] = []
  const refuse = (name: string) => {
    if (refused.includes(name)) throw new ConnectError('unavailable', code)
  }
  const handlers: ManagementHandlers = {
    activity: {
      browseActivityRoots: () => ({
        roots: [
          { record: { case: 'task' as const, value: goal } },
          { record: { case: 'run' as const, value: runOf(standalone, '') } },
        ],
      }),
      inspectTask: (input) => {
        refuse(input.name)
        if (input.pageSize > 1) reads.push(input.name)
        return {
          task: goal,
          interactions: [
            {
              name: `${task}/interactions/submit`,
              kind: 'submit',
              intent: 'Send reminders',
              outcome: 'succeeded',
              startedAtUnixNanos: s(12),
              acceptedRun: run,
              run: runOf(run, task),
            },
          ],
          total: 1n,
        }
      },
      browseRunEntries: (input) => {
        refuse(input.name)
        reads.push(input.name)
        return {
          run: runOf(input.name, input.name === run ? task : ''),
          entries:
            input.name === run ? [{ record: { case: 'invocation' as const, value: served } }] : [],
          total: input.name === run ? 1n : 0n,
        }
      },
      inspectRun: (input) => {
        refuse(input.name)
        return { run: runOf(input.name, input.name === run ? task : '') }
      },
      inspectInvocation: () => ({
        invocation: served,
        ...(callFailed && {
          outcome: {
            case: 'coralFailure' as const,
            value: { code: 'upstream_rejected', message: 'The provider refused the bearer token.' },
          },
          requests: [{ request: { case: 'approval' as const, value: `${workspace}/approvals/a` } }],
        }),
      }),
      inspectRunEvent: (input) => ({
        event: { sequence: input.sequence, kind: 'program.emitted', occurredAtUnixNanos: s(15) },
        run: runOf(input.run, input.run === run ? task : ''),
      }),
    },
  }
  return { reads, api: fakeManagement(handlers) }
}

/** A call that failed in Coral, admitted at `from` and dispatched at `dispatched`. */
const failedCall = (name: string, from: number, dispatched: number) => ({
  ...invocation,
  name,
  outcome: ActivityInvocationOutcome.CORAL_FAILURE,
  startedAtUnixNanos: s(from),
  dispatchedAtUnixNanos: s(dispatched),
  endedAtUnixNanos: s(dispatched + 1),
})

const at = (search: string) => new Request(`https://reef.test/${workspace}/activity${search}`)

describe('the Activity page root', () => {
  // Catches: the page opening the wrong root when the address names more than one. Which root
  // a record belongs to is the navigation tests' rule; here only the order counts.
  it.each([
    { label: 'the newest root when nothing names one', search: '', root: task },
    // The newest root is the Task, so only the selected Run's own root can open here.
    {
      label: "the selected record's root over the newest",
      search: `?record=${standalone}`,
      root: standalone,
    },
    {
      label: 'the named root over the selected record',
      search: `?root=${standalone}&record=${call}`,
      root: standalone,
    },
  ])('opens $label', async ({ search, root }) => {
    const { api } = coral()

    const page = await loadActivity(at(search), api, { scope })

    expect(page.trace?.root.name).toBe(root)
  })

  // Catches: `run_status_url` opening the newest root instead of the Run's own Task.
  it("opens a Run's Task on the Run's status page, with the Run selected", async () => {
    const { api } = coral()

    const page = await loadRunActivity(new Request(`https://reef.test/${run}`), api, {
      workspace: workspace.split('/')[1]!,
      run: run.split('/').at(-1)!,
    })

    expect(page.trace?.root.name).toBe(task)
    expect(page.trace?.children[run]?.map((row) => row.name)).toEqual([call])
    expect(page.selected).toBe(run)
  })

  // Catches: a root you cannot read, or a passing fault, failing the whole page.
  it.each([
    { label: 'cannot read the root', code: Code.NotFound, reason: 'refused' },
    { label: 'are refused the root', code: Code.PermissionDenied, reason: 'refused' },
    { label: 'meet a passing Coral fault', code: Code.Unavailable, reason: 'failed' },
  ])('keeps the list when you $label', async ({ code, reason }) => {
    const { api } = coral({ refused: [task], code })

    const page = await loadActivity(at(''), api, { scope })

    expect(page.trace).toBeNull()
    expect(page.traceUnavailable).toBe(reason)
    expect(page.rows.map((row) => row.name)).toEqual([task, standalone])
  })

  // Catches: a crafted `root` reaching Coral before it is checked.
  it('refuses a root that is not a Task or a Run', async () => {
    const { api, reads } = coral()

    await expect(loadActivity(at(`?root=${call}`), api, { scope })).rejects.toMatchObject({
      status: 400,
    })
    expect(reads).toEqual([])
  })

  // Catches: list paging reading a timeline it never shows.
  it('reads no timeline for a further page of the list', async () => {
    const { api, reads } = coral()

    const page = await loadActivity(at(`?load=page&after=${task}`), api, { scope })

    expect(page.trace).toBeNull()
    expect(reads).toEqual([])
  })

  // Catches: a failed root whose error you must click through to find, or a wait that reads
  // as the failure.
  it('names the last failed call of the root with its error, and what it waited for', async () => {
    const { api } = coral({ callFailed: true })

    const page = await loadActivity(at(''), api, { scope })

    expect(page.trace?.failure).toEqual({
      record: call,
      label: 'coral.providers.mail.send',
      code: 'upstream_rejected',
      message: 'The provider refused the bearer token.',
      waitedFor: 'approval',
    })
  })

  // Catches: a retry that failed the same way read as a new failure, or a wait whose reason
  // (an approval) the timeline cannot name, finished or still waiting.
  it('names why calls waited and how often the failed call failed the same way', async () => {
    const first = `${workspace}/invocations/first-attempt-call`
    const second = `${workspace}/invocations/second-attempt-call`
    const waiting = `${workspace}/invocations/still-waiting-call`
    const pending = {
      ...invocation,
      name: waiting,
      state: ActivityInvocationState.PENDING,
      requiresAction: true,
      startedAtUnixNanos: s(18),
      endedAtUnixNanos: undefined,
    }
    const error = {
      case: 'coralFailure' as const,
      value: { code: 'upstream_incomplete', message: 'Upstream result is incomplete' },
    }
    const api = fakeManagement({
      activity: {
        browseActivityRoots: () => ({
          roots: [{ record: { case: 'task' as const, value: goal } }],
        }),
        inspectTask: () => ({
          task: goal,
          interactions: [
            {
              name: `${task}/interactions/submit`,
              kind: 'submit',
              intent: 'Send reminders',
              outcome: 'succeeded',
              startedAtUnixNanos: s(12),
              acceptedRun: run,
              run: runOf(run, task),
            },
          ],
        }),
        browseRunEntries: () => ({
          run: runOf(run, task),
          entries: [
            { record: { case: 'invocation' as const, value: failedCall(first, 12, 15) } },
            { record: { case: 'invocation' as const, value: failedCall(second, 17, 17) } },
            { record: { case: 'invocation' as const, value: pending } },
          ],
        }),
        inspectInvocation: ({ name }) => ({
          invocation: name === waiting ? pending : failedCall(name, 0, 0),
          ...(name !== waiting && { outcome: error }),
          requests:
            name === second
              ? []
              : [{ request: { case: 'approval' as const, value: `${workspace}/approvals/a` } }],
        }),
      },
    })

    const page = await loadActivity(at(''), api, { scope })

    // A call still waiting names its reason too, however short its wait so far.
    expect(page.trace?.waitedFor).toEqual({ [first]: 'approval', [waiting]: 'approval' })
    expect(page.trace?.failure).toMatchObject({ record: second, sameBefore: 1 })
  })
  // Catches: a resave shown as a new function, a saved function named by the step that saved
  // it when a later Run of the Task called it by its path, or a function never tested shown
  // as tested.
  it('names the function each save saved, and whether a later Run called it and succeeded', async () => {
    const saved = `${workspace}/savedFunctions/health-report`
    const save = (id: string, second: number) => ({
      name: `${task}/interactions/${id}`,
      kind: 'save',
      intent: 'Save the health report',
      outcome: 'succeeded',
      startedAtUnixNanos: s(second),
      completedAtUnixNanos: s(second),
    })
    const api = fakeManagement({
      activity: {
        browseActivityRoots: () => ({
          roots: [{ record: { case: 'task' as const, value: goal } }],
        }),
        inspectTask: () => ({
          task: goal,
          interactions: [
            save('first-save', 11),
            save('resave', 13),
            {
              name: `${task}/interactions/smoke`,
              kind: 'submit',
              intent: 'Smoke-test it',
              outcome: 'succeeded',
              startedAtUnixNanos: s(14),
              acceptedRun: run,
              run: { ...runOf(run, task), startedAtUnixNanos: s(14) },
            },
          ],
        }),
        browseRunEntries: () => ({ run: runOf(run, task), entries: [] }),
        inspectTaskInteraction: ({ name }) => ({
          interaction: save(name.split('/').at(-1)!, 11),
          evidence: { case: 'savedFunction' as const, value: saved },
        }),
        inspectRun: () => ({
          run: runOf(run, task),
          linkedFunctions: [{ savedFunction: saved, path: 'clickstack.healthReport', source: '' }],
        }),
      },
    })

    const page = await loadActivity(at(''), api, { scope })

    // The smoke test ran after both saves and succeeded, so it tested each of them.
    const named = { savedFunction: saved, path: 'clickstack.healthReport', tested: true }
    expect(page.trace?.saves).toEqual({
      [`${task}/interactions/first-save`]: named,
      [`${task}/interactions/resave`]: named,
    })
  })
})
