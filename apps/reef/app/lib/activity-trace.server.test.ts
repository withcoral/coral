import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  ActivityInvocationState,
  ActivityRunState,
} from '@/generated/coral/management/v1/activity_pb'
import { fakeManagement, type ManagementHandlers } from './management.test-support'
import { loadActivityTrace } from './activity.server'

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const scope = { workspace }
const task = `${workspace}/tasks/plain-amber-river-stone`
const run = (id: string) => `${workspace}/runs/${id}`
const s = (seconds: number) => BigInt(seconds * 1_000_000_000)

const goal = {
  name: task,
  workspace,
  intent: 'Find overdue invoices',
  outcome: 'open',
  actor: 'user',
  startedAtUnixNanos: s(0),
}
const search = (id: string, at: number) => ({
  name: `${task}/interactions/${id}`,
  kind: 'search',
  intent: 'Find the invoice tools',
  outcome: 'succeeded',
  startedAtUnixNanos: s(at),
  completedAtUnixNanos: s(at + 1),
})
const submit = (id: string, at: number) => ({
  name: `${task}/interactions/${id}`,
  kind: 'submit',
  intent: 'Send reminders',
  outcome: 'succeeded',
  startedAtUnixNanos: s(at),
  completedAtUnixNanos: s(at),
  acceptedRun: run(id),
  run: {
    name: run(id),
    workspace,
    task,
    actor: 'user',
    state: ActivityRunState.SUCCEEDED,
    startedAtUnixNanos: s(at),
    endedAtUnixNanos: s(at + 10),
  },
})
const call = (runId: string, id: string, at: number) => ({
  record: {
    case: 'invocation' as const,
    value: {
      name: `${workspace}/invocations/${id}`,
      workspace,
      run: run(runId),
      task,
      tool: 'send',
      callPath: 'coral.providers.mail.send',
      state: ActivityInvocationState.ENDED,
      startedAtUnixNanos: s(at),
      endedAtUnixNanos: s(at + 1),
      dispatchedAtUnixNanos: s(at),
    },
  },
})
const emission = (sequence: number, at: number) => ({
  record: {
    case: 'event' as const,
    value: { sequence: BigInt(sequence), kind: 'program.emitted', occurredAtUnixNanos: s(at) },
  },
})
const ended = (sequence: number, at: number, bytes: bigint) => ({
  record: {
    case: 'event' as const,
    value: {
      sequence: BigInt(sequence),
      kind: 'run.ended',
      occurredAtUnixNanos: s(at),
      contextImpactBytes: bytes,
    },
  },
})
type Interaction = ReturnType<typeof search> | ReturnType<typeof submit>
type Entry = ReturnType<typeof call> | ReturnType<typeof emission> | ReturnType<typeof ended>

/**
 * An in-memory Coral holding one Task, or one standalone Run when `task` is absent.
 * It pages as Coral does: up to `pageSize` records after the cursor it returned last.
 * `reads` counts each page it served.
 */
function coral({
  interactions = [],
  entries = {},
  standalone = false,
  refused = {},
}: {
  interactions?: Interaction[]
  entries?: Record<string, Entry[]>
  /** Its Runs belong to no Task. */
  standalone?: boolean
  /** Runs whose children Coral refuses to return, and how. */
  refused?: Record<string, Code>
}) {
  const reads: string[] = []
  const handlers: ManagementHandlers = {
    activity: {
      inspectTask: (input) => {
        reads.push(`task after ${input.after || 'start'}`)
        const from = input.after ? interactions.findIndex((i) => i.name === input.after) + 1 : 0
        const page = interactions.slice(from, from + input.pageSize)
        const last = from + page.length >= interactions.length
        return {
          task: goal,
          interactions: page.map((value, index) => ({ ...value, position: BigInt(from + index) })),
          nextAfter: last ? '' : page.at(-1)!.name,
          total: BigInt(interactions.length),
          offset: BigInt(from),
        }
      },
      browseRunEntries: (input) => {
        reads.push(`${input.name.split('/').at(-1)} after ${input.after}`)
        const refusal = refused[input.name]
        if (refusal !== undefined) throw new ConnectError('run unavailable', refusal)
        const all = entries[input.name] ?? []
        const from = Number(input.after)
        const page = all.slice(from, from + input.pageSize)
        return {
          run: {
            name: input.name,
            workspace,
            task: standalone ? '' : task,
            actor: 'user',
            intent: 'Export paid invoices',
            state: ActivityRunState.RUNNING,
            startedAtUnixNanos: s(0),
          },
          entries: page,
          nextAfter: BigInt(from + page.length),
          hasMore: from + page.length < all.length,
          total: BigInt(all.length),
          offset: BigInt(from),
        }
      },
    },
  }
  return { reads, api: fakeManagement(handlers) }
}

const names = (rows: { name: string }[] | undefined) => rows?.map((row) => row.name)

describe('loadActivityTrace', () => {
  // Catches: a timeline missing a Task's tool calls, a Run, or a Run's own records.
  it("reads a Task's tool calls and Runs, and each Run's calls and events", async () => {
    const { api } = coral({
      interactions: [search('look', 0), submit('send', 2)],
      entries: { [run('send')]: [call('send', 'mail', 3), emission(2, 4)] },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(trace.root).toMatchObject({ kind: 'task', name: task, intent: 'Find overdue invoices' })
    expect(names(trace.children[task])).toEqual([`${task}/interactions/look`, run('send')])
    expect(trace.children[task]?.map((row) => row.kind)).toEqual(['tool', 'run'])
    expect(names(trace.children[run('send')])).toEqual([
      `${workspace}/invocations/mail`,
      `${run('send')}/events/2`,
    ])
    expect(trace.complete).toBe(true)
  })

  // Catches: a timeline cut at Coral's first page of records.
  it('follows every page of a Task and of its Runs', async () => {
    const searches = Array.from({ length: 150 }, (_, i) => search(`look-${i}`, i))
    const calls = Array.from({ length: 130 }, (_, i) => call('send', `mail-${i}`, 200 + i))
    const { api } = coral({
      interactions: [...searches, submit('send', 160)],
      entries: { [run('send')]: calls },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(trace.children[task]).toHaveLength(151)
    expect(trace.children[task]?.at(-1)?.name).toBe(run('send'))
    expect(names(trace.children[run('send')])).toEqual(calls.map(({ record }) => record.value.name))
    expect(trace.complete).toBe(true)
  })

  // Catches: a very long root holding a page load, or a limit that hides it read everything.
  it.each([
    { limit: 120, read: 120, complete: false },
    { limit: 151, read: 151, complete: true },
  ])('reads at most $limit records', async ({ limit, read, complete }) => {
    const { api, reads } = coral({
      interactions: [
        ...Array.from({ length: 100 }, (_, i) => search(`look-${i}`, i)),
        submit('send', 100),
      ],
      entries: {
        [run('send')]: Array.from({ length: 50 }, (_, i) => call('send', `mail-${i}`, 101 + i)),
      },
    })

    const trace = await loadActivityTrace(api, task, { scope, limit })

    expect(Object.values(trace.children).flat()).toHaveLength(read)
    expect(trace.complete).toBe(complete)
    expect(reads).toEqual([
      'task after start',
      `task after ${task}/interactions/look-99`,
      'send after 0',
    ])
  })

  // Catches: a Run with no Task opening no timeline.
  it('reads a standalone Run as its own root', async () => {
    const exported = run('export')
    const { api } = coral({
      standalone: true,
      entries: { [exported]: [call('export', 'upload', 1), emission(3, 2)] },
    })

    const trace = await loadActivityTrace(api, exported, { scope })

    expect(trace.root).toMatchObject({
      kind: 'run',
      name: exported,
      intent: 'Export paid invoices',
    })
    expect(names(trace.children[exported])).toEqual([
      `${workspace}/invocations/upload`,
      `${exported}/events/3`,
    ])
    expect(trace.complete).toBe(true)
  })

  // Catches: one Run you cannot read hiding the whole Task, or its records leaking.
  it("names a Run whose records you cannot read and reads the Task's other Runs", async () => {
    const { api } = coral({
      interactions: [submit('hidden', 0), submit('send', 2)],
      entries: { [run('send')]: [call('send', 'mail', 3)] },
      refused: { [run('hidden')]: Code.PermissionDenied },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(trace.unavailable).toEqual([run('hidden')])
    expect(trace.children[run('hidden')]).toBeUndefined()
    expect(names(trace.children[run('send')])).toEqual([`${workspace}/invocations/mail`])
    expect(trace.complete).toBe(true)
  })

  // Catches: a fault in a Run read taken for a refusal and hidden.
  it('fails when Coral fails a Run read for another reason', async () => {
    const { api } = coral({
      interactions: [submit('send', 0)],
      refused: { [run('send')]: Code.Internal },
    })

    await expect(loadActivityTrace(api, task, { scope })).rejects.toMatchObject({
      code: Code.Internal,
    })
  })

  // Catches: a Run's return value missing from its timeline, or an empty end shown as one.
  it.each([
    { bytes: 12n, name: 'final', label: 'return value' },
    { bytes: 0n, name: 'events/4', label: 'execution ended' },
  ])('shows a Run end that returned $bytes bytes as $label', async ({ bytes, name, label }) => {
    const { api } = coral({
      interactions: [submit('send', 0)],
      entries: { [run('send')]: [ended(4, 5, bytes)] },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(trace.children[run('send')]).toMatchObject([{ name: `${run('send')}/${name}`, label }])
  })

  // Catches: a cursor that never moves keeping the read going until the limit.
  it('stops at a page with no records, though Coral names another', async () => {
    const reads: string[] = []
    const api = fakeManagement({
      activity: {
        inspectTask: (input) => {
          reads.push(`task after ${input.after || 'start'}`)
          return { task: goal, interactions: [], nextAfter: 'again' }
        },
      },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(reads).toEqual(['task after start'])
    expect(trace.complete).toBe(true)
  })

  // Catches: the agent's last word on a Task, the one record of its time after its last call,
  // missing from the timeline.
  it('keeps the feedback the agent gave when it ended the Task', async () => {
    const api = fakeManagement({
      activity: {
        inspectTask: () => ({
          task: goal,
          interactions: [],
          feedback: 'The provider would not accept the token.',
        }),
      },
    })

    const trace = await loadActivityTrace(api, task, { scope })

    expect(trace.feedback).toBe('The provider would not accept the token.')
  })
})
