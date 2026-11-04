import { createClient, createRouterTransport, Code, ConnectError } from '@connectrpc/connect'
import { expect, it } from 'vitest'
import { ActivityService, ActivityRunState } from '@/generated/coral/management/v1/activity_pb'
import { ProviderAccountService } from '@/generated/coral/management/v1/account_pb'
import { ProviderService } from '@/generated/coral/management/v1/provider_pb'
import { SavedFunctionService } from '@/generated/coral/management/v1/saved_function_pb'
import { observeActivity } from './activity-observation.server'
import { activityChanges } from './activity-stream.test-support'

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const task = `${workspace}/tasks/00000000-0000-4000-8000-000000000002`
const child = (index: number) =>
  `${task}/interactions/00000000-0000-4000-8000-${String(index).padStart(12, '0')}`
function fixture(children = 0) {
  const changes = activityChanges()
  let authorized = true
  let resolved = false
  let reads = 0
  let exactReads = 0
  const transport = createRouterTransport(({ service }) => {
    service(ActivityService, {
      watchActivity: changes.watch,
      browseActivityRoots: (input) => {
        reads++
        if (!authorized)
          return {
            roots: [],
            anchorUnavailable: Boolean(input.retained?.names.length),
          }
        const names = input.retained ? input.retained.names : [task, `${task}-new`]
        return {
          roots: names.map((name) => ({
            record: {
              case: 'task' as const,
              value: {
                name,
                workspace,
                intent: 'Summary only',
                outcome: 'open',
              },
            },
          })),
        }
      },
      inspectTask: ({ name: taskName, viewport, pageSize }) => {
        if (!authorized) throw new ConnectError('forbidden', Code.NotFound)
        const offset = Number(viewport?.offset ?? 0n)
        const names = Array.from(
          { length: Math.min(Math.max(0, children - offset), pageSize) },
          (_, index) => child(offset + index + 1),
        )
        const last = Number(names.at(-1)?.split('-').at(-1) ?? 0)
        return {
          task: { name: taskName, workspace, intent: 'Summary only' },
          total: BigInt(children),
          offset: BigInt(offset),
          nextAfter: last && last < children ? names.at(-1)! : '',
          interactions: names.map((name) => ({
            name,
            kind: 'describe',
            intent: 'Historical discovery',
            outcome: 'succeeded',
          })),
        }
      },
      inspectRun: ({ name }) => {
        exactReads++
        if (!authorized) throw new ConnectError('forbidden', Code.NotFound)
        return {
          run: {
            name,
            workspace,
            state: resolved ? ActivityRunState.SUCCEEDED : ActivityRunState.RUNNING,
            requiresAction: !resolved,
            durationNanos: BigInt(exactReads),
          },
          submittedSource: 'selected-source-only',
          terminal: resolved
            ? { case: 'succeeded' as const, value: { finalValueJson: 'null' } }
            : undefined,
        }
      },
      inspectTaskInteraction: () => {
        exactReads++
        throw new ConnectError('forbidden', Code.NotFound)
      },
      // A selected Run with no Task opens as its own timeline, which has no records here.
      browseRunEntries: ({ name }) => {
        if (!authorized) throw new ConnectError('forbidden', Code.NotFound)
        return { run: { name, workspace }, entries: [] }
      },
    })
    service(ProviderAccountService, {})
  })
  return {
    api: {
      activity: createClient(ActivityService, transport),
      accounts: createClient(ProviderAccountService, transport),
      providers: createClient(ProviderService, transport),
      savedFunctions: createClient(SavedFunctionService, transport),
    },
    revoke: () => {
      authorized = false
      changes.emit()
    },
    append: () => {
      children++
      changes.emit()
    },
    resolve: () => {
      resolved = true
      changes.emit()
    },
    notify: changes.emit,
    reads: () => reads,
    exactReads: () => exactReads,
  }
}
function request(body: unknown, signal?: AbortSignal) {
  return new Request('http://reef.test/activity/observe', {
    method: 'POST',
    body: JSON.stringify(body),
    signal,
  })
}
const observation = {
  path: `/${workspace}/activity`,
  search: '',
  window: { roots: [task], followRoot: false },
}
async function emission(reader: ReadableStreamDefaultReader<Uint8Array>) {
  return JSON.parse(
    new TextDecoder()
      .decode((await reader.read()).value)
      .split('\ndata: ')[1]
      .trim(),
  )
}
it('keeps roots stable away from the global edge and conceals revoked rows', async () => {
  const f = fixture()
  const response = await observeActivity(request(observation), f.api)
  expect(response.headers.get('cache-control')).toContain('no-store')
  const reader = response.body!.getReader()
  try {
    const first = await emission(reader)
    expect(first.page.rows.map((row: { name: string }) => row.name)).toEqual([task])
    expect(first.page).not.toHaveProperty('total')
    expect(first.page.detail).toBeNull()
    expect(f.exactReads()).toBe(0)
    f.revoke()
    const lost = await emission(reader)
    expect(lost.page).toMatchObject({
      rows: [],
      anchorUnavailable: true,
    })
  } finally {
    await reader.cancel()
  }
})
it('does not expose unrelated mutations through changed clock samples in exact detail', async () => {
  const f = fixture()
  const run = `${workspace}/runs/00000000-0000-4000-8000-000000000003`
  const response = await observeActivity(
    request({ ...observation, search: `?record=${run}` }),
    f.api,
  )
  const reader = response.body!.getReader()
  try {
    expect((await emission(reader)).kind).toBe('page')
    f.notify()
    f.notify(false)
    expect(await emission(reader)).toEqual({ kind: 'heartbeat' })
    expect(f.exactReads()).toBe(2)
  } finally {
    await reader.cancel()
  }
})

it('cancels an in-flight history request when the reader cancels observation', async () => {
  let enter!: (signal: AbortSignal) => void
  let release!: () => void
  const entered = new Promise<AbortSignal>((resolve) => {
    enter = resolve
  })
  const pending = new Promise<void>((resolve) => {
    release = resolve
  })
  const transport = createRouterTransport(({ service }) => {
    service(ActivityService, {
      watchActivity: activityChanges().watch,
      browseActivityRoots: async (_input, context) => {
        enter(context.signal)
        await pending
        return { roots: [] }
      },
    })
  })
  const response = await observeActivity(request(observation), {
    ...fixture().api,
    activity: createClient(ActivityService, transport),
  })
  const reader = response.body!.getReader()
  const reading = reader.read()
  try {
    const signal = await entered
    expect(signal.aborted).toBe(false)
    await reader.cancel()
    expect(await reading).toEqual({ done: true, value: undefined })
    expect(signal.aborted).toBe(true)
  } finally {
    release()
    await reader.cancel()
  }
})
it('automatically inserts new roots when following the global edge', async () => {
  const f = fixture()
  const response = await observeActivity(
    request({
      ...observation,
      window: { ...observation.window, followRoot: true },
    }),
    f.api,
  )
  const reader = response.body!.getReader()
  try {
    const update = await emission(reader)
    expect(update.page.rows.map((row: { name: string }) => row.name)).toEqual([task, `${task}-new`])
    expect(update.page.detail).toBeNull()
    expect(f.exactReads()).toBe(0)
  } finally {
    await reader.cancel()
  }
})
it('keeps the Workspace Activity destination while updating independent exact detail and concealing access loss', async () => {
  const path = `/${workspace}/activity`
  const f = fixture()
  const run = `${workspace}/runs/00000000-0000-4000-8000-000000000003`
  const response = await observeActivity(
    request({ ...observation, path, search: `?tree=1&record=${run}` }),
    f.api,
  )
  const reader = response.body!.getReader()
  try {
    const first = await emission(reader)
    expect(first.page.listPath).toBe(path)
    expect(first.page.detail).toMatchObject({
      source: 'selected-source-only',
      run: { state: 'running', requiresAction: true },
    })
    expect(f.exactReads()).toBe(1)
    f.resolve()
    const changed = await emission(reader)
    expect(changed.page.selected).toBe(run)
    expect(changed.page.detail).toMatchObject({
      source: 'selected-source-only',
      run: { state: 'succeeded', requiresAction: false },
      terminal: { finalValue: 'null' },
    })
    expect(f.exactReads()).toBe(2)
    f.revoke()
    const lost = await emission(reader)
    expect(lost.page).toMatchObject({
      selected: run,
      detail: null,
      selectedRow: null,
      unavailable: true,
    })
    expect(JSON.stringify(lost)).not.toContain('selected-source-only')
  } finally {
    await reader.cancel()
  }
})
it('cancels a waiting emission when its authenticated request disconnects', async () => {
  const f = fixture()
  const cancellation = new AbortController()
  const response = await observeActivity(request(observation, cancellation.signal), f.api)
  const reader = response.body!.getReader()
  await reader.read()
  const reads = f.reads()
  const waiting = reader.read()
  cancellation.abort()
  expect((await waiting).done).toBe(true)
  expect(f.reads()).toBe(reads)
})
it('accepts encoded Agent navigation and rejects encoded path separators', async () => {
  const f = fixture()
  const response = await observeActivity(
    request({
      ...observation,
      path: `/${workspace}/agents/agent%3A00000000-0000-4000-8000-000000000003`,
    }),
    f.api,
    { endsAt: 0, renewAt: 0 },
  )
  expect(await response.text()).toContain('sessionExpired')
  await expect(
    observeActivity(request({ ...observation, path: `/${workspace}/agents/agent%2Fother` }), f.api),
  ).rejects.toMatchObject({ status: 400 })
})
it('rejects an unbounded or invalid window before any domain query', async () => {
  const f = fixture()
  for (const body of [
    { ...observation, path: '/organization/members' },
    // The Overview shows a summary, not an activity reader.
    { ...observation, path: `/${workspace}` },
    { ...observation, path: `/${workspace}/settings` },
    {
      ...observation,
      window: { ...observation.window, roots: Array(26).fill(task) },
    },
    { ...observation, window: { ...observation.window, roots: ['workspaces/w/approvals/a'] } },
  ])
    await expect(observeActivity(request(body), f.api)).rejects.toMatchObject({
      status: 400,
    })
  expect(f.reads()).toBe(0)
})
