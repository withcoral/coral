import { describe, expect, it } from 'vitest'
import {
  interaction,
  invocation,
  outputRow,
  run,
  runName,
  task,
  taskName,
  toolRow,
} from './side-panel-fixtures'
import type { ActivityRow, ActivityTrace } from './types'
import { recordOrder, timeShare, timeline } from './timeline'

const origin = Date.parse('2026-09-18T14:00:00.000Z')
/** The instant `seconds` after the Task started, as Coral sends it. */
const t = (seconds: number) => new Date(origin + seconds * 1000).toISOString()
/** The same instant as the timeline reports it. */
const ms = (seconds: number) => origin + seconds * 1000

const search = toolRow(
  interaction('search', 'succeeded', {
    name: `${taskName}/interactions/look`,
    startedAt: t(0),
    completedAt: t(1),
  }),
)
const program = run({ startedAt: t(2), endedAt: t(10), state: 'succeeded' })
const send = invocation({
  startedAt: t(3),
  dispatchedAt: t(5),
  endedAt: t(6),
  state: 'ended',
  outcome: 'upstream_outcome',
})

function trace(
  root: Extract<ActivityRow, { kind: 'task' | 'run' }>,
  children: Record<string, ActivityRow[]>,
): ActivityTrace {
  return { root, children, complete: true, unavailable: [] }
}

describe('timeline', () => {
  // Catches: records out of tree order, or bars placed off their recorded start and end.
  it("lays out a Task's records in tree order on one axis", () => {
    const value = timeline(
      trace(task({ startedAt: t(0), endedAt: t(12), outcome: 'success' }), {
        [taskName]: [search, program],
        [runName]: [send],
      }),
      ms(60),
    )

    expect(value.from).toBe(ms(0))
    expect(value.to).toBe(ms(12))
    expect(
      value.spans.map(({ record, depth, parent, start, end }) => ({
        name: record.name,
        depth,
        parent,
        start,
        end,
      })),
    ).toEqual([
      { name: taskName, depth: 0, parent: null, start: ms(0), end: { kind: 'ended', at: ms(12) } },
      {
        name: search.name,
        depth: 1,
        parent: taskName,
        start: ms(0),
        end: { kind: 'ended', at: ms(1) },
      },
      {
        name: runName,
        depth: 1,
        parent: taskName,
        start: ms(2),
        end: { kind: 'ended', at: ms(10) },
      },
      {
        name: send.name,
        depth: 2,
        parent: runName,
        start: ms(3),
        end: { kind: 'ended', at: ms(6) },
      },
    ])
    expect(value.spans.at(-1)?.wait).toEqual({ until: ms(5) })
  })

  /** The end the timeline gives `record`, under a finished Task and Run. */
  function endOf(record: ActivityRow) {
    const root =
      record.kind === 'task' ? record : task({ startedAt: t(0), endedAt: t(1), outcome: 'success' })
    const parent = record.kind === 'invocation' || record.kind === 'output' ? program : null
    const children: Record<string, ActivityRow[]> =
      record.kind === 'task'
        ? {}
        : parent
          ? { [taskName]: [parent], [runName]: [record] }
          : { [taskName]: [record] }
    return timeline(trace(root, children), ms(60)).spans.find(
      (span) => span.record.name === record.name,
    )?.end
  }

  // Catches: a missing end drawn as running, or running work drawn as ended (#2446).
  it.each([
    {
      label: 'an open Task',
      kind: 'ongoing',
      record: task({ startedAt: t(0), endedAt: null, outcome: 'open' }),
    },
    {
      label: 'a decided Task without an end',
      kind: 'unknown',
      record: task({ startedAt: t(0), endedAt: null, outcome: 'success' }),
    },
    {
      label: 'a tool call without a completion',
      kind: 'unknown',
      record: toolRow(interaction('search', 'unknown', { startedAt: t(0) })),
    },
    {
      label: 'a running Run',
      kind: 'ongoing',
      record: run({ startedAt: t(0), endedAt: null, state: 'running' }),
    },
    {
      label: 'a Run in an unknown state',
      kind: 'unknown',
      record: run({ startedAt: t(0), endedAt: null, state: 'unknown' }),
    },
    {
      label: 'a pending call',
      kind: 'ongoing',
      record: invocation({ startedAt: t(3), endedAt: null, state: 'pending' }),
    },
    {
      label: 'a dispatching call',
      kind: 'ongoing',
      record: invocation({
        startedAt: t(3),
        endedAt: null,
        state: 'dispatching',
        dispatchedAt: t(4),
      }),
    },
    {
      label: 'an ended call without an end',
      kind: 'unknown',
      record: invocation({ startedAt: t(3), endedAt: null, state: 'ended' }),
    },
  ])('gives $label an end that is $kind', ({ record, kind }) => {
    expect(endOf(record)).toEqual({ kind })
  })

  // Catches: an axis that cuts off running work or a child that outlives its Task, an
  // open Task stretching it to now, or a zero-width axis that divides by zero.
  it.each([
    {
      label: 'runs to now while a Run runs',
      root: task({ startedAt: t(0), endedAt: null, outcome: 'open' }),
      children: { [taskName]: [run({ startedAt: t(2), endedAt: null, state: 'running' })] },
      to: ms(60),
    },
    {
      // An open Task is a goal, not work: one left open for days keeps its records in view.
      label: 'stops at the last record while an open Task waits',
      root: task({ startedAt: t(0), endedAt: null, outcome: 'open' }),
      children: { [taskName]: [run({ startedAt: t(2), endedAt: t(9), state: 'succeeded' })] },
      to: ms(9),
    },
    {
      label: 'stops at the last known end when an end is unknown',
      root: task({ startedAt: t(0), endedAt: t(2), outcome: 'success' }),
      children: { [taskName]: [toolRow(interaction('search', 'unknown', { startedAt: t(1) }))] },
      to: ms(2),
    },
    {
      label: 'reaches a Run that outlives its Task',
      root: task({ startedAt: t(0), endedAt: t(5), outcome: 'success' }),
      children: { [taskName]: [run({ startedAt: t(4), endedAt: t(20), state: 'succeeded' })] },
      to: ms(20),
    },
    {
      label: 'stays wider than zero when every record starts and ends at once',
      root: run({ startedAt: t(3), endedAt: t(3), state: 'succeeded' }),
      children: {},
      to: ms(3) + 1,
    },
  ] as {
    label: string
    root: ActivityTrace['root']
    children: ActivityTrace['children']
    to: number
  }[])('$label', ({ root, children, to }) => {
    expect(timeline(trace(root, children), ms(60)).to).toBe(to)
  })

  // Catches: a wait drawn past dispatch, a pending call without one, or a call dispatched
  // at once shown as waiting.
  it.each([
    {
      label: 'until dispatch',
      call: invocation({ startedAt: t(3), dispatchedAt: t(5), endedAt: t(6), state: 'ended' }),
      wait: { until: ms(5) },
    },
    {
      label: 'on while it is pending',
      call: invocation({ startedAt: t(3), dispatchedAt: null, endedAt: null, state: 'pending' }),
      wait: { until: 'ongoing' },
    },
    {
      label: 'until its end when it ended without dispatch',
      call: invocation({ startedAt: t(3), dispatchedAt: null, endedAt: t(7), state: 'ended' }),
      wait: { until: ms(7) },
    },
    {
      label: 'not at all when it dispatched as it started',
      call: invocation({ startedAt: t(3), dispatchedAt: t(3), endedAt: t(4), state: 'ended' }),
      wait: undefined,
    },
  ])('makes a call wait $label', ({ call, wait }) => {
    const value = timeline(
      trace(task({ startedAt: t(0), endedAt: t(10), outcome: 'success' }), {
        [taskName]: [program],
        [runName]: [call],
      }),
      ms(60),
    )
    expect(value.spans.find((span) => span.record.name === call.name)?.wait).toEqual(wait)
  })

  // Catches: Events given rows of their own, which the waterfall shows as marks.
  it("marks a Run's Events on its own row instead of giving them rows", () => {
    const emitted = outputRow(
      { sequence: '4', occurredAt: t(4), kind: 'program.emitted', value: '{}' },
      'emission #4',
    )
    const logged = outputRow(
      { sequence: '7', occurredAt: t(8), kind: 'program.logged', level: 'info', message: 'done' },
      'log #7',
    )
    const value = timeline(
      trace(task({ startedAt: t(0), endedAt: t(10), outcome: 'success' }), {
        [taskName]: [program],
        [runName]: [emitted, send, logged],
      }),
      ms(60),
    )

    expect(value.spans.map((span) => span.record.name)).toEqual([taskName, runName, send.name])
    expect(value.spans.find((span) => span.record.name === runName)?.marks).toEqual([
      { record: emitted, at: ms(4) },
      { record: logged, at: ms(8) },
    ])
  })
})

describe('recordOrder', () => {
  // Catches: ↑ and ↓ in the detail skipping Events, or stepping out of tree order.
  it('lists every record of a trace, Events included, each before the records under it', () => {
    const emitted = outputRow(
      { sequence: '4', occurredAt: t(4), kind: 'program.emitted', value: '{}' },
      'emission #4',
    )
    const order = recordOrder(
      trace(task({ startedAt: t(0), endedAt: t(12), outcome: 'success' }), {
        [taskName]: [search, program],
        [runName]: [send, emitted],
      }),
    )

    expect(order).toEqual([taskName, search.name, runName, send.name, emitted.name])
  })
})

describe('timeShare', () => {
  // Catches: agent time between calls hidden in the root's bar, waits counted as running,
  // or the time split into parts that do not add up to the root's.
  it('splits the root into agent time between calls, waiting, and running', () => {
    const layout = timeline(
      trace(task({ startedAt: t(0), endedAt: t(60), outcome: 'failure' }), {
        [taskName]: [
          toolRow(interaction('search', 'succeeded', { startedAt: t(0), completedAt: t(1) })),
          run({ startedAt: t(10), endedAt: t(20), state: 'succeeded' }),
        ],
        [runName]: [
          invocation({ startedAt: t(11), dispatchedAt: t(15), endedAt: t(19), state: 'ended' }),
        ],
      }),
      ms(60),
    )

    expect(timeShare(layout, ms(60), { [invocation().name]: 'approval' })).toEqual({
      agent: 49_000,
      waiting: { approval: 4000, 'account connection': 0, start: 0 },
      running: 7000,
      gaps: [
        { from: ms(1), to: ms(10) },
        { from: ms(20), to: ms(60) },
      ],
    })
  })
})
