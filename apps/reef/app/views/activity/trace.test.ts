import { describe, expect, it } from 'vitest'
import { absorbPage, openTrace, traceChildren } from '@/lib/activity-trace'
import { foldedScale } from '@/wax/components/timeline'
import { activityPage } from './fixtures'
import { recordParents, resolveStop, stepStop, stopPosition, type StopPosition } from './navigation'
import type { ActivityRow } from './types'
import {
  adjacentMark,
  compressedTime,
  enclosingRecords,
  isOngoing,
  overview,
  pendingWaitReason,
  recordRows,
  rootOf,
  spanOf,
  waitOf,
  waterfall,
  waterfallStops,
} from './trace'
import {
  T0,
  traceCall,
  traceEvent,
  traceRun,
  traceStore,
  traceTask,
  traceTool,
} from './trace-fixtures'

const task = traceTask()
const none = new Set<string>()

it.each([
  ['an open task', traceTask({ outcome: 'open', endedAt: null }), 'ongoing'],
  ['a task with an outcome and no end', traceTask({ endedAt: null }), 'unknown'],
  ['a running run', traceRun('a', 0, null), 'ongoing'],
  ['a run in a state Reef does not know', traceRun('a', 0, null, { state: 'unknown' }), 'unknown'],
  ['a pending call', traceCall(traceRun('a', 0, 9), 'c', [0, null, null]), 'ongoing'],
  ['a sent call', traceCall(traceRun('a', 0, 9), 'c', [0, 1, null]), 'ongoing'],
  [
    'a call in a state Reef does not know',
    traceCall(traceRun('a', 0, 9), 'c', [0, 1, null], { state: 'unknown' }),
    'unknown',
  ],
  // A missing completion is not evidence of work.
  ['a search with no recorded completion', traceTool('s', 'search', 0, null), 'unknown'],
  ['an ended run', traceRun('a', 0, 9), 'ended'],
] as const)('reads %s as %s', (_, row, end) => {
  expect(spanOf(row).end.kind).toBe(end)
  expect(isOngoing(row)).toBe(end === 'ongoing')
})

it('starts a call at its admission and counts its wait before dispatch', () => {
  const run = traceRun('a', 0, 9_000)
  expect(waitOf(traceCall(run, 'sent', [100, 400, 900]))).toEqual({ from: T0 + 100, to: T0 + 400 })
  expect(waitOf(traceCall(run, 'at-once', [100, 100, 900]))).toBeUndefined()
  expect(waitOf(traceCall(run, 'waits', [100, null, null]))).toEqual({ from: T0 + 100, to: null })
  // It ended without being sent, as after a decline: it waited all its life.
  expect(waitOf(traceCall(run, 'declined', [100, null, 700]))).toEqual({
    from: T0 + 100,
    to: T0 + 700,
  })
  expect(spanOf(traceCall(run, 'sent', [100, 400, 900]))).toEqual({
    start: T0 + 100,
    end: { kind: 'ended', at: T0 + 900 },
  })
})

it('names a wait only while the run still lists its request', () => {
  const action = {
    kind: 'approval' as const,
    name: 'workspaces/finance/approvals/one',
    callPath: 'coral.providers.linear.listIssues',
    providerLabel: 'Linear',
    deadline: '2030-01-01T00:00:00Z',
  }
  const run = traceRun('a', 0, null, {
    requiresAction: true,
    actionPage: { actions: [action], total: '1', next: '' },
  })
  const waits = traceCall(run, 'c', [0, null, null], { requiresAction: true })
  expect(pendingWaitReason(waits, run)).toBe('approval')
  // A settled wait has no row that says what it was for.
  expect(pendingWaitReason(traceCall(run, 'c', [0, 5, 9]), run)).toBeNull()
  // Two kinds of request for one tool do not say which this call waits on.
  const both = {
    ...run,
    actionPage: {
      actions: [action, { ...action, kind: 'account_connection' as const }],
      total: '2',
      next: '',
    },
  }
  expect(pendingWaitReason(waits, both)).toBeNull()
  expect(pendingWaitReason(waits, traceRun('b', 0, null))).toBeNull()
})

it('lists records in order with events as marks on their run, not as rows', () => {
  const run = traceRun('a', 1_000, 5_000)
  const store = traceStore(task, {
    [task.name]: [traceTool('s', 'search', 100, 300), run],
    [run.name]: [
      traceEvent(run, 1, 1_000, 'run.started'),
      traceCall(run, 'c1', [1_200, 1_200, 2_000]),
      traceEvent(run, 2, 2_100),
      traceEvent(run, 3, 2_200, 'program.emitted'),
      traceCall(run, 'c2', [2_300, 2_300, 4_000]),
      traceEvent(run, 4, 5_000, 'run.ended'),
    ],
  })
  const view = waterfall(store, none, T0)
  expect(
    view.rows.map((row) => (row.kind === 'record' ? row.row.name.split('/').at(-1) : 'gap')),
  ).toEqual(['invoices', 's', 'a', 'c1', 'c2'])
  expect(recordRows(view.rows).map((row) => row.depth)).toEqual([0, 1, 1, 2, 2])
  const runRow = recordRows(view.rows)[2]!
  // The Run's start and end are the ends of its bar, not marks.
  expect(runRow.marks.map((mark) => [mark.row.label, mark.at - T0])).toEqual([
    ['log #2', 2_100],
    ['emission #3', 2_200],
  ])
  expect(recordRows(view.rows)[3]!.parents).toEqual([run.name, task.name])
})

it('hides the children of a collapsed record and keeps them on the axis', () => {
  const run = traceRun('a', 1_000, 90_000)
  const store = traceStore(task, {
    [task.name]: [run],
    [run.name]: [traceCall(run, 'c1', [1_200, 1_200, 80_000])],
  })
  const view = waterfall(store, new Set([run.name]), T0)
  expect(recordRows(view.rows).map((row) => [row.row.kind, row.expandable, row.expanded])).toEqual([
    ['task', true, true],
    ['run', true, false],
  ])
  // The Run outlives its Task, which closed at 60 s: the axis covers the Run.
  expect([view.from - T0, view.to - T0]).toEqual([0, 90_000])
})

it('runs the axis to now for work in progress, but not for an open task', () => {
  const open = traceTask({ outcome: 'open', endedAt: null })
  const run = traceRun('a', 1_000, 5_000)
  const idle = waterfall(
    traceStore(open, { [open.name]: [run], [run.name]: [] }),
    none,
    T0 + 900_000,
  )
  expect(idle.ongoing).toBe(false)
  expect(idle.to - T0).toBe(5_000)

  const live = traceRun('b', 6_000, null)
  const busy = waterfall(
    traceStore(open, { [open.name]: [run, live], [run.name]: [], [live.name]: [] }),
    none,
    T0 + 9_000,
  )
  expect(busy.ongoing).toBe(true)
  expect(busy.to - T0).toBe(9_000)
})

it('gives a root of no length an axis that can be drawn', () => {
  const instantRun = traceRun('a', 0, 0, { task: '' })
  const view = waterfall(traceStore(instantRun, { [instantRun.name]: [] }), none, T0)
  expect(view.to).toBeGreaterThan(view.from)
  // Folding it divides by nothing.
  expect(foldedScale(view, view.busy).extent).toBeGreaterThan(0)
})

it('shows a gap where children are not read yet, between the ones that are', () => {
  const run = traceRun('a', 1_000, 5_000)
  const store = absorbPage(
    openTrace(task.name),
    activityPage({
      rows: [task],
      branches: {
        [task.name]: {
          offset: 0,
          total: '1',
          rows: [{ ...run, position: 0 }],
          next: '',
          unavailable: false,
        },
        [run.name]: {
          offset: 2,
          total: '6',
          rows: [
            { ...traceCall(run, 'c2', [1_200, 1_200, 2_000]), position: 2 },
            { ...traceCall(run, 'c3', [2_200, 2_200, 3_000]), position: 3 },
          ],
          next: '',
          unavailable: false,
        },
      },
    }),
  )
  const view = waterfall(store, none, T0)
  expect(
    view.rows.map((row) =>
      row.kind === 'gap' ? `${row.reason} ${row.from}-${row.to}` : row.row.name.split('/').at(-1),
    ),
  ).toEqual(['invoices', 'a', 'unloaded 0-2', 'c2', 'c3', 'unloaded 4-6'])
})

it('shows that a history is unavailable instead of an empty run', () => {
  const run = traceRun('a', 1_000, 5_000)
  const store = absorbPage(
    openTrace(task.name),
    activityPage({
      rows: [task],
      branches: {
        [task.name]: {
          offset: 0,
          total: '1',
          rows: [{ ...run, position: 0 }],
          next: '',
          unavailable: false,
        },
        [run.name]: { rows: [], next: '', unavailable: true },
      },
    }),
  )
  const rows = waterfall(store, none, T0).rows
  expect(rows.at(-1)).toMatchObject({ kind: 'gap', reason: 'unavailable', parent: run.name })
  expect(recordRows(rows)[1]).toMatchObject({ expandable: true })
})

it('finds the top-level record of any record', () => {
  const run = traceRun('a', 0, 9)
  const alone = traceRun('b', 0, 9, { task: '' })
  expect(rootOf(task)).toBe(task.name)
  expect(rootOf(run)).toBe(task.name)
  expect(rootOf(traceCall(run, 'c', [0, 1, 2]))).toBe(task.name)
  expect(rootOf(alone)).toBe(alone.name)
  expect(rootOf(traceEvent(alone, 1, 0))).toBe(alone.name)
  expect(rootOf(traceTool('s', 'search', 0, 1))).toBe(task.name)
})

const at = <Row extends ActivityRow>(row: Row, position: number): Row => ({ ...row, position })
const branch = (rows: ActivityRow[], total: number) => ({
  rows,
  total: String(total),
  next: '',
  unavailable: false,
})
const positionOf = (row: ActivityRow): StopPosition => ({
  name: row.name,
  parents: recordParents(row),
})

describe('keyboard stops', () => {
  const run = traceRun('b', 1_000, 9_000)
  const first = traceTool('first', 'search', 100, 200)
  const last = traceTool('last', 'search', 9_500, 9_600)
  const call = (index: number) => traceCall(run, `c${index}`, [2_000, 2_000, 3_000])
  // task ▸ first, run ▸ (c0, c1, ·gap·, c4), ·gap·, last
  const page = activityPage({
    rows: [task],
    branches: {
      [task.name]: branch([at(first, 0), at(run, 1), at(last, 3)], 4),
      [run.name]: branch([at(call(0), 0), at(call(1), 1), at(call(4), 4)], 5),
    },
  })
  const store = absorbPage(openTrace(task.name), page)
  const stops = waterfallStops(waterfall(store, none, 0).rows)
  /** Every record ↓ or ↑ reaches from `from`, until it stops. */
  const walk = (from: ActivityRow | null, step: 1 | -1, list = stops) => {
    const reached: string[] = []
    let position = from && positionOf(from)
    for (let next = stepStop(list, position, step); next; next = stepStop(list, position, step)) {
      reached.push(next.row.name)
      position = stopPosition(next)
    }
    return reached
  }

  it.each<[string, ActivityRow | null, 1 | -1, ActivityRow[]]>([
    [
      '↓ with no cursor starts at the first record and stops at children that are not loaded',
      null,
      1,
      [task, first, run, call(0), call(1)],
    ],
    ['↑ with no cursor starts at the last record and stops under the gap', null, -1, [last]],
    ['↑ from the first record does not wrap', task, -1, []],
    ['↓ from the last record does not wrap', last, 1, []],
    ['↓ stops before a gap between child ordinals', call(1), 1, []],
    ['↑ stops after a gap between child ordinals', call(4), -1, []],
    ["↓ from a Run's last child stops at the gap that follows its Run", call(4), 1, []],
  ])('%s', (_name, from, step, expected) => {
    expect(walk(from, step)).toEqual(expected.map((row) => row.name))
  })

  it('stops at a history Coral withholds, from above and from below', () => {
    const withheld = absorbPage(
      openTrace(task.name),
      activityPage({
        rows: [task],
        branches: {
          [task.name]: branch([at(first, 0), at(run, 1), at(last, 2)], 3),
          [run.name]: { rows: [], next: '', unavailable: true },
        },
      }),
    )
    const list = waterfallStops(waterfall(withheld, none, 0).rows)
    expect(walk(null, 1, list)).toEqual([task.name, first.name, run.name])
    expect(walk(null, -1, list)).toEqual([last.name])
  })

  // Catches: ↓ resting on a Run's last child, or ↑ from a closed Run entering the Run
  // above it from the wrong end.
  it('leaves a run for the record after it, and passes a closed run in one step', () => {
    const second = traceRun('second', 9_700, 9_800)
    const whole = traceStore(task, {
      [task.name]: [first, run, second, last],
      [run.name]: [call(0), call(1)],
      [second.name]: [call(7)],
    })
    const open = waterfallStops(waterfall(whole, none, 0).rows)
    expect(walk(call(1), 1, open)).toEqual([second.name, call(7).name, last.name])
    const closed = waterfallStops(waterfall(whole, new Set([run.name]), 0).rows)
    expect(walk(second, -1, closed)).toEqual([run.name, first.name, task.name])
    expect(walk(first, 1, closed).slice(0, 2)).toEqual([run.name, second.name])
  })

  it('does nothing in an empty table', () => {
    expect(stepStop(waterfallStops([]), null, 1)).toBeNull()
  })

  it('rests a record inside a collapsed Run on that Run, and an expanded Run never stands in', () => {
    const collapsed = waterfallStops(waterfall(store, new Set([run.name]), 0).rows)
    const hidden = positionOf(call(4))
    const rested = collapsed[resolveStop(collapsed, hidden)]
    expect(rested?.kind === 'record' && rested.row.name).toBe(run.name)
    // The gap at the Task's third ordinal follows the Run, as when it shows its children.
    expect(stepStop(collapsed, hidden, 1)).toBeNull()
    expect(stepStop(collapsed, hidden, -1)?.row.name).toBe(first.name)
    // The Run is expanded but the trace lacks this call: no stop until it arrives.
    expect(resolveStop(stops, positionOf(call(3)))).toBe(-1)
    expect(stepStop(stops, positionOf(call(3)), 1)).toBeNull()
  })

  it('follows a submission to the Run it was accepted as', () => {
    const submission = traceTool('submitted', 'exec', 900, 950, { acceptedRun: run.name })
    const accepted = { ...at(run, 1), submission: { ...submission, position: undefined } }
    const replaced = absorbPage(
      openTrace(task.name),
      activityPage({
        rows: [task],
        branches: { [task.name]: branch([at(first, 0), accepted as ActivityRow], 2) },
      }),
    )
    const list = waterfallStops(waterfall(replaced, none, 0).rows)
    const rested = list[resolveStop(list, positionOf(submission))]
    expect(rested?.kind === 'record' && rested.row.name).toBe(run.name)
  })
})

describe('tool calls inside tool calls', () => {
  // A standalone Run: no Task encloses it.
  const run = traceRun('n', 1_000, 9_000, { task: '' })
  const tool = (id: string, parent?: ActivityRow) =>
    traceCall(run, id, [2_000, 2_000, 3_000], {
      toolKind: 'private',
      callPath: `coral.private.${id}`,
      parent:
        parent?.kind === 'invocation'
          ? { invocation: parent.name, callPath: parent.callPath, toolKind: 'private' }
          : undefined,
    })
  const outer = tool('outer')
  const inner = tool('inner', outer)
  const leaf = traceCall(run, 'leaf', [2_000, 2_000, 3_000], { parent: tool('x', inner).parent })
  const after = traceCall(run, 'after', [4_000, 4_000, 5_000])
  const store = traceStore(run, {
    [run.name]: [outer, after],
    [outer.name]: [inner],
    [inner.name]: [leaf],
  })
  const layout = (collapsed = none) =>
    recordRows(waterfall(store, collapsed, 0).rows).map((row) => [
      row.row.name,
      row.depth,
      row.expandable,
    ])

  // Catches: the calls a tool's body made listed beside the program's own, or not at all.
  it('lays each call out under the tool call whose body made it', () => {
    expect(layout()).toEqual([
      [run.name, 0, true],
      [outer.name, 1, true],
      [inner.name, 2, true],
      // A provider tool's call holds no calls.
      [leaf.name, 3, false],
      [after.name, 1, false],
    ])
    expect(layout(new Set([outer.name])).map(([name]) => name)).toEqual([
      run.name,
      outer.name,
      after.name,
    ])
  })

  // Catches: the keys skipping the calls inside an open tool call, or a key on one of them
  // collapsing the Run instead of the call that holds it.
  it('steps through each call inside the open tool call that holds it', () => {
    const stops = waterfallStops(waterfall(store, none, 0).rows).flatMap((stop) =>
      stop.kind === 'record' ? [[stop.row.name, stop.parents]] : [],
    )
    expect(stops).toEqual([
      [run.name, []],
      [outer.name, [run.name]],
      [inner.name, [outer.name, run.name]],
      [leaf.name, [inner.name, outer.name, run.name]],
      [after.name, [run.name]],
    ])
    // A record names only the nearest call: the trace gives the ones around it, so a
    // selected call opens every record that hides it.
    expect(recordParents(leaf)).toEqual([inner.name, run.name])
    expect(enclosingRecords(store, leaf)).toEqual([inner.name, outer.name, run.name])
  })

  // Catches: ↓ walking past the calls of a tool call that are not read yet.
  it('stops at the calls of a tool call that are not read yet', () => {
    const unread = absorbPage(
      openTrace(run.name),
      activityPage({
        rows: [run],
        branches: {
          [run.name]: branch([at(outer, 0), at(after, 1)], 2),
          [outer.name]: branch([at(inner, 1)], 2),
        },
      }),
    )
    const rows = waterfall(unread, none, 0).rows
    expect(rows.map((row) => (row.kind === 'gap' ? `gap ${row.parent}` : row.row.name))).toEqual([
      run.name,
      outer.name,
      `gap ${outer.name}`,
      inner.name,
      // No read has returned the calls of `inner` at all.
      `gap ${inner.name}`,
      after.name,
    ])
    expect(stepStop(waterfallStops(rows), positionOf(outer), 1)).toBeNull()
  })

  // Catches: a Run or a tool call whose window no read has returned yet shown as a record
  // with nothing under it, so ↓ steps past its children and nothing says they load.
  it('shows a record whose children no read has returned as not loaded, and stops there', () => {
    const open = traceTask()
    const first = traceRun('first', 0, 1_000)
    const second = traceRun('second', 2_000, 3_000)
    const unread = absorbPage(
      openTrace(open.name),
      activityPage({
        rows: [open],
        branches: { [open.name]: branch([at(first, 0), at(second, 1)], 2) },
      }),
    )
    const rows = waterfall(unread, none, 0).rows
    expect(
      rows.map((row) => (row.kind === 'gap' ? `${row.reason} ${row.parent}` : row.row.name)),
    ).toEqual([
      open.name,
      first.name,
      `unloaded ${first.name}`,
      second.name,
      `unloaded ${second.name}`,
    ])
    expect(recordRows(rows).map((row) => row.expandable)).toEqual([true, true, true])
    // ↓ from the first Run rests there until its children arrive.
    expect(stepStop(waterfallStops(rows), positionOf(first), 1)).toBeNull()
    const read = absorbPage(
      unread,
      activityPage({ rows: [open], branches: { [first.name]: branch([], 0) } }),
    )
    const stops = waterfallStops(waterfall(read, none, 0).rows)
    expect(stepStop(stops, positionOf(first), 1)).toMatchObject({ row: { name: second.name } })
  })

  // Catches: ← and → from a call that a tool's body made comparing its ordinal among that
  // body's calls with the Run's ordinals, so they pick a mark far from the call.
  describe('the mark beside a call', () => {
    const early = traceEvent(run, 1, 1_500, 'program.emitted')
    const middle = traceEvent(run, 2, 2_600, 'program.emitted')
    const late = traceEvent(run, 3, 6_000, 'program.emitted')
    // The body's third call starts between the first two marks: its ordinal there is 2.
    const third = traceCall(run, 'third', [2_500, 2_500, 2_550], { parent: inner.parent })
    const marked = absorbPage(
      openTrace(run.name),
      activityPage({
        rows: [run],
        branches: {
          [run.name]: branch(
            [at(early, 0), at(outer, 1), at(middle, 2), at(after, 3), at(late, 4)],
            5,
          ),
          [outer.name]: branch([at(inner, 0), at(tool('second', outer), 1), at(third, 2)], 3),
        },
      }),
    )
    const rows = waterfall(marked, none, 0).rows
    const held = (row: ActivityRow) =>
      traceChildren(
        marked,
        row.kind === 'invocation' ? (row.parent?.invocation ?? run.name) : '',
      ).find((item) => item.name === row.name)
    const beside = (row: ActivityRow, step: 1 | -1) =>
      adjacentMark(marked, rows, held(row), step)?.name

    it.each([
      ['a call a tool’s body made, by when it started', third, early.name, middle.name],
      ['the program’s own call, by its ordinal in the run', after, middle.name, late.name],
      ['the tool call around the others', outer, early.name, middle.name],
    ])('finds the marks before and after %s', (_, row, before, next) => {
      expect(beside(row, -1)).toBe(before)
      expect(beside(row, 1)).toBe(next)
    })

    it('steps from mark to mark, and from the run to its first or last mark', () => {
      expect(adjacentMark(marked, rows, middle, 1)?.name).toBe(late.name)
      expect(adjacentMark(marked, rows, late, 1)).toBeUndefined()
      expect(adjacentMark(marked, rows, run, 1)?.name).toBe(early.name)
      expect(adjacentMark(marked, rows, run, -1)?.name).toBe(late.name)
      // A Run with no mark takes neither key.
      expect(adjacentMark(store, waterfall(store, none, 0).rows, outer, 1)).toBeNull()
    })
  })
})

describe('the overview', () => {
  const run = traceRun('o', 0, 10_000)
  const calls = (count: number) =>
    Array.from({ length: count }, (_, index) =>
      traceCall(run, `o${index}`, [index * 100, index * 100, index * 100 + 50]),
    )
  const strip = (count: number, join = 0) =>
    overview(
      waterfall(traceStore(task, { [task.name]: [run], [run.name]: calls(count) }), none, 0).rows,
      10_000,
      join,
    )

  // Catches: an overview that joins everything into one bar, so its shape says nothing.
  it('gives each record a lane and a span of its own while the lanes last', () => {
    const { lanes, spans } = strip(3)
    // The Run and its three calls. The Task only frames them.
    expect(lanes).toBe(4)
    expect(spans.map((span) => [span.lane, span.kind, span.from - T0, span.to - T0])).toEqual([
      [0, 'run', 0, 10_000],
      [1, 'invocation', 0, 50],
      [2, 'invocation', 100, 150],
      [3, 'invocation', 200, 250],
    ])
  })

  // Catches: a strip with one element for each of thousands of records.
  it('shares the lanes of a long waterfall in row order, and joins close neighbours', () => {
    const { lanes, spans } = strip(79, 60)
    expect(lanes).toBe(8)
    // Ten rows to a lane: the Run with the first nine calls, then ten calls each.
    expect(spans.map((span) => [span.lane, span.kind])).toEqual([
      [0, 'run'],
      ...Array.from({ length: 8 }, (_, lane) => [lane, 'invocation']),
    ])
    expect(spans.at(-1)).toMatchObject({ from: T0 + 6_900, to: T0 + 7_850 })
    // Calls further apart than the strip joins stay apart.
    expect(strip(79, 0).spans.length).toBe(80)
  })

  it('marks a span in which a record failed, and says how each span ends', () => {
    const failed = traceRun('f', 0, 1_000, { state: 'failed' })
    const running = traceRun('g', 2_000, null)
    const lost = traceRun('h', 3_000, null, { state: 'unknown' })
    const store = traceStore(task, {
      [task.name]: [failed, running, lost],
      [failed.name]: [],
      [running.name]: [],
      [lost.name]: [],
    })
    const { spans, counts } = overview(waterfall(store, none, T0 + 9_000).rows, T0 + 9_000)
    expect(spans).toEqual([
      { lane: 0, kind: 'run', from: T0, to: T0 + 1_000, failed: true, end: 'ended' },
      // An open end reaches the end of the overview. An unknown one has no length.
      { lane: 1, kind: 'run', from: T0 + 2_000, to: T0 + 9_000, failed: false, end: 'ongoing' },
      { lane: 2, kind: 'run', from: T0 + 3_000, to: T0 + 3_000, failed: false, end: 'unknown' },
    ])
    expect(counts).toEqual({ runs: 3, calls: 0, failed: 1, ongoing: 1, unknownEnds: 1 })
  })

  // Catches: a span that reaches back over a neighbour that started earlier, so it covers
  // time in which nothing ran. Rows are in Coral's order: overlapping Runs and the calls a
  // tool's body made start out of order within one lane.
  it('never draws a span over a stretch in which none of its records worked', () => {
    // Nine calls in one Run, so they share lanes. The first one in the rows starts last.
    const times = [9_900, 200, 300, 400, 500, 600, 700, 800, 900]
    const out = traceRun('out', 0, 10_000)
    const nested = traceCall(out, 'holder', [100, 100, 9_950], {
      toolKind: 'private',
      callPath: 'coral.private.holder',
    })
    const made = times.map((start, index) =>
      traceCall(out, `m${index}`, [start, start, start + 50], {
        parent: { invocation: nested.name, callPath: nested.callPath, toolKind: 'private' },
      }),
    )
    const store = traceStore(task, {
      [task.name]: [out],
      [out.name]: [nested],
      [nested.name]: made,
    })
    const { spans } = overview(waterfall(store, none, 0).rows, T0 + 10_000, 60)
    const worked = [out, nested, ...made].map((row) => spanOf(row))
    for (const span of spans.filter((item) => item.kind === 'invocation' && item.lane > 0)) {
      // Every instant of the span is within 60 ms of a record of its lane at work.
      const inside = worked.filter(
        ({ start, end }) => end.kind === 'ended' && start >= span.from && end.at <= span.to,
      )
      const covered = inside.reduce(
        (sum, { start, end }) => sum + (end.kind === 'ended' ? end.at - start : 0),
        0,
      )
      expect(span.to - span.from - covered).toBeLessThanOrEqual(60 * inside.length)
    }
    // The call that starts last has a span of its own, far from the others.
    expect(spans.map((span) => [span.from - T0, span.to - T0])).toContainEqual([9_900, 9_950])
  })

  // Catches: neighbours joined by their distance in time on an axis that draws a long gap
  // short, or kept apart where the axis draws them on one point.
  it('joins neighbours by their distance on the axis it draws on', () => {
    const rows = waterfall(
      traceStore(task, { [task.name]: [run], [run.name]: calls(79) }),
      none,
      0,
    ).rows
    // An axis that draws every instant on one point joins each lane into one span.
    expect(overview(rows, 10_000, 0, () => 0).spans).toHaveLength(9)
    // An axis that draws the same calls far apart keeps each one.
    expect(overview(rows, 10_000, 60, (time) => time * 10).spans).toHaveLength(80)
  })
})

// Catches: a range label that says more time is compressed than the range it states holds,
// because it counts the whole of a gap that the view only cuts.
it('counts only the part of a compressed gap that the shown range holds', () => {
  const hour = 3_600_000
  const folds = [
    { from: 1_000, to: 1_000 + hour },
    { from: 2 * hour, to: 3 * hour },
  ]
  // The view starts half way through the first gap and ends before the second.
  expect(compressedTime(folds, 1_000 + hour / 2, 1_000 + hour + 50_000)).toBe(hour / 2)
  // Both gaps whole.
  expect(compressedTime(folds, 0, 4 * hour)).toBe(2 * hour)
  expect(compressedTime(folds, 0, 500)).toBe(0)
})
