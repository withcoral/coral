import { describe, expect, it } from 'vitest'
import { activityPage, invocationRow, runRow, taskRow, toolRow } from './fixtures'
import {
  activityNavigation,
  activityStops,
  liveRun,
  liveStep,
  recordParents,
  resolveStop,
  stepStop,
  stopPosition,
  type ActivityStop,
  type StopPosition,
} from './navigation'
import type { ActivityPage, ActivityRow } from './types'

const task = 'workspaces/w/tasks/t'
const run = 'workspaces/w/runs/r'
const page = activityPage({
  expanded: [task, run],
  hasBefore: true,
  hasAfter: true,
  selected: run,
})
const reading = {
  anchor: task,
  branches: {
    [task]: { offset: 125, anchor: `${task}/interactions/i`, preceding: 25 },
    [run]: { offset: 300, anchor: `${run}/events/320`, preceding: 20 },
  },
}
const target = (href: string) => new URL(href, 'http://reef').searchParams

it('keeps a revealed terminal Event identity when buffering predecessors before closing detail', () => {
  const terminal = `${run}/events/301`
  const current: ActivityPage = {
    ...page,
    anchors: { [run]: terminal },
    branches: {
      [run]: {
        rows: [
          {
            kind: 'output',
            name: terminal,
            position: 299,
            workspace: 'workspaces/w',
            run,
            label: 'execution ended',
            startedAt: '1970-01-01T00:00:00.000Z',
            eventKind: 'run.ended',
            agent: '',
          },
        ],
        offset: 200,
        total: '300',
        next: '',
        unavailable: false,
      },
    },
  }
  const nav = activityNavigation(
    current,
    '/activity',
    `?tree=1&expand=${run}&anchor.${run}=${terminal}`,
    {
      ...reading,
      branches: { [run]: { offset: 200, anchor: '', preceding: 0 } },
    },
  )
  const closed = target(nav.backHref)
  expect(closed.get(`anchor.${run}`)).toBe(terminal)
  expect(closed.get(`preceding.${run}`)).toBe('99')
})

it('does not subtract predecessors twice when navigating from a newly loaded gap window', () => {
  const first = `${run}/events/277`
  const current: ActivityPage = {
    ...page,
    branches: {
      [run]: {
        rows: [
          {
            kind: 'output',
            name: first,
            position: 275,
            workspace: 'workspaces/w',
            run,
            label: 'emission #277',
            startedAt: '1970-01-01T00:00:00.000Z',
            eventKind: 'program.emitted',
            agent: '',
          },
        ],
        offset: 275,
        total: '500',
        next: '',
        unavailable: false,
      },
    },
  }
  const nav = activityNavigation(current, '/activity', `?tree=1&expand=${run}`, {
    ...reading,
    branches: { [run]: { offset: 275, anchor: '', preceding: 25 } },
  })
  const next = target(nav.recordHref(first))
  expect(next.get(`offset.${run}`)).toBe('275')
  expect(next.get(`anchor.${run}`)).toBe(first)
  expect(next.get(`preceding.${run}`)).toBe('0')
})

it('converts a revealed accepted Run into its interaction identity for a Task viewport', () => {
  const interaction = `${task}/interactions/submission`
  const at = '1970-01-01T00:00:00.000Z'
  const current: ActivityPage = {
    ...page,
    anchors: { [task]: run },
    branches: {
      [task]: {
        rows: [
          {
            kind: 'run',
            name: run,
            workspace: 'workspaces/w',
            task,
            agent: '',
            actor: 'you',
            state: 'succeeded',
            startedAt: at,
            endedAt: at,
            durationNanos: '0',
            requiresAction: false,
            position: 50,
            submission: {
              name: interaction,
              tool: 'exec',
              intent: 'Run work',
              outcome: 'succeeded',
              startedAt: at,
              completedAt: at,
              acceptedRun: run,
              agent: '',
            },
          },
        ],
        offset: 25,
        next: '',
        unavailable: false,
      },
    },
  }
  const nav = activityNavigation(
    current,
    '/activity',
    `?tree=1&expand=${task}&anchor.${task}=${run}`,
    {
      ...reading,
      branches: { [task]: { offset: 25, anchor: '', preceding: 0 } },
    },
  )
  expect(target(nav.backHref).get(`anchor.${task}`)).toBe(interaction)
  expect(target(nav.backHref).get(`preceding.${task}`)).toBe('25')
})

it('carries bounded reader identities and anchored child demand when selecting, expanding, or closing detail', () => {
  const nav = activityNavigation(page, '/activity', '', reading)
  for (const href of [
    nav.recordHref(run),
    nav.toggleHref('workspaces/w/runs/other'),
    nav.backHref,
  ]) {
    const query = target(href)
    // A reload opens at the first record on screen.
    expect(query.get('anchor')).toBe(task)
    expect(query.get(`anchor.${task}`)).toBe(`${task}/interactions/i`)
    expect(query.get(`offset.${task}`)).toBe('125')
    expect(query.get(`preceding.${task}`)).toBe('25')
    expect(query.get(`anchor.${run}`)).toBe(`${run}/events/320`)
    expect(href).not.toContain('total')
  }
  expect(target(nav.backHref).has('record')).toBe(false)
})
it('writes the reading anchor for selection, not for explicit root navigation or pending-action pages', () => {
  const nav = activityNavigation(page, '/activity', '?anchor=old', reading)
  expect(target(nav.recordHref(run)).get('anchor')).toBe(task)
  expect(target(nav.href({ anchor: run })).get('anchor')).toBe(run)
  expect(target(nav.href({ anchor: null })).has('anchor')).toBe(false)
  expect(target(nav.href({ pendingAfter: 'next' })).get('anchor')).toBe('old')
})
it('removes collapsed branch coordinates and never accumulates a child-window history', () => {
  const nav = activityNavigation(
    page,
    '/activity',
    `?tree=1&expand=${task}&expand=${run}&cursor.${run}=old`,
    reading,
  )
  const query = target(nav.toggleHref(run))
  expect(query.getAll('expand')).toEqual([task])
  for (const prefix of ['offset', 'anchor', 'preceding', 'cursor']) {
    expect(query.has(`${prefix}.${run}`)).toBe(false)
  }
  expect(query.get(`offset.${task}`)).toBe('125')
})
it('preserves stable Run selection while returning to its personal list context', () => {
  const nav = activityNavigation(page, `/${run}`, '?workspace=workspaces/w', reading)
  expect(target(nav.toggleHref(task)).get('record')).toBe(run)
  expect(target(nav.backHref).has('record')).toBe(false)
  expect(target(nav.backHref).get('anchor')).toBe(task)
})
it('names no anchor at the newest records and drops the coordinates of page links', () => {
  const nav = activityNavigation(
    page,
    '/activity',
    `?after=older&anchor=old&rootMode=page&root=${task}`,
    {
      ...reading,
      anchor: null,
    },
  )
  const query = target(nav.recordHref(run))
  for (const key of ['anchor', 'after', 'before', 'rootMode', 'root'])
    expect(query.has(key)).toBe(false)
})

const output = (parent: string, id: string, position: number): ActivityRow => ({
  kind: 'output',
  name: `${parent}/${id}`,
  workspace: 'workspaces/finance',
  run: parent,
  label: id,
  startedAt: '2026-09-18T14:02:00.000Z',
  eventKind: 'program.emitted',
  agent: '',
  position,
})
const childPage = (rows: ActivityRow[], total: number) => ({
  rows,
  total: String(total),
  next: '',
  unavailable: false,
})
const positionOf = (row: ActivityRow): StopPosition => ({
  name: row.name,
  parents: recordParents(row),
})
/** The record a position rests on, if any. */
const restsOn = (list: ActivityStop[], position: StopPosition) => {
  const stop = list[resolveStop(list, position)]
  return stop?.kind === 'record' ? stop.row.name : undefined
}

describe('keyboard traversal', () => {
  const [a, c, d, e] = ['a', 'c', 'd', 'e'].map((id) => taskRow(id))
  const b = runRow('b', a.name, { position: 1 })
  const first = toolRow(a.name, 'first', { position: 0 })
  const last = toolRow(a.name, 'last', { position: 3 })
  const emitted = output(b.name, 'events/1', 0)
  const logged = output(b.name, 'events/2', 1)
  const final = output(b.name, 'final', 4)
  // a ▸ first, b ▸ (emitted, logged, ·gap·, final), ·gap·, last; c collapsed;
  // d's history is unavailable; e.
  const tree = activityPage({
    rows: [a, c, d, e],
    expanded: [a.name, b.name, d.name],
    branches: {
      [a.name]: childPage([first, b, last], 4),
      [b.name]: childPage([emitted, logged, final], 5),
      [d.name]: { ...childPage([], 0), unavailable: true },
    },
  })
  const stops = activityStops(tree, tree.rows)
  /** Every record ↓ or ↑ reaches from `from`, until it stops. */
  const walk = (from: ActivityRow | null, step: 1 | -1) => {
    const reached: string[] = []
    let position = from && positionOf(from)
    for (let next = stepStop(stops, position, step); next; next = stepStop(stops, position, step)) {
      reached.push(next.row.name)
      position = stopPosition(next)
    }
    return reached
  }

  it.each<[string, ActivityRow | null, 1 | -1, ActivityRow[]]>([
    [
      '↓ with no cursor starts at the first record and stops at a gap',
      null,
      1,
      [a, first, b, emitted, logged],
    ],
    ['↑ with no cursor starts at the last record and stops at unavailable history', null, -1, [e]],
    ['↑ from the first record does not wrap', a, -1, []],
    ['↓ from the last record does not wrap', e, 1, []],
    ['↓ stops before a gap between child ordinals', logged, 1, []],
    ['↑ stops after a gap between child ordinals', final, -1, []],
    ['↓ from a final value stops at the gap that follows its Run', final, 1, []],
    ['↓ leaves a branch for the next loaded sibling', last, 1, [c, d]],
    ['↑ from a collapsed record reaches the record above its sibling branch', c, -1, [last]],
    ['↓ stops at unavailable history', c, 1, [d]],
    ['↑ stops at unavailable history from below', e, -1, []],
  ])('%s', (_name, from, step, expected) => {
    expect(walk(from, step)).toEqual(expected.map((row) => row.name))
  })

  it('does nothing in an empty table', () => {
    expect(stepStop(activityStops(activityPage(), []), null, 1)).toBeNull()
  })

  it('rests a record inside a collapsed branch on that branch, and an expanded branch never stands in', () => {
    const collapsed = activityStops({ ...tree, expanded: [a.name, d.name] }, tree.rows)
    const hidden = positionOf(final)
    expect(restsOn(collapsed, hidden)).toBe(b.name)
    // The gap at ordinal 2 follows b, as when it shows its children.
    expect(stepStop(collapsed, hidden, 1)).toBeNull()
    expect(stepStop(collapsed, hidden, -1)?.row.name).toBe(first.name)
    // b is expanded but its window lacks this record: no stop until it arrives.
    expect(resolveStop(stops, positionOf(output(b.name, 'events/3', 2)))).toBe(-1)
    expect(stepStop(stops, positionOf(output(b.name, 'events/3', 2)), 1)).toBeNull()
  })

  it('names the parents of a final value that only its URL identifies', () => {
    expect(recordParents(b, `${b.name}/final`)).toEqual([b.name, a.name])
    expect(recordParents(b)).toEqual([a.name])
  })

  it('follows a submission to the Run it was accepted as', () => {
    const submission = toolRow(a.name, 'submitted', { position: 1, acceptedRun: b.name })
    const accepted = { ...b, submission: { ...submission, position: undefined } } as ActivityRow
    const replaced = activityStops(
      { ...tree, branches: { ...tree.branches, [a.name]: childPage([first, accepted, last], 4) } },
      tree.rows,
    )
    expect(restsOn(replaced, positionOf(submission))).toBe(b.name)
  })
})

describe('live mode', () => {
  const owner = runRow('live', taskRow('t').name, { state: 'running' })
  const standalone = runRow('alone', '', { state: 'running' })
  const call = invocationRow(owner.name, 'call', { task: owner.task, position: 0 })
  it.each<[string, ActivityRow | null, ReturnType<typeof liveRun>]>([
    ['a Run follows itself', owner, { run: owner.name, chain: [owner.task, owner.name] }],
    [
      'a standalone Run has no Task',
      standalone,
      { run: standalone.name, chain: [standalone.name] },
    ],
    [
      'an operation follows the Run that called it',
      call,
      { run: owner.name, chain: [owner.task, owner.name] },
    ],
    [
      'a log follows its Run',
      output(owner.name, 'events/2', 1),
      { run: owner.name, chain: [owner.name] },
    ],
    ['a Task has no Run to follow', taskRow('t'), null],
    ['a tool call has no Run to follow', toolRow('t', 'i'), null],
    ['nothing selected follows nothing', null, null],
  ])('%s', (_, row, expected) => {
    expect(liveRun(row)).toEqual(expected)
  })

  const branchPage = (rows: ActivityRow[], total: number, offset = 0) =>
    activityPage({
      expanded: [owner.name],
      branches: { [owner.name]: { ...childPage(rows, total), offset } },
    })
  const log = output(owner.name, 'events/3', 1)
  const emission = output(owner.name, 'events/4', 2)
  const final = output(owner.name, 'final', 3)
  const other = runRow('other', '')
  it.each<[string, ActivityPage, ReturnType<typeof liveStep>]>([
    [
      'selects the newest child by its position',
      branchPage([log, call, emission], 3),
      {
        kind: 'select',
        record: emission.name,
      },
    ],
    [
      'selects the final value once the Run ends',
      branchPage([call, log, emission, final], 4),
      {
        kind: 'select',
        record: final.name,
      },
    ],
    [
      'selects the Run itself while it has no children',
      branchPage([], 0),
      {
        kind: 'select',
        record: owner.name,
      },
    ],
    // The window holds positions 0–2 of 5: the newest child is not loaded yet.
    [
      'loads the end when the newest child is past the window',
      branchPage([call, log, emission], 5),
      {
        kind: 'load',
        total: 5,
      },
    ],
    [
      'loads the end when the window holds no children',
      branchPage([], 7, 0),
      {
        kind: 'load',
        total: 7,
      },
    ],
    ['expands a collapsed Run', activityPage(), { kind: 'expand' }],
    [
      'waits for an expanded Run whose children are not loaded',
      activityPage({
        expanded: [owner.name],
      }),
      { kind: 'wait' },
    ],
    [
      'waits on an unavailable history',
      activityPage({
        expanded: [owner.name],
        branches: { [owner.name]: { rows: [], next: '', unavailable: true } },
      }),
      { kind: 'wait' },
    ],
    // Another expanded Run's newer records never count.
    [
      'never follows another Run',
      activityPage({
        expanded: [owner.name, other.name],
        branches: {
          [owner.name]: childPage([log], 1),
          [other.name]: childPage(
            [output(other.name, 'events/9', 0), output(other.name, 'events/10', 1)],
            2,
          ),
        },
      }),
      { kind: 'select', record: log.name },
    ],
  ])('%s', (_, current, expected) => {
    expect(liveStep(current, owner.name)).toEqual(expected)
  })

  // The loader names the Run's oldest action; it may be outside the loaded window.
  const waiting = invocationRow(owner.name, 'waiting', { position: 0, requiresAction: true })
  const action = { run: owner.name, record: waiting.name, request: 'workspaces/w/approvals/a' }
  const withAction = (current: ActivityPage, value = action): ActivityPage => ({
    ...current,
    liveAction: value,
  })
  it.each<[string, ActivityPage, ReturnType<typeof liveStep>]>([
    [
      'selects the oldest action over newer output',
      withAction(branchPage([waiting, log, emission], 3)),
      { kind: 'select', record: waiting.name, request: action.request },
    ],
    [
      'selects an action outside the loaded window',
      withAction(branchPage([emission], 3, 2)),
      { kind: 'select', record: waiting.name, request: action.request },
    ],
    [
      'selects an action before the Run expands',
      withAction(activityPage()),
      { kind: 'select', record: waiting.name, request: action.request },
    ],
    [
      'selects an account connection on the Run',
      withAction(branchPage([log], 1), { ...action, record: owner.name }),
      { kind: 'select', record: owner.name, request: action.request },
    ],
    [
      'waits while a live update says the action no longer waits',
      withAction(branchPage([{ ...waiting, requiresAction: false }, log], 2)),
      { kind: 'wait' },
    ],
    [
      "never selects another Run's action",
      withAction(branchPage([log, emission], 2), { ...action, run: other.name }),
      { kind: 'select', record: emission.name },
    ],
  ])('%s', (_, current, expected) => {
    expect(liveStep(current, owner.name)).toEqual(expected)
  })
  it('follows the newest record when you cannot act, such as in an archived workspace', () => {
    expect(liveStep(withAction(branchPage([log, emission], 2)), owner.name, false)).toEqual({
      kind: 'select',
      record: emission.name,
    })
  })

  const search = `?tree=1&expand=${task}&expand=${run}&record=${run}&live=1&filter=kept`
  const nav = activityNavigation(page, '/activity', search, reading)
  it('turns off when you pick a record, collapse the shown one, or close the detail', () => {
    for (const href of [nav.recordHref(task), nav.collapseHref(run), nav.backHref]) {
      expect(target(href).has('live')).toBe(false)
      expect(target(href).get('filter')).toBe('kept')
    }
  })
  it('stays on while you expand a record', () => {
    expect(target(nav.toggleHref('workspaces/w/runs/other')).get('live')).toBe('1')
  })
  it('follows to a record and keeps the reading window', () => {
    const query = target(nav.followHref(`${run}/events/321`))
    expect(query.get('record')).toBe(`${run}/events/321`)
    expect(query.get('live')).toBe('1')
    expect(query.get('filter')).toBe('kept')
    expect(query.get(`offset.${task}`)).toBe('125')
  })
  it('opens an action with its request, at the action when the window does not hold it', () => {
    const outside = {
      run,
      record: 'workspaces/w/invocations/old',
      request: 'workspaces/w/approvals/a',
    }
    const query = target(nav.followHref(outside.record, outside))
    expect(query.get('record')).toBe(outside.record)
    expect(query.get('request')).toBe(outside.request)
    expect(query.get('live')).toBe('1')
    expect(query.get(`anchor.${run}`)).toBe(outside.record)
    expect(query.has(`offset.${run}`)).toBe(false)
    expect(query.has(`preceding.${run}`)).toBe(false)
    expect(query.get(`offset.${task}`)).toBe('125')
  })
  it('keeps the reading window for an action it holds, and keeps live mode off after a decision', () => {
    const loaded = invocationRow(run, 'loaded', { position: 310, requiresAction: true })
    const current = activityPage({
      ...page,
      branches: { [run]: { ...childPage([loaded], 400), offset: 300 } },
    })
    const off = activityNavigation(
      current,
      '/activity',
      `?tree=1&expand=${task}&expand=${run}`,
      reading,
    )
    const held = { run, record: loaded.name, request: 'workspaces/w/approvals/b' }
    const query = target(off.followHref(held.record, held, false))
    expect(query.get('record')).toBe(loaded.name)
    expect(query.get('request')).toBe(held.request)
    expect(query.has('live')).toBe(false)
    expect(query.get(`offset.${run}`)).toBe('300')
  })
  it('asks for the end of the Run without moving the selection', () => {
    const query = target(nav.followEndHref(run, 400))
    expect(query.get('record')).toBe(run)
    expect(query.get('live')).toBe('1')
    // An offset at or past the end reads the last window.
    expect(query.get(`offset.${run}`)).toBe('400')
    expect(query.get(`preceding.${run}`)).toBe('0')
    expect(query.has(`anchor.${run}`)).toBe(false)
    expect(query.get(`offset.${task}`)).toBe('125')
  })
  it('follows from a Run page to the list, which can select any record of the Run', () => {
    const route = activityNavigation(page, `/${run}`, '?live=1', reading)
    const next = new URL(route.followHref(`${run}/events/3`), 'http://reef')
    expect(next.pathname).toBe(page.listPath)
    expect(next.searchParams.get('record')).toBe(`${run}/events/3`)
    expect(route.live).toBe(true)
  })
  it('turns on with the Run and its Task expanded, and turns off in place', () => {
    const off = activityNavigation(page, '/activity', `?tree=1&record=${run}&filter=kept`, reading)
    const on = target(off.liveHref(true, [task, run]))
    expect(on.get('live')).toBe('1')
    expect(on.get('record')).toBe(run)
    expect(on.getAll('expand')).toEqual(expect.arrayContaining([task, run]))
    expect(on.get('filter')).toBe('kept')
    const stopped = target(nav.liveHref(false))
    expect(stopped.has('live')).toBe(false)
    expect(stopped.get('record')).toBe(run)
  })
})
