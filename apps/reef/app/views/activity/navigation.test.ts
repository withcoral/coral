import { describe, expect, it } from 'vitest'
import { activityPage, runRow, taskRow, toolRow } from './fixtures'
import {
  activityNavigation,
  activityStops,
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
