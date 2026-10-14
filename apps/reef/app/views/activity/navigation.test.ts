import { expect, it } from 'vitest'
import { activityPage } from './fixtures'
import { activityNavigation, activitySelection } from './navigation'
import type { ActivityPage } from './types'

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

// Catches: links and observation keeping the window the URL named after the loader
// moved a Run's window to hold the selected final value.
it('pins the Run window the loader moved for a selected final value', () => {
  const query = activitySelection(
    activityPage({ selected: run, initialRecord: `${run}/final`, expanded: [run] }),
    new URLSearchParams({
      record: `${run}/final`,
      [`offset.${run}`]: '0',
      [`anchor.${run}`]: `${run}/events/2`,
      [`preceding.${run}`]: '0',
    }).toString(),
  )
  expect(Object.fromEntries(query)).toEqual({
    record: `${run}/final`,
    [`anchor.${run}`]: `${run}/final`,
  })
})
