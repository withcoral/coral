import { describe, expect, it } from 'vitest'
import { activityPage } from '@/views/activity/fixtures'
import type { ActivityBranch, ActivityPage, ActivityRow } from '@/views/activity/types'
import {
  MAX_LOADED_RECORDS,
  activityDays,
  failPage,
  liveRoots,
  loadsAutomatically,
  mergeActivityPage,
  openActivityList,
  pageQuery,
  receivePage,
  startPage,
  type ActivityList,
} from './activity-list'

const workspace = 'workspaces/w'
/** Record `n` started `n` minutes before midnight on 2 Oct, so a larger `n` is older. */
function run(n: number, overrides: Partial<Extract<ActivityRow, { kind: 'run' }>> = {}) {
  const startedAt = new Date(Date.UTC(2026, 9, 2) - n * 60_000).toISOString()
  return {
    kind: 'run' as const,
    name: `${workspace}/runs/r${n}`,
    workspace,
    agent: '',
    actor: 'you',
    task: '',
    state: 'succeeded' as const,
    startedAt,
    endedAt: startedAt,
    durationNanos: '1',
    requiresAction: false,
    ...overrides,
  }
}
function task(n: number, overrides: Partial<Extract<ActivityRow, { kind: 'task' }>> = {}) {
  const { startedAt } = run(n)
  return {
    kind: 'task' as const,
    name: `${workspace}/tasks/t${n}`,
    workspace,
    intent: `Task ${n}`,
    outcome: 'success',
    startedAt,
    endedAt: startedAt,
    actor: 'you',
    agent: '',
    ...overrides,
  }
}
const range = (from: number, to: number) =>
  Array.from({ length: to - from + 1 }, (_, index) => run(from + index))
const names = (list: ActivityList) => list.rows.map((row) => row.name.split('/').at(-1))
const branch = (rows: ActivityRow[] = []): ActivityBranch => ({
  rows,
  next: '',
  unavailable: false,
})
const page = (overrides: Partial<ActivityPage>) => activityPage(overrides)

describe('pages', () => {
  it.each([
    {
      direction: 'older' as const,
      opened: page({ rows: range(1, 3), hasAfter: true }),
      request: { after: `${workspace}/runs/r3` },
      // The page repeats r3; the list keeps one r3, in place.
      received: page({ rows: range(3, 6), hasAfter: false }),
      rows: ['r1', 'r2', 'r3', 'r4', 'r5', 'r6'],
      hasNewer: false,
      hasOlder: false,
    },
    {
      direction: 'newer' as const,
      opened: page({ rows: range(4, 6), hasBefore: true }),
      request: { before: `${workspace}/runs/r4` },
      received: page({ rows: range(2, 4), hasBefore: true }),
      rows: ['r2', 'r3', 'r4', 'r5', 'r6'],
      hasNewer: true,
      hasOlder: false,
    },
  ])(
    'loads $direction records next to the loaded ones without duplicates',
    ({ direction, opened, request, received, rows, hasNewer, hasOlder }) => {
      const list = openActivityList(opened)
      expect(pageQuery(list, direction)).toEqual(request)
      const loading = startPage(list, direction)
      expect(loading.loading).toEqual({ direction, from: Object.values(request)[0] })
      const loaded = receivePage(loading, direction, received)
      expect(names(loaded)).toEqual(rows)
      expect(loaded).toMatchObject({ hasNewer, hasOlder, loading: null, failed: null })
      // The records that were on screen keep their order.
      const kept = opened.rows.map((row) => row.name.split('/').at(-1)!)
      expect(names(loaded).filter((name) => kept.includes(name!))).toEqual(kept)
    },
  )
  it('requests no page in a direction that has nothing more, or while one is loading', () => {
    const list = openActivityList(page({ rows: range(1, 2), hasAfter: true }))
    expect(pageQuery(list, 'newer')).toBeNull()
    expect(pageQuery(startPage(list, 'older'), 'older')).toBeNull()
  })
  it('ignores a page that no longer continues the list', () => {
    const list = startPage(openActivityList(page({ rows: range(1, 2), hasAfter: true })), 'older')
    const moved: ActivityList = { ...list, rows: [...list.rows, run(3)] }
    expect(receivePage(moved, 'older', page({ rows: range(4, 5) }))).toBe(moved)
    expect(receivePage(list, 'newer', page({ rows: range(4, 5) }))).toBe(list)
  })
  it('keeps the loaded records when a page fails, and loads again on retry', () => {
    const opened = openActivityList(page({ rows: range(1, 2), hasAfter: true }))
    for (const failed of [
      failPage(startPage(opened, 'older'), 'older'),
      receivePage(startPage(opened, 'older'), 'older', page({ listUnavailable: true })),
    ]) {
      expect(failed).toMatchObject({ rows: opened.rows, loading: null, failed: 'older' })
      expect(loadsAutomatically(failed)).toBe(false)
      expect(pageQuery(failed, 'older')).toEqual({ after: `${workspace}/runs/r2` })
      expect(startPage(failed, 'older')).toMatchObject({
        failed: null,
        loading: { direction: 'older' },
      })
    }
  })
  it(`loads automatically only until ${MAX_LOADED_RECORDS} records are loaded`, () => {
    const below = openActivityList(page({ rows: range(1, MAX_LOADED_RECORDS - 1), hasAfter: true }))
    expect(loadsAutomatically(below)).toBe(true)
    const full = receivePage(
      startPage(below, 'older'),
      'older',
      page({ rows: [run(MAX_LOADED_RECORDS)], hasAfter: true }),
    )
    expect(full.rows).toHaveLength(MAX_LOADED_RECORDS)
    expect(loadsAutomatically(full)).toBe(false)
    // The button still loads more.
    expect(pageQuery(full, 'older')).not.toBeNull()
  })
  it('adds the branches of expanded records that a page brings', () => {
    const list = openActivityList(
      page({
        rows: [run(1)],
        hasAfter: true,
        expanded: [run(1).name, run(2).name],
        branches: { [run(1).name]: branch() },
      }),
    )
    const loaded = receivePage(
      startPage(list, 'older'),
      'older',
      page({
        rows: [run(2)],
        expanded: [run(2).name],
        branches: { [run(2).name]: branch([run(9)]) },
      }),
    )
    expect(Object.keys(loaded.branches)).toEqual([run(1).name, run(2).name])
  })
})

describe('merging', () => {
  const opened = openActivityList(
    page({
      rows: [run(1), run(2, { state: 'running' }), run(3)],
      hasAfter: true,
      expanded: [run(2).name, run(3).name],
      branches: { [run(2).name]: branch(), [run(3).name]: branch([run(30)]) },
    }),
  )
  it('replaces watched records in place and never drops or reorders the others', () => {
    const finished = run(2, { state: 'failed' })
    const merged = mergeActivityPage(
      opened,
      page({
        rows: [finished, run(1)],
        expanded: [run(2).name, run(3).name],
        branches: { [run(2).name]: branch([run(20)]) },
        // A watched window reports nothing about the list around it.
        hasBefore: true,
        anchorUnavailable: true,
      }),
      { retained: true },
    )!
    expect(names(merged)).toEqual(['r1', 'r2', 'r3'])
    expect(merged.rows[1]).toBe(finished)
    expect(merged.branches[run(2).name]!.rows).toEqual([run(20)])
    expect(merged.branches[run(3).name]).toBe(opened.branches[run(3).name])
    expect(merged).toMatchObject({ hasNewer: false, hasOlder: true, anchorUnavailable: false })
  })
  it('drops watched records that are not loaded', () => {
    const merged = mergeActivityPage(opened, page({ rows: [run(0), run(9)] }), { retained: true })!
    expect(names(merged)).toEqual(['r1', 'r2', 'r3'])
  })
  it('drops the branches of records that are no longer expanded', () => {
    const merged = mergeActivityPage(opened, page({ rows: [run(1)], expanded: [run(3).name] }), {
      retained: true,
    })!
    expect(Object.keys(merged.branches)).toEqual([run(3).name])
  })
  it.each([
    {
      name: 'puts arrivals at the newest edge above the loaded records',
      incoming: page({ rows: [run(-1), run(0), run(1), run(2)], hasAfter: true }),
      rows: ['r-1', 'r0', 'r1', 'r2', 'r3'],
      hasOlder: true,
    },
    {
      name: 'puts arrivals at the newest edge even when none of the loaded records is in the page',
      incoming: page({ rows: [run(-2), run(-1)], hasAfter: true }),
      rows: ['r-2', 'r-1', 'r1', 'r2', 'r3'],
      hasOlder: true,
    },
    {
      name: 'continues past the last loaded record and takes its end of history',
      incoming: page({ rows: [run(2), run(3), run(4)], hasBefore: true, hasAfter: false }),
      rows: ['r1', 'r2', 'r3', 'r4'],
      hasOlder: false,
    },
  ])('$name', ({ incoming, rows, hasOlder }) => {
    const merged = mergeActivityPage(opened, incoming, { retained: false })!
    expect(names(merged)).toEqual(rows)
    expect(merged).toMatchObject({ hasNewer: false, hasOlder })
  })
  it('does not join a page that neither touches the list nor starts at its newest edge', () => {
    const later = openActivityList(page({ rows: range(30, 32), hasBefore: true, hasAfter: true }))
    expect(
      mergeActivityPage(later, page({ rows: range(1, 3), hasAfter: true }), { retained: false }),
    ).toBeNull()
    expect(
      mergeActivityPage(opened, page({ rows: range(50, 52), hasBefore: true, hasAfter: true }), {
        retained: false,
      }),
    ).toBeNull()
  })
  it('joins the records that a page puts right above a list with newer records to load', () => {
    const later = openActivityList(page({ rows: range(30, 32), hasBefore: true, hasAfter: true }))
    const merged = mergeActivityPage(
      later,
      page({ rows: [run(29), ...range(30, 31)], hasBefore: true }),
      { retained: false },
    )!
    expect(names(merged)).toEqual(['r29', 'r30', 'r31', 'r32'])
    expect(merged).toMatchObject({ hasNewer: true, hasOlder: true })
  })
})

describe('days', () => {
  it('gives each UTC day one group across loaded pages', () => {
    // r1 and r2 start on 1 Oct; r1500 starts on 30 Sep.
    let list = openActivityList(page({ rows: [run(1)], hasAfter: true }))
    list = receivePage(startPage(list, 'older'), 'older', page({ rows: [run(2), run(1500)] }))
    expect(
      activityDays(list.rows).map(({ day, key, rows }) => ({ day, key, rows: rows.length })),
    ).toEqual([
      { day: '2026-10-01', key: '2026-10-01', rows: 2 },
      { day: '2026-09-30', key: '2026-09-30', rows: 1 },
    ])
  })
  it('keys a repeated day apart, so each group keeps a distinct key', () => {
    const days = activityDays([run(1), run(1500), run(2)])
    expect(days.map(({ key }) => key)).toEqual([
      '2026-10-01',
      '2026-09-30',
      `2026-10-01:${run(2).name}`,
    ])
  })
})

describe('live records', () => {
  const rows = [
    run(1, { state: 'running' }),
    task(2, { endedAt: null }),
    // An ended Task stays live while one of its Runs still runs.
    task(3, { contextImpactPartial: true }),
    // Without a context impact total, Coral does not say whether a Run still runs.
    task(4, { contextImpactBytes: undefined }),
    run(5, { state: 'unknown' }),
    ...range(6, 40),
    run(41, { state: 'running' }),
  ]
  const list = openActivityList(page({ rows, hasAfter: true }))
  it.each([
    {
      name: 'watches running records and expanded ones near the viewport',
      visible: [run(6).name],
      expanded: [run(8).name],
      roots: [run(1).name, task(2).name, task(3).name, run(5).name, run(8).name],
    },
    {
      name: 'watches an ended Task with no context impact total only while it is expanded',
      visible: [task(4).name],
      expanded: [task(4).name],
      roots: [run(1).name, task(2).name, task(3).name, task(4).name, run(5).name],
    },
    {
      name: 'stops watching records far from the viewport',
      visible: [run(40).name],
      expanded: [run(8).name],
      roots: [run(41).name],
    },
    {
      name: 'watches nothing when no loaded record is on screen',
      visible: [`${run(6).name}/events/1`],
      expanded: [],
      roots: [],
    },
  ])('$name', ({ visible, expanded, roots }) => {
    expect(liveRoots(list, visible, expanded)).toEqual(roots)
  })
  it('watches at most 25 records, nearest to the viewport first', () => {
    const running = openActivityList(
      page({ rows: Array.from({ length: 40 }, (_, n) => run(n, { state: 'running' })) }),
    )
    const roots = liveRoots(
      running,
      range(10, 35).map((row) => row.name),
      [],
    )
    expect(roots).toHaveLength(25)
    expect(roots).toEqual(range(10, 34).map((row) => row.name))
  })
})
