import { describe, expect, it } from 'vitest'
import { activityPage } from './fixtures'
import { activityNavigation, activitySelection, readsActivity, revealHref } from './navigation'
import {
  invocation,
  outputRow,
  run,
  runName,
  task,
  taskName,
  workspace,
} from './side-panel-fixtures'
import type { ActivityTrace } from './types'

const standalone = run({ name: `${workspace}/runs/export`, task: '' })
const trace: ActivityTrace = { root: task(), children: {}, complete: true, unavailable: [] }
const query = (href: string) => Object.fromEntries(new URL(href, 'https://reef.test').searchParams)

describe('revealHref', () => {
  // Catches: an Approvals or Agent link opening the wrong root for its record.
  it.each([
    { label: "a call opens its Task's timeline", row: invocation(), root: taskName },
    { label: 'a Run in a Task opens the Task', row: run(), root: taskName },
    { label: 'a standalone Run opens itself', row: standalone, root: standalone.name },
    {
      label: "a standalone Run's Event opens its Run",
      row: {
        ...outputRow(
          {
            sequence: '2',
            occurredAt: '2026-09-18T14:02:00.000Z',
            kind: 'program.emitted',
            value: '{}',
          },
          'emission #2',
        ),
        task: '',
        run: standalone.name,
      },
      root: standalone.name,
    },
  ])('$label, with the record selected', ({ row, root }) => {
    expect(query(revealHref(row, { request: 'approval' }))).toEqual({
      root,
      record: row.name,
      request: 'approval',
    })
  })

  // Catches: an Agent's record opening in your own Activity.
  it("opens an Agent's record on the Agent's page", () => {
    expect(revealHref(run()).split('?')[0]).toBe(`/${run().agent}`)
  })
})

describe('activitySelection', () => {
  // Catches: a live update switching the open root to a newer one.
  it('pins the root the page opened, so a live update keeps it', () => {
    expect(activitySelection(activityPage({ trace }), 'record=x').get('root')).toBe(taskName)
  })

  // Catches: the pin replacing a root you chose.
  it('keeps the root the address names', () => {
    expect(activitySelection(activityPage({ trace }), `root=${standalone.name}`).get('root')).toBe(
      standalone.name,
    )
  })
})

describe('activityNavigation', () => {
  const page = activityPage({ trace, listPath: `/${workspace}/activity` })
  const nav = activityNavigation(
    page,
    page.listPath,
    `root=${taskName}&record=${runName}&request=r`,
    {
      anchor: taskName,
    },
  )

  // Catches: selecting a record closing its root or losing the list's place.
  it('selects a record in the open root, at the reading position', () => {
    expect(query(nav.recordHref('call'))).toEqual({
      root: taskName,
      record: 'call',
      anchor: taskName,
    })
  })

  // Catches: another root opening with the last root's record still selected.
  it('opens another root with nothing selected', () => {
    expect(query(nav.rootHref(standalone.name))).toEqual({
      root: standalone.name,
      anchor: taskName,
    })
  })

  // Catches: closing the detail closing the timeline too.
  it('closes the detail and keeps the root', () => {
    expect(query(nav.backHref)).toEqual({ root: taskName, anchor: taskName })
  })
})

describe('readsActivity', () => {
  // Catches: an Agent link with a root opening its dashboard instead of its full history.
  it.each([
    ['root=x', true],
    ['record=x', true],
    ['anchor=x', true],
    ['view=settings', false],
  ])('reads %s as %s', (search, reads) => {
    expect(readsActivity(new URLSearchParams(search))).toBe(reads)
  })
})
