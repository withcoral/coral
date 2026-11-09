import { expect, it } from 'vitest'
import { invocationRow, runRow, workspace } from './fixtures'
import { callDepths } from './nesting'
import type { ActivityRow, ParentInvocation } from './types'

const run = runRow('one', '').name
const call = (id: string, parent?: string) =>
  invocationRow(run, id, {
    callPath: `coral.private.${id}`,
    parent: parent ? parentCall(parent) : undefined,
  })
const parentCall = (id: string): ParentInvocation => ({
  invocation: `${workspace}/invocations/${id}`,
  callPath: `coral.private.${id}`,
  toolKind: 'private',
})
const log = (sequence: number): ActivityRow => ({
  kind: 'output',
  name: `${run}/events/${sequence}`,
  workspace,
  run,
  label: `log #${sequence}`,
  startedAt: '2026-09-18T14:02:00.000Z',
  eventKind: 'program.logged',
  agent: '',
})
const depths = (rows: ActivityRow[]) =>
  rows.map((row) => [row.name.split('/').at(-1), callDepths(rows).get(row.name) ?? 0])

// Catches: a call a tool's body made that sits level with the program's own calls while it
// follows the call that made it, or one indented under a row that did not make it.
it.each<[string, ActivityRow[], [string, number][]]>([
  [
    'the program’s own calls sit at the top',
    [call('a'), call('b')],
    [
      ['a', 0],
      ['b', 0],
    ],
  ],
  [
    'each call sits one level under the call that made it',
    [call('outer'), call('inner', 'outer'), call('leaf', 'inner'), call('next', 'outer')],
    [
      ['outer', 0],
      ['inner', 1],
      ['leaf', 2],
      ['next', 1],
    ],
  ],
  [
    'a log between a call and its caller does not part them',
    [call('outer'), log(2), call('inner', 'outer')],
    [
      ['outer', 0],
      ['2', 0],
      ['inner', 1],
    ],
  ],
  [
    'a call that another call parts from its caller sits at the top',
    [call('outer'), call('other'), call('inner', 'outer'), call('leaf', 'inner')],
    [
      ['outer', 0],
      ['other', 0],
      ['inner', 0],
      ['leaf', 1],
    ],
  ],
  [
    'a call whose caller is not loaded sits at the top',
    [call('inner', 'gone'), call('leaf', 'inner')],
    [
      ['inner', 0],
      ['leaf', 1],
    ],
  ],
])('%s', (_case, rows, expected) => {
  expect(depths(rows)).toEqual(expected)
})
