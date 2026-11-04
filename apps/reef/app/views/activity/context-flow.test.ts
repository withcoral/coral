import { expect, it } from 'vitest'
import { contextFlow } from './context-flow'
import {
  interaction,
  invocation,
  run,
  runName,
  task,
  taskName,
  toolRow,
} from './side-panel-fixtures'
import type { ActivityTrace } from './types'

const bytes = (value: number) => ({ contextImpactBytes: String(value) })

// Catches: a column that nets what reached the agent against what a program kept from it, as
// Coral's signed Context Impact does. A search adds its response; a Run adds its output, which
// is its Context Impact plus the tool results its calls read.
it('splits context into what reached the agent and what programs kept out', () => {
  const search = {
    ...toolRow(interaction('search', 'succeeded', { name: `${taskName}/interactions/look` })),
    ...bytes(25_700),
  }
  // Its calls read 40 kB of tool results and it gave back 10 kB: −30 kB as Coral counts it.
  const program = { ...run({}), ...bytes(-30_000) }
  const calls = [
    { ...invocation({ name: `${runName}/calls/a` }), ...bytes(-15_000) },
    { ...invocation({ name: `${runName}/calls/b` }), ...bytes(-25_000) },
  ]
  const trace: ActivityTrace = {
    root: { ...task(), ...bytes(-4300) },
    children: { [taskName]: [search, program], [runName]: calls },
    complete: true,
    unavailable: [],
  }

  const flow = contextFlow(trace)

  expect(flow.total).toEqual({
    into: 35_700,
    providerData: 40_000,
    output: 10_000,
    complete: true,
    running: 0,
    unmeasured: 0,
  })
  expect(flow.records.get(search.name)).toEqual({ into: 25_700 })
  expect(flow.records.get(program.name)).toEqual({ into: 10_000, keptOut: 30_000 })
  expect(flow.records.get(calls[0]!.name)).toEqual({ result: 15_000 })
})

// Catches: a negative "kept out" for a program that wrote more than it read, a total that
// claims to be whole while a Run is still running, or one called partial for a save, which
// Coral never counts toward context.
it.each([
  {
    label: 'keeps nothing out for a program that gave back more than it read',
    record: { ...run({}), ...bytes(2000) },
    expected: { into: 2000, keptOut: 0 },
    total: { complete: true, running: 0, unmeasured: 0 },
  },
  {
    label: 'counts a running Run apart from the partial total',
    record: run({ state: 'running', endedAt: null }),
    expected: undefined,
    total: { complete: false, running: 1, unmeasured: 0 },
  },
  {
    label: 'counts an ended Run with no measurement apart from the partial total',
    record: run({ state: 'succeeded' }),
    expected: undefined,
    total: { complete: false, running: 0, unmeasured: 1 },
  },
  {
    label: 'leaves a save out of the total, measured or not',
    record: toolRow(
      interaction('save_function', 'succeeded', { name: `${taskName}/interactions/save` }),
    ),
    expected: undefined,
    total: { complete: true, running: 0, unmeasured: 0 },
  },
])('$label', ({ record, expected, total }) => {
  const flow = contextFlow({
    root: task(),
    children: { [taskName]: [record] },
    complete: true,
    unavailable: [],
  })
  expect(flow.records.get(record.name)).toEqual(expected)
  expect(flow.total).toMatchObject(total)
})
