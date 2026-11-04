import { expect, it } from 'vitest'
import {
  interaction,
  invocation,
  run,
  runName,
  task,
  taskName,
  toolRow,
} from './side-panel-fixtures'
import { callsByProvider, reportedIssues, runCalls, savedFunctionsOf } from './trace-summary'
import type { ActivityTrace } from './types'

const at = (ms: number) => new Date(Date.parse('2026-09-18T14:00:00.000Z') + ms).toISOString()
const call = (id: string, callPath: string, from: number, to: number, failed = false) =>
  invocation({
    name: `${runName}/calls/${id}`,
    callPath,
    startedAt: at(from),
    endedAt: at(to),
    state: 'ended',
    outcome: failed ? 'coral_failure' : 'upstream_outcome',
    sourceLocation: undefined,
  })
const save = (id: string, ms: number) =>
  toolRow(
    interaction('save_function', 'succeeded', {
      name: `${taskName}/interactions/${id}`,
      startedAt: at(ms),
      completedAt: at(ms),
    }),
  )

const program = run({ startedAt: at(0), endedAt: at(400) })
const calls = [
  call('a', 'coral.providers.clickstack.health', 10, 110),
  call('b', 'coral.providers.clickstack.logs', 20, 300, true),
  call('c', 'coral.providers.mail.send', 30, 60),
]
const saves = [save('first', 500), save('other', 600), save('resave', 700)]
const trace: ActivityTrace = {
  root: task(),
  children: { [taskName]: [program, ...saves], [runName]: calls },
  complete: true,
  unavailable: [],
  saves: {
    [saves[0]!.name]: { savedFunction: 'f/health', path: 'clickstack.health' },
    [saves[1]!.name]: { savedFunction: 'f/other' },
    [saves[2]!.name]: { savedFunction: 'f/health', path: 'clickstack.health', tested: true },
  },
}

// Catches: a resave counted as a second function, or a function out of the order it was saved.
it('lists each saved function once, its later saves as revisions', () => {
  expect(
    savedFunctionsOf(trace).map(({ path, saves: made, tested }) => ({
      path,
      saves: made.map((row) => row.name.split('/').at(-1)),
      tested,
    })),
  ).toEqual([
    // A function reads as tested when a Run that succeeded called it after its last save.
    { path: 'clickstack.health', saves: ['first', 'resave'], tested: true },
    { path: undefined, saves: ['other'], tested: false },
  ])
})

// Catches: calls counted per tool instead of per provider, or failures lost in the count.
it("counts a root's calls by provider, with their failures", () => {
  expect(callsByProvider(trace)).toEqual([
    { callRoot: 'coral.providers.clickstack', callPath: calls[0]!.callPath, count: 2, failed: 1 },
    { callRoot: 'coral.providers.mail', callPath: calls[2]!.callPath, count: 1, failed: 0 },
  ])
})

// Catches: calls made at once shown as if one followed another.
it("states a Run's calls, how long they took together, and that they overlapped", () => {
  expect(runCalls(trace, program.name)).toMatchObject({
    calls: calls.map((row) => ({ name: row.name })),
    span: 290,
    parallel: true,
  })
})

// Catches: a provider problem the agent reported left as prose, or every tool the summary
// merely names flagged as broken.
it.each([
  {
    label: 'flags the tool a sentence names together with a problem',
    feedback:
      'Saved the health report. The logs tool filters on STATUS_CODE_ERROR, but this data stores "Error", so it matched nothing.',
    expected: [
      {
        tool: 'logs',
        callPath: 'coral.providers.clickstack.logs',
        provider: calls[1]!.provider,
        sentence:
          'The logs tool filters on STATUS_CODE_ERROR, but this data stores "Error", so it matched nothing.',
        calls: [calls[1]!.name],
      },
    ],
  },
  {
    label: 'flags nothing when the summary only names a tool',
    feedback: 'Called health and logs, then saved the report.',
    expected: [],
  },
])('$label', ({ feedback, expected }) => {
  expect(reportedIssues({ ...trace, feedback })).toEqual(expected)
})
