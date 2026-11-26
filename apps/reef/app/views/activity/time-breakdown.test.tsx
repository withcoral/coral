/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { TimeBreakdown } from './time-breakdown'
import { T0, traceCall, traceRun, traceStore, traceTask } from './trace-fixtures'
import { timeSummary } from './trace-summary'
import type { ActivityRow } from './types'

const lostRun = (task: string) =>
  traceRun('lost', 0, null, { state: 'unknown', task, durationNanos: '0' })
const endedTask = traceTask()
const openTask = traceTask({ outcome: 'open', endedAt: null })
const endedRun = traceRun('ended', 0, 9_000, { task: '' })

// Catches: a note that says Agent model holds time after a missing end where the time
// measured stops there, so Agent model holds none of it: a standalone Run, or an open Task.
it.each([
  [
    'a standalone Run with no recorded end',
    lostRun(''),
    () => ({ [lostRun('').name]: [traceCall(lostRun(''), 'c', [1_000, 1_000, 2_000])] }),
    'It counts up to its last recorded activity.',
  ],
  [
    'an open Task whose Run has no recorded end',
    openTask,
    () => ({ [openTask.name]: [lostRun(openTask.name)], [lostRun(openTask.name).name]: [] }),
    'It counts up to its last recorded activity.',
  ],
  [
    'an ended Task whose Run has no recorded end',
    endedTask,
    () => ({ [endedTask.name]: [lostRun(endedTask.name)], [lostRun(endedTask.name).name]: [] }),
    'Agent model can include time after its last recorded activity.',
  ],
  [
    'an ended standalone Run whose sent call has no recorded end',
    endedRun,
    () => ({
      [endedRun.name]: [traceCall(endedRun, 'lost', [0, 4_000, null], { state: 'ended' })],
    }),
    'It counts up to its last recorded activity.',
  ],
] as const)('states what %s means for the parts', (_, root, children, consequence) => {
  const store = traceStore(root, children() as Record<string, ActivityRow[]>)
  render(
    <TimeBreakdown
      root={root.name}
      summary={timeSummary(store, T0 + 500_000)}
      taskTools={false}
      rootRow={root}
      announced
    />,
  )
  expect(screen.getByText(`1 record has no recorded end. ${consequence}`)).toBeInTheDocument()
})
