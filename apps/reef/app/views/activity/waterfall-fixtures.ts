// Traces for the waterfall's stories: a long Task with idle gaps and waits, a short Task,
// a Run with many calls, and a standalone Run. Times count from each root's start.
import type { RunEvent } from './types'
import {
  interaction,
  invocation,
  outputRow,
  run,
  task,
  toolRow,
  workspace,
} from './side-panel-fixtures'
import type { ActivityRow, ActivityTrace } from './types'

type RunRow = Extract<ActivityRow, { kind: 'run' }>
type CallRow = Extract<ActivityRow, { kind: 'invocation' }>

/** Builds one trace whose instants count in milliseconds from `startedAt`. */
function builder(startedAt: string) {
  const origin = Date.parse(startedAt)
  const t = (ms: number) => new Date(origin + ms).toISOString()
  const taskName = `${workspace}/tasks/${startedAt.slice(11, 19).replaceAll(':', '')}`
  let sequence = 0
  return {
    t,
    taskName,
    search(id: string, from: number, to: number, intent: string) {
      return {
        ...toolRow(
          interaction('search', 'succeeded', {
            name: `${taskName}/interactions/${id}`,
            intent,
            startedAt: t(from),
            completedAt: t(to),
          }),
        ),
        task: taskName,
      }
    },
    describe(id: string, from: number, to: number, intent: string) {
      return {
        ...toolRow(
          interaction('describe', 'succeeded', {
            name: `${taskName}/interactions/${id}`,
            intent,
            startedAt: t(from),
            completedAt: t(to),
          }),
        ),
        task: taskName,
      }
    },
    save(id: string, from: number, to: number, intent: string, bytes: number) {
      return {
        ...toolRow(
          interaction('save_function', 'succeeded', {
            name: `${taskName}/interactions/${id}`,
            intent,
            startedAt: t(from),
            completedAt: t(to),
            contextImpactBytes: String(bytes),
          }),
        ),
        task: taskName,
      }
    },
    run(id: string, intent: string, from: number, to: number | null, state: RunRow['state']) {
      const name = `${workspace}/runs/${id}`
      return run({
        name,
        intent,
        task: taskName,
        startedAt: t(from),
        endedAt: to === null ? null : t(to),
        state,
        submission: {
          name: `${taskName}/interactions/${id}`,
          agent: '',
          tool: 'exec',
          intent,
          outcome: 'succeeded',
          startedAt: t(from),
          completedAt: t(from),
          acceptedRun: name,
        },
      })
    },
    call(
      runRow: RunRow,
      id: string,
      callPath: string,
      from: number,
      to: number | null,
      extra: Partial<CallRow> = {},
    ): CallRow {
      return invocation({
        name: `${workspace}/invocations/${id}`,
        run: runRow.name,
        task: runRow.task,
        callPath,
        tool: callPath.split('.').at(-1)!,
        startedAt: t(from),
        dispatchedAt: t(from + 4),
        endedAt: to === null ? null : t(to),
        state: to === null ? 'dispatching' : 'ended',
        outcome: to === null ? 'unknown' : 'upstream_outcome',
        // The waterfall reads no source, so no call needs a place in a program.
        sourceLocation: undefined,
        ...extra,
      })
    },
    event(
      runRow: RunRow,
      at: number,
      event: Partial<RunEvent> & Pick<RunEvent, 'kind'>,
      label: string,
    ) {
      sequence += 1
      return {
        ...outputRow(
          { sequence: String(sequence), occurredAt: t(at), ...event } as RunEvent,
          label,
        ),
        name: `${runRow.name}/events/${sequence}`,
        run: runRow.name,
        task: runRow.task,
      }
    },
  }
}

/** A Task that runs for minutes: idle gaps, a rejected program, an approval wait, a failure. */
export function longTrace(): ActivityTrace {
  const b = builder('2026-09-18T14:00:00.000Z')
  const root = task({
    name: b.taskName,
    intent: 'Find overdue invoices and notify their owners',
    startedAt: b.t(0),
    endedAt: null,
    outcome: 'open',
  })
  const list = b.run('list-overdue', 'List overdue invoices', 21_040, 23_410, 'succeeded')
  const send = b.run('send-reminders', 'Send invoice reminders', 48_000, 252_300, 'succeeded')
  const check = b.run('check-issue', 'Check the follow-up issue', 290_000, null, 'running')
  const rejected = {
    ...toolRow(
      interaction('exec', 'rejected', {
        name: `${b.taskName}/interactions/rejected`,
        intent: 'Remind each owner',
        startedAt: b.t(40_100),
        completedAt: b.t(40_162),
      }),
    ),
    task: b.taskName,
  }
  return {
    root,
    complete: true,
    unavailable: [],
    children: {
      [b.taskName]: [
        b.search('find-invoices', 0, 182, 'Find the invoice tools'),
        b.search('find-mail', 410, 548, 'Find a tool that sends email'),
        b.describe('describe-invoices', 3100, 3191, 'Read how to list invoices'),
        b.describe('describe-mail', 3402, 3487, 'Read how to send email'),
        list,
        rejected,
        send,
        b.search('find-issue', 270_000, 270_160, 'Find the issue tools'),
        check,
      ],
      [list.name]: [
        b.call(list, 'list-1', 'coral.providers.billing.listInvoices', 21_090, 22_880),
        b.event(
          list,
          22_950,
          { kind: 'program.logged', level: 'info', message: 'overdue 14' },
          'log #1',
        ),
        b.event(list, 23_000, { kind: 'program.emitted', value: '{}' }, 'emission #2'),
      ],
      [send.name]: [
        b.call(send, 'list-2', 'coral.providers.billing.listInvoices', 48_060, 48_910),
        b.call(send, 'mail-1', 'coral.providers.mail.send', 48_950, 235_640, {
          dispatchedAt: b.t(235_100),
        }),
        b.call(send, 'mail-2', 'coral.providers.mail.send', 235_700, 236_180),
        b.call(send, 'mail-3', 'coral.providers.mail.send', 235_705, 236_930, {
          outcome: 'coral_failure',
        }),
        b.call(send, 'mail-4', 'coral.providers.mail.send', 235_712, 236_020),
        b.call(send, 'issues-1', 'coral.providers.linear.listIssues', 237_000, 238_120),
        b.call(send, 'issue-2', 'coral.providers.linear.createIssue', 238_200, 251_450, {
          dispatchedAt: b.t(249_800),
        }),
        b.event(
          send,
          251_900,
          { kind: 'program.logged', level: 'info', message: 'reminded 3' },
          'log #3',
        ),
      ],
      [check.name]: [
        b.call(check, 'issue-3', 'coral.providers.linear.getIssue', 290_050, 290_900),
        b.call(check, 'comments-1', 'coral.providers.linear.listComments', 291_000, null),
      ],
    },
  }
}

/** A Task done in seconds, with calls in parallel. */
export function shortTrace(): ActivityTrace {
  const b = builder('2026-09-18T15:10:00.000Z')
  const remind = b.run('remind-open', 'Remind owners and open the issue', 300, 4200, 'succeeded')
  return {
    root: task({
      name: b.taskName,
      intent: 'Open a follow-up issue for INV-4171',
      startedAt: b.t(0),
      endedAt: b.t(4300),
      outcome: 'success',
    }),
    complete: true,
    unavailable: [],
    children: {
      [b.taskName]: [b.search('find-issue', 0, 120, 'Find the issue tools'), remind],
      [remind.name]: [
        b.call(remind, 'list', 'coral.providers.billing.listInvoices', 340, 900),
        b.call(remind, 'mail-a', 'coral.providers.mail.send', 920, 1500),
        b.call(remind, 'mail-b', 'coral.providers.mail.send', 925, 1710),
        b.call(remind, 'mail-c', 'coral.providers.mail.send', 930, 1420),
        b.call(remind, 'issue', 'coral.providers.linear.createIssue', 1750, 4100),
      ],
    },
  }
}

/** A Run that makes 60 calls in turn, and a trace cut at the read limit. */
export function manyTrace(): ActivityTrace {
  const b = builder('2026-09-18T16:00:00.000Z')
  const remind = b.run('remind-all', 'Remind every owner', 2000, 92_000, 'failed')
  let at = 3500
  const calls = Array.from({ length: 60 }, (_, i) => {
    const length = 900 + ((i * 7919) % 700)
    const value = b.call(remind, `mail-${i}`, 'coral.providers.mail.send', at, at + length, {
      outcome: i % 10 === 7 ? 'coral_failure' : 'upstream_outcome',
    })
    at += length + 40
    return value
  })
  return {
    root: task({
      name: b.taskName,
      intent: 'Remind every owner with an overdue invoice',
      startedAt: b.t(0),
      endedAt: b.t(95_000),
      outcome: 'failure',
    }),
    complete: false,
    unavailable: [],
    children: {
      [b.taskName]: [b.search('find-mail', 0, 150, 'Find a tool that sends email'), remind],
      [remind.name]: calls,
    },
  }
}

/** A Run with no Task. */
export function standaloneTrace(): ActivityTrace {
  const b = builder('2026-09-18T13:41:00.000Z')
  const root = run({
    name: `${workspace}/runs/export-paid`,
    intent: 'Export last month’s paid invoices',
    task: '',
    startedAt: b.t(0),
    endedAt: b.t(7800),
    state: 'succeeded',
    submission: undefined,
  })
  return {
    root,
    complete: true,
    unavailable: [],
    children: {
      [root.name]: [
        b.call(root, 'page-1', 'coral.providers.billing.listInvoices', 40, 2900),
        b.call(root, 'page-2', 'coral.providers.billing.listInvoices', 2950, 5300),
        b.call(root, 'upload', 'coral.providers.drive.uploadFile', 5400, 7700),
      ],
    },
  }
}

/** A standalone Run that still runs: one call done, one waiting on approval. */
export function runningTrace(): ActivityTrace {
  const b = builder('2026-09-18T14:05:00.000Z')
  const root = run({
    name: `${workspace}/runs/refresh-owners`,
    intent: 'Refresh the invoice owners',
    task: '',
    startedAt: b.t(0),
    endedAt: null,
    state: 'running',
    submission: undefined,
  })
  return {
    root,
    complete: true,
    unavailable: [],
    children: {
      [root.name]: [
        b.event(root, 0, { kind: 'run.started' }, 'execution accepted'),
        b.call(root, 'owners', 'coral.providers.billing.listInvoices', 200, 1400),
        b.event(
          root,
          1500,
          { kind: 'program.logged', level: 'info', message: 'owners 3' },
          'log #2',
        ),
        b.call(root, 'notify', 'coral.providers.mail.send', 1600, null, {
          state: 'pending',
          dispatchedAt: null,
          requiresAction: true,
        }),
      ],
    },
  }
}

/** A Task that failed: two programs whose one call each failed after a wait, then a long gap. */
export function failedTrace(): ActivityTrace {
  const b = builder('2026-09-18T12:21:28.000Z')
  const root = task({
    name: b.taskName,
    intent: 'Add ClickStack MCP provider',
    startedAt: b.t(0),
    endedAt: b.t(65_000),
    outcome: 'failure',
  })
  const first = b.run('add-clickstack', 'Add ClickStack MCP provider', 14_400, 21_070, 'succeeded')
  const retry = b.run(
    'retry-clickstack',
    'Retry adding the ClickStack provider',
    24_500,
    27_200,
    'succeeded',
  )
  const attempt = (program: typeof first, id: string, from: number, waited: number, ran: number) =>
    b.call(program, id, 'coral.providers.coral.addProvider', from, from + waited + ran, {
      dispatchedAt: b.t(from + waited),
      outcome: 'coral_failure',
    })
  const firstCall = attempt(first, 'add-1', 14_420, 6620, 31)
  const second = attempt(retry, 'add-2', 24_520, 2570, 19)
  return {
    root,
    complete: true,
    unavailable: [],
    failure: {
      record: second.name,
      label: second.callPath,
      code: 'upstream_rejected',
      message: 'The MCP server refused the bearer token.',
      waitedFor: 'approval',
      sameBefore: 1,
    },
    waitedFor: { [firstCall.name]: 'approval', [second.name]: 'approval' },
    feedback:
      'The ClickStack MCP server rejects the bearer token. Check the token in its settings.',
    children: {
      [b.taskName]: [
        b.search('find-tool', 2000, 2021, 'Find a tool that adds an MCP provider'),
        b.describe('shapes', 4100, 4105, 'Read the input shapes for an MCP provider'),
        first,
        retry,
      ],
      [first.name]: [firstCall],
      [retry.name]: [second],
    },
  }
}

const withBytes = <T extends ActivityRow>(row: T, bytes: number): T => ({
  ...row,
  contextImpactBytes: String(bytes),
})
const clickstack = (name: string) => `coral.providers.clickstack.${name}`
const fn = (id: string) => `${workspace}/savedFunctions/${id}`

/**
 * A Task where the agent did nearly all the work itself: three rounds of exploring a few
 * seconds apart, 38.9s of thought, three saved functions and one resaved, then a smoke test of
 * eleven calls at once. Its calls take under a second of its 1m 29s.
 */
export function exploreTrace(): ActivityTrace {
  const b = builder('2026-09-18T16:05:00.000Z')
  const root = task({
    name: b.taskName,
    intent: 'Build saved functions for ClickStack service health',
    startedAt: b.t(0),
    endedAt: b.t(89_300),
    outcome: 'success',
    contextImpactBytes: '-26100',
  })
  // A program's Context Impact is its output minus the tool results its calls read; a call's
  // is its tool result, as a negative number.
  type CallSpec = { id: string; tool: string; from: number; to: number; result: number }
  const program = (row: ReturnType<typeof b.run>, output: number, specs: CallSpec[]) => {
    const calls = specs.map((spec) =>
      withBytes(b.call(row, spec.id, clickstack(spec.tool), spec.from, spec.to), -spec.result),
    )
    const read = specs.reduce((sum, spec) => sum + spec.result, 0)
    return { row: withBytes(row, output - read), calls }
  }
  const sources = program(
    b.run('sources', 'List the ClickStack sources', 1200, 1500, 'succeeded'),
    1800,
    [{ id: 'sources', tool: 'listSources', from: 1210, to: 1480, result: 4670 }],
  )
  const health = program(
    b.run('health', 'Check service health and log patterns', 9500, 10_200, 'succeeded'),
    5200,
    [
      { id: 'health', tool: 'serviceHealth', from: 9510, to: 9900, result: 6000 },
      { id: 'patterns', tool: 'logPatterns', from: 9910, to: 10_180, result: 9080 },
    ],
  )
  const errors = program(
    b.run(
      'ui-errors',
      'Drill into UI errors, logs over the last hour',
      17_000,
      17_600,
      'succeeded',
    ),
    9400,
    [
      { id: 'ui-errors', tool: 'searchLogs', from: 17_010, to: 17_300, result: 24_100 },
      { id: 'ui-traces', tool: 'clickstackTraceWaterfall', from: 17_310, to: 17_580, result: 2740 },
    ],
  )
  const sample = program(
    b.run('trace-503', 'Sample a 503 trace', 26_500, 27_100, 'succeeded'),
    3100,
    [{ id: 'trace', tool: 'clickstackTraceWaterfall', from: 26_510, to: 27_080, result: 4140 }],
  )
  // Eleven calls at once: each takes about 90ms, and together they take 296ms.
  const smoke = program(
    b.run('smoke', 'Smoke-test the three saved functions', 84_000, 84_296, 'succeeded'),
    1900,
    Array.from({ length: 11 }, (_, index) => ({
      id: `smoke-${index + 1}`,
      tool: ['serviceHealth', 'errorBreakdown', 'slowOperations'][index % 3]!,
      from: 84_010 + index * 18,
      to: 84_090 + index * 18 + (index % 4) * 6,
      result: 3000 + index * 400,
    })),
  )
  const saves = {
    health: b.save('save-health', 66_000, 66_040, 'Save the service health report', 310),
    errors: b.save('save-errors', 66_500, 66_530, 'Save the error breakdown', 290),
    slow: b.save('save-slow', 67_000, 67_030, 'Save the slow operation finder', 280),
    resave: b.save('resave-slow', 81_500, 81_540, 'Resave the slow operation finder', 300),
  }
  return {
    root,
    complete: true,
    unavailable: [],
    feedback:
      'Saved three tested functions: service health, error breakdown and slow operations. ' +
      'Resaved slow operations: clickstackTraceWaterfall\'s pickBy "first_error" filters on ' +
      'StatusCode:STATUS_CODE_ERROR, but this data stores "Error", so it matched nothing; an ' +
      'explicit SQL pickFilter worked.',
    // The smoke test called all three, so each has its path; the resave revised the third.
    saves: {
      [saves.health.name]: {
        savedFunction: fn('health'),
        path: 'clickstack.serviceHealth',
        tested: true,
      },
      [saves.errors.name]: {
        savedFunction: fn('errors'),
        path: 'clickstack.errorBreakdown',
        tested: true,
      },
      [saves.slow.name]: {
        savedFunction: fn('slow'),
        path: 'clickstack.slowOperations',
        tested: true,
      },
      [saves.resave.name]: {
        savedFunction: fn('slow'),
        path: 'clickstack.slowOperations',
        tested: true,
      },
    },
    children: {
      [b.taskName]: [
        withBytes(b.search('find-tools', 500, 535, 'Find ClickStack tools'), 25_700),
        withBytes(b.search('find-saved', 600, 610, 'Find existing saved functions'), 2860),
        sources.row,
        health.row,
        errors.row,
        sample.row,
        saves.health,
        saves.errors,
        saves.slow,
        saves.resave,
        smoke.row,
      ],
      [sources.row.name]: sources.calls,
      [health.row.name]: health.calls,
      [errors.row.name]: errors.calls,
      [sample.row.name]: sample.calls,
      [smoke.row.name]: smoke.calls,
    },
  }
}
