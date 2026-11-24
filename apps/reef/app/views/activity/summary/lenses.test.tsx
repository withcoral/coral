/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import type { ActivitySummaryData, ContextEntry } from '@/lib/activity-summary'
import { renderRoute } from '@/test-support/route'
import { CallsLens } from './calls-lens'
import { ContextLens } from './context-lens'
import { NeedsYouLens } from './needs-you-lens'

const called = (path: string, calls: number) => ({
  tool: `${path}#tool`,
  callPath: `coral.providers.${path}`,
  calls,
  daily: [],
  callers: [],
})
const stopped = (path: string, stops: { blocked?: number; needsSetup?: number }) => ({
  tool: `${path}#tool`,
  callPath: `coral.providers.${path}`,
  blocked: stops.blocked ?? 0,
  notAllowed: 0,
  needsSetup: stops.needsSetup ?? 0,
})

/** With no providers in the workspace, each tool reads as its namespace, then its name. */
const SUMMARY: ActivitySummaryData = {
  calls: { answered: 14, blocked: 4, notAllowed: 0, needsSetup: 2, failed: 0, canceled: 0 },
  tools: [
    called('slack.postMessage', 5),
    called('github.mergePullRequest', 5),
    called('github.getPullRequest', 9),
    called('asana.listTasks', 1),
  ],
  stopped: [
    stopped('slack.postMessage', { blocked: 3 }),
    stopped('github.mergePullRequest', { blocked: 1 }),
    stopped('asana.listTasks', { needsSetup: 2 }),
  ],
  callers: [],
  approvals: { approved: 0, declined: 0, medianWaitSeconds: null },
}

// Past what a number holds exactly, so only an exact comparison tells the two apart.
const CONTEXT: ContextEntry[] = [
  { key: 'review', label: 'Review agent', bytes: '-9007199254740992' },
  { key: 'digest', label: 'Digest agent', bytes: '4096' },
  { key: 'you', label: 'You', bytes: '-9007199254740993' },
]

/** Each row's first cell, by its title (a tool's Call Path) or its text. */
function shown(table: string) {
  return within(screen.getByRole('table', { name: table }))
    .getAllByRole('row')
    .slice(1)
    .map((row) => {
      const cell = within(row).getAllByRole('cell')[0]
      return (cell.getAttribute('title') ?? cell.textContent)?.replace('coral.providers.', '')
    })
}

async function sortBy(table: string, columns: string[]) {
  const heading = within(screen.getByRole('table', { name: table }))
  for (const column of columns) {
    await userEvent.click(heading.getByRole('button', { name: `Sort by ${column.toLowerCase()}` }))
  }
}

/** The view that holds each table. */
const VIEWS = {
  Tools: () => <CallsLens summary={SUMMARY} />,
  'To fix': () => <NeedsYouLens summary={SUMMARY} />,
  'Context by caller': () => <ContextLens entries={CONTEXT} />,
}

// Catches: a table that drops its starting order, a count that starts at the smallest,
// equal counts in no order you can scan, and context compared after rounding.
it.each<[keyof typeof VIEWS, string[], string[]]>([
  [
    'Tools',
    [],
    ['github.getPullRequest', 'github.mergePullRequest', 'slack.postMessage', 'asana.listTasks'],
  ],
  [
    'Tools',
    ['Tool'],
    ['asana.listTasks', 'github.getPullRequest', 'github.mergePullRequest', 'slack.postMessage'],
  ],
  [
    'Tools',
    ['Stopped'],
    ['slack.postMessage', 'asana.listTasks', 'github.mergePullRequest', 'github.getPullRequest'],
  ],
  ['To fix', [], ['slack.postMessage', 'asana.listTasks', 'github.mergePullRequest']],
  // Blocked by a rule, then Needs setup.
  ['To fix', ['Reason'], ['github.mergePullRequest', 'slack.postMessage', 'asana.listTasks']],
  ['Context by caller', [], ['You', 'Review agent', 'Digest agent']],
  ['Context by caller', ['Share'], ['You', 'Review agent', 'Digest agent']],
  ['Context by caller', ['Caller'], ['Digest agent', 'Review agent', 'You']],
])('%s, after sorting by %j, lists %j', async (table, columns, order) => {
  const View = VIEWS[table]
  renderRoute(<View />)
  await sortBy(table, columns)
  expect(shown(table)).toEqual(order)
})
