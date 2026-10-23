import { expect, it } from 'vitest'
import { fixFor, groupByProvider, operationRows } from './operations'

const links = {
  provider: (callPath: string) =>
    callPath.startsWith('coral.providers.github.')
      ? { name: 'GitHub', path: '/workspaces/w/providers/gh' }
      : undefined,
}

// Catches: an operation stopped without being among the busiest dropping out of the table.
it('joins a stopped operation that is not among the busiest, with its stops as its calls', () => {
  const rows = operationRows(
    {
      operations: [{ callPath: 'coral.providers.github.getIssue', calls: 9 }],
      stopped: [
        { callPath: 'coral.providers.github.getIssue', blocked: 1, notAllowed: 0, needsSetup: 0 },
        { callPath: 'coral.providers.slack.post', blocked: 2, notAllowed: 1, needsSetup: 0 },
      ],
    },
    links,
  )
  expect(rows).toEqual([
    expect.objectContaining({ callPath: 'coral.providers.github.getIssue', calls: 9, stopped: 1 }),
    expect.objectContaining({ callPath: 'coral.providers.slack.post', calls: 3, stopped: 3 }),
  ])
})

// Catches: a row named by its full Call Path, or a removed provider left without a name.
it.each([
  ['coral.providers.github.issues.list', 'GitHub', 'issues.list'],
  ['coral.providers.slack.post', 'slack', 'post'],
])('names %s as provider %s and operation %s', (callPath, provider, operation) => {
  const [row] = operationRows({ operations: [{ callPath, calls: 1 }], stopped: [] }, links)
  expect(row).toMatchObject({ provider, operation })
})

// Catches: a provider's totals that leave out one of its operations.
it('totals each provider across its operations', () => {
  const rows = operationRows(
    {
      operations: [
        { callPath: 'coral.providers.github.getIssue', calls: 9 },
        { callPath: 'coral.providers.slack.post', calls: 4 },
        { callPath: 'coral.providers.github.merge', calls: 3 },
      ],
      stopped: [
        { callPath: 'coral.providers.github.merge', blocked: 2, notAllowed: 1, needsSetup: 0 },
      ],
    },
    links,
  )
  expect(groupByProvider(rows)).toEqual([
    expect.objectContaining({
      provider: 'GitHub',
      calls: 12,
      stops: { blocked: 2, notAllowed: 1, needsSetup: 0 },
      stopped: 3,
    }),
    expect.objectContaining({ provider: 'slack', calls: 4, stopped: 0 }),
  ])
})

// Catches: a stop that links to the wrong page, or a link for an operation Coral cannot open.
it.each([
  ['blocked', { to: '/p/policy', label: 'Review rules' }],
  ['needsSetup', { to: '/p/settings', label: 'Fix setup' }],
] as const)('fixes a %s call on its page', (reason, fix) => {
  const pages = { operation: '/p/ops/x', rules: '/p/policy', settings: '/p/settings' }
  expect(fixFor(reason, pages)).toEqual(fix)
  expect(fixFor(reason, undefined)).toBeUndefined()
})
