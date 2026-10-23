import { expect, it } from 'vitest'
import { fixFor, operationRows } from './operations'

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

// Catches: a row named by its full Call Path, a removed provider left without a name, or two
// providers with one name that only the namespace tells apart.
it.each([
  ['coral.providers.github.issues.list', 'GitHub', 'github', 'issues.list'],
  ['coral.providers.slack.post', 'slack', 'slack', 'post'],
])(
  'names %s as provider %s, namespace %s and operation %s',
  (callPath, provider, namespace, operation) => {
    const [row] = operationRows({ operations: [{ callPath, calls: 1 }], stopped: [] }, links)
    expect(row).toMatchObject({ provider, namespace, operation })
  },
)

// Catches: a stop that links to the wrong page, or a link for an operation Coral cannot open.
it.each([
  ['blocked', '/p/ops/x'],
  ['needsSetup', '/p/settings'],
] as const)('fixes a %s call on its page', (reason, fix) => {
  const pages = { operation: '/p/ops/x', settings: '/p/settings' }
  expect(fixFor(reason, pages)).toEqual(fix)
  expect(fixFor(reason, undefined)).toBeUndefined()
})
