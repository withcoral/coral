import { expect, it } from 'vitest'
import { fixFor, toolRows } from './tools'

const providers = [
  { callRoot: 'coral.providers.github', displayName: 'GitHub', path: '/p/github' },
  { callRoot: 'coral.providers.ghe', displayName: 'GitHub', path: '/p/ghe' },
  { callRoot: 'coral.workspace', displayName: 'Coral', path: '/p/builtin' },
]

// Catches: a tool stopped without being among the busiest dropping out of the table.
it('joins a stopped tool that is not among the busiest, with its stops as its calls', () => {
  const rows = toolRows(
    {
      tools: [{ callPath: 'coral.providers.github.getIssue', calls: 9 }],
      stopped: [
        { callPath: 'coral.providers.github.getIssue', blocked: 1, notAllowed: 0, needsSetup: 0 },
        { callPath: 'coral.providers.slack.post', blocked: 2, notAllowed: 1, needsSetup: 0 },
      ],
    },
    providers,
  )
  expect(rows).toEqual([
    expect.objectContaining({ callPath: 'coral.providers.github.getIssue', calls: 9, stopped: 1 }),
    expect.objectContaining({ callPath: 'coral.providers.slack.post', calls: 3, stopped: 3 }),
  ])
})

// Catches: a row named by its full Call Path, two providers with one name that only the
// namespace tells apart, a built-in call misread as a provider's, or a removed provider left
// without a name or given a page that does not exist.
it.each([
  ['coral.providers.github.issues.list', 'GitHub', 'github', 'issues.list', '/p/github'],
  ['coral.providers.ghe.issues.list', 'GitHub', 'ghe', 'issues.list', '/p/ghe'],
  ['coral.workspace.addProvider', 'Coral', 'workspace', 'addProvider', '/p/builtin'],
  ['coral.providers.slack.post', 'slack', 'slack', 'post', undefined],
])(
  'names %s as provider %s, namespace %s and tool %s',
  (callPath, provider, namespace, tool, path) => {
    const [row] = toolRows({ tools: [{ callPath, calls: 1 }], stopped: [] }, providers)
    expect(row).toMatchObject({ provider, namespace, tool })
    expect(row.pages).toEqual(
      path && { tool: `${path}/tools/${tool}`, settings: `${path}/settings` },
    )
  },
)

// Catches: a stop that links to the wrong page, or a link for a tool Coral cannot open.
it.each([
  ['blocked', '/p/tools/x'],
  ['needsSetup', '/p/settings'],
] as const)('fixes a %s call on its page', (reason, fix) => {
  const pages = { tool: '/p/tools/x', settings: '/p/settings' }
  expect(fixFor(reason, pages)).toEqual(fix)
  expect(fixFor(reason, undefined)).toBeUndefined()
})
