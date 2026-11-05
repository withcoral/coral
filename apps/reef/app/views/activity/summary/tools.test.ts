import { expect, it } from 'vitest'
import { fixFor, toolRows } from './tools'

const providers = [
  { callRoot: 'coral.providers.github', displayName: 'GitHub', path: '/p/github' },
  { callRoot: 'coral.providers.ghe', displayName: 'GitHub', path: '/p/ghe' },
  { callRoot: 'coral.workspace', displayName: 'Coral', path: '/p/builtin' },
]

const github = 'workspaces/w/providers/github'
const getIssue = {
  tool: `${github}/tools/Z2V0SXNzdWU`,
  callPath: 'coral.providers.github.getIssue',
}
const post = {
  tool: 'workspaces/w/providers/slack/tools/cG9zdA',
  callPath: 'coral.providers.slack.post',
}

// Catches: a tool stopped without being among the busiest dropping out of the table.
it('joins a stopped tool that is not among the busiest, with its stops as its calls', () => {
  const rows = toolRows(
    {
      tools: [{ ...getIssue, calls: 9, callers: [] }],
      stopped: [
        { ...getIssue, blocked: 1, notAllowed: 0, needsSetup: 0 },
        { ...post, blocked: 2, notAllowed: 1, needsSetup: 0 },
      ],
    },
    providers,
  )
  expect(rows).toEqual([
    expect.objectContaining({ ...getIssue, calls: 9, stopped: 1 }),
    expect.objectContaining({ ...post, calls: 3, stopped: 3 }),
  ])
})

// Catches: the calls of a tool that left the catalog merged into the tool that took
// over its Call Path, or the stops of one credited to the other.
it('keeps two tools that held one Call Path apart', () => {
  const earlier = { ...getIssue, tool: `${github}/tools/Z2V0LWlzc3Vl` }
  const rows = toolRows(
    {
      tools: [
        { ...getIssue, calls: 4, callers: [] },
        { ...earlier, calls: 2, callers: [] },
      ],
      stopped: [{ ...earlier, blocked: 2, notAllowed: 0, needsSetup: 0 }],
    },
    providers,
  )
  expect(rows).toEqual([
    expect.objectContaining({ tool: getIssue.tool, calls: 4, stopped: 0 }),
    expect.objectContaining({ tool: earlier.tool, calls: 2, stopped: 2 }),
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
  (callPath, provider, namespace, relativePath, path) => {
    const tools = [{ tool: 'workspaces/w/providers/p/tools/eA', callPath, calls: 1, callers: [] }]
    const [row] = toolRows({ tools, stopped: [] }, providers)
    expect(row).toMatchObject({ provider, namespace, relativePath })
    expect(row.pages).toEqual(
      path && { tool: `${path}/tools/${relativePath}`, settings: `${path}/settings` },
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
