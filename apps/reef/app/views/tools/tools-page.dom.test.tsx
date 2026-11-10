/** @vitest-environment happy-dom */

import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { RouteObject } from 'react-router'
import { expect, test, vi } from 'vitest'
import type { SourceChoice } from '@/lib/provider-recipes/choice'
import { renderRoute } from '@/test-support/route'

import {
  builtinProvider,
  privateProvider,
  provider,
  sharedProvider,
  workspaceView,
} from '@/views/providers/fixtures'
import { selectionKey } from '@/views/providers/selection'
import type { ToolCallCounts, ToolView, ProviderView, WorkspaceView } from '@/views/providers/types'
import { toolPath } from './addresses'
import { ToolsPage } from './tools-page'
import type { PublishReviewResult, SharedToolDetailView, ToolDetailView } from './types'

// happy-dom lays nothing out, so the virtualizer would mount no tool
// rows. Give the scroll port a viewport; row heights come from estimates.
const isTable = (element: HTMLElement) => element.getAttribute('aria-label') === 'Tools'
Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
  get() {
    return isTable(this) ? 2000 : 0
  },
})
Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
  get() {
    return isTable(this) ? 1000 : 0
  },
})

const github = provider('00000000-0000-4000-8000-000000000001', 'GitHub', 'github', [
  'issues.list',
  'repos.get',
])
const weather = provider('00000000-0000-4000-8000-000000000002', 'Weather', 'weather', [
  'getForecast',
])
// Your own private tools, and an agent's at the path of one of yours.
const mine = privateProvider('00000000-0000-4000-8000-0000000000aa', [
  'people.greet',
  ['people.greet', 'Worker'],
  'reports.total',
])

type Action = (request: Request) => Promise<unknown>

function renderPage({
  action,
  entry = '/workspaces/demo/tools',
  providers = [github, weather],
  recipes = [],
  routes,
  selectedTool = null,
  selectedToolDetail = null,
  toolCalls,
  workspace = workspaceView,
}: {
  action?: Action
  entry?: string
  providers?: ProviderView[]
  recipes?: SourceChoice[]
  /** Routes beside the page, such as the one a dialog reads. */
  routes?: RouteObject[]
  selectedTool?: ToolView | null
  selectedToolDetail?: ToolDetailView | null
  toolCalls?: ToolCallCounts | null
  workspace?: WorkspaceView
} = {}) {
  const page = (selected: ToolView | null, owner = providerOf(selected, providers)) => (
    <ToolsPage
      workspace={workspace}
      providers={providers}
      recipes={recipes}
      selectedProvider={owner}
      selectedTool={selected?.relativePath ?? null}
      selectedToolAgent={selected?.agent?.id ?? null}
      selectedToolDetail={selectedToolDetail}
      toolCalls={toolCalls}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />
  )
  // Mirrors the route: the URL names the selection, and the loader hands the
  // page the matching provider and tool.
  let element = page(selectedTool)
  const { router } = renderRoute(() => element, {
    entry,
    action: action ? ({ request }) => action(request) : undefined,
    routes,
  })
  // The route re-renders the page with what the URL now selects.
  const show = async (selected: ToolView | null, owner = providerOf(selected, providers)) => {
    element = page(selected, owner)
    await act(async () =>
      router.navigate(
        (selected && pathOf(selected, providers)) ??
          (owner ? providers.find((item) => item.id === owner)!.path : '/workspaces/demo/tools'),
      ),
    )
  }
  // The route re-renders the page with what a reload returned.
  const rerenderProviders = async (next: ProviderView[]) => {
    providers = next
    element = page(null)
    await act(async () => router.navigate('/workspaces/demo/tools#reloaded'))
  }
  // The route re-renders the open tool with what a reload returned of it.
  const reloadDetail = async (next: ToolDetailView) => {
    selectedToolDetail = next
    element = page(selectedTool)
    await act(async () => router.navigate(`${router.state.location.pathname}#reloaded`))
  }
  return { show, router, rerenderProviders, reloadDetail }
}

const pathOf = (tool: ToolView, providers = [github, weather]) => {
  const owner = providers.find((item) => item.tools.includes(tool))!
  return toolPath(owner, tool)
}

function providerOf(selected: ToolView | null, providers = [github, weather]) {
  return selected ? providers.find((item) => item.tools.includes(selected))!.id : null
}

// Catches: the drawer losing the prose the table no longer loads, or showing
// another tool's prose while the next tool loads.
test('the drawer shows the prose of the tool the route names', () => {
  renderPage({
    entry: `${github.path}/tools/issues.list`,
    selectedTool: github.tools[0]!,
    selectedToolDetail: {
      kind: 'provider',
      relativePath: 'issues.list',
      summary: 'List issues',
      description: 'Lists **every** issue.',
    },
  })
  const drawer = screen.getByRole('dialog')
  expect(within(drawer).getByText('every').tagName).toBe('STRONG')
  expect(within(drawer).getByText('coral.providers.github.issues.list')).toBeInTheDocument()
})

// Catches: a drawer that shows another tool's usage, callers left out, or usage
// shown when Coral cannot count calls.
test.each([
  ['a count', true],
  ['no count', false],
])("the drawer shows the tool's usage and callers, given %s", (_, counted) => {
  renderPage({
    entry: `${github.path}/tools/issues.list`,
    selectedTool: github.tools[0]!,
    toolCalls: counted
      ? {
          days: 5,
          tools: {
            [selectionKey(github.id, 'issues.list')]: {
              calls: 13,
              daily: [0, 3, 0, 9, 1],
              callers: [
                {
                  key: 'a',
                  label: 'Nightly sync',
                  agentPath: '/workspaces/demo/agents/a',
                  calls: 9,
                },
                { key: 'u', label: 'You', calls: 4 },
              ],
            },
            [selectionKey(github.id, 'repos.get')]: {
              calls: 99,
              daily: [99, 0, 0, 0, 0],
              callers: [],
            },
          },
        }
      : null,
  })
  const drawer = screen.getByRole('dialog')
  const usage = within(drawer).queryByRole('region', { name: 'Usage' })
  if (!counted) return expect(usage).toBeNull()
  expect(within(usage!).getByRole('group', { name: 'Calls in 5 days' })).toHaveTextContent('13')
  expect(within(usage!).getByRole('link', { name: 'Nightly sync' })).toHaveAttribute(
    'href',
    '/workspaces/demo/agents/a',
  )
  const callers = within(usage!).getByRole('list', { name: 'Callers' })
  expect(
    within(callers)
      .getAllByRole('listitem')
      .map((item) => item.textContent),
  ).toEqual(['Nightly sync9 calls', 'You4 calls'])
  // The bars are hidden from assistive technology, so each day with calls is read out.
  const days = within(usage!).getByRole('list', { name: 'Calls by day' })
  expect(
    within(days)
      .getAllByRole('listitem')
      .map((item) => item.textContent),
  ).toEqual(['3 calls 3 days ago', '9 calls 1 day ago', '1 call in the last 24 hours'])
})

// Catches: the drawer staying scrolled between two providers' tools, as a
// Tool Key is unique only within its provider.
test('the drawer starts at the top for a tool with the same key in another provider', async () => {
  const [first, second] = [github, weather].map((item) => ({
    ...item,
    tools: [{ ...item.tools[0]!, key: 'shared' }],
  }))
  const providers = [first!, second!]
  const selectedTool = first!.tools[0]!
  const { show } = renderPage({
    entry: pathOf(selectedTool, providers),
    providers,
    selectedTool,
  })
  const scrollTo = vi.fn()
  screen.getByRole('dialog').scrollTo = scrollTo
  await show(second!.tools[0]!)
  expect(scrollTo).toHaveBeenCalledWith({ top: 0 })
})

test('the drawer ignores prose that belongs to another tool', () => {
  renderPage({
    entry: `${github.path}/tools/issues.list`,
    selectedTool: github.tools[0]!,
    selectedToolDetail: {
      kind: 'provider',
      relativePath: 'repos.get',
      summary: 'Stale',
      description: 'Stale prose',
    },
  })
  const drawer = screen.getByRole('dialog')
  expect(within(drawer).queryByText('Stale prose')).not.toBeInTheDocument()
  expect(within(drawer).getByText('No description.')).toBeInTheDocument()
})

const user = userEvent.setup()
// The drawer is a modal dialog: while it is open, the page behind it leaves the
// accessibility tree, so a query for it passes `hidden`.
const checkbox = (name: string, hidden = false) => screen.getByRole('checkbox', { name, hidden })

async function filter(label: string, value: string) {
  const input = screen.getByLabelText(label)
  await user.clear(input)
  if (value) await user.type(input, value)
}

const filterTools = (value: string) => filter('Filter tools', value)
const filterProviders = (value: string) => filter('Filter providers', value)

const table = () => screen.getByRole('table', { name: 'Tools' })

// Each tool's name links to its details. The only other links in the table
// are the group headings, which open their groups. An open drawer hides the
// table from the accessibility tree, so these helpers read hidden rows too.
const toolLinks = () =>
  within(screen.getByRole('table', { name: 'Tools', hidden: true }))
    .queryAllByRole('link', { hidden: true })
    .filter((link) => !link.closest('[data-pinned-heading]'))

const rowNames = () => toolLinks().map((link) => link.textContent)

/** The bolded filter matches in a tool's name. */
const boldIn = (tool: string) =>
  [...rowOf(tool).querySelectorAll('strong')].map((match) => match.textContent)

const rowOf = (tool: string) =>
  toolLinks()
    .find((link) => link.textContent === tool)!
    .closest<HTMLElement>('[role="row"]')!

/** Changes a tool's policy from its row. Each toggle segment is a radio
 * named by its policy. */
async function choosePolicy(tool: string, to: string) {
  await user.click(within(rowOf(tool)).getByRole('radio', { name: to }))
}

const providerGroups = () =>
  within(table())
    .queryAllByRole('rowgroup')
    .map((group) => group.getAttribute('aria-label'))
    .filter(Boolean)

const bulkForm = (hidden = false) => screen.getByRole('form', { name: 'Bulk policy', hidden })
const policyFilter = () => screen.getByRole('radiogroup', { name: 'Filter tools by policy' })
/** The toolbar choice whose label starts its name, before its count. */
const policyChoice = (label: string) =>
  within(policyFilter()).getByRole('radio', { name: new RegExp(`^${label} \\d`) })
const choosePolicyFilter = (label: string) => user.click(policyChoice(label))
/** Each toolbar choice's label and count, marked when it is disabled. */
const filterChoices = () =>
  within(policyFilter())
    .getAllByRole('radio')
    .map((radio) => `${radio.textContent}${radio.hasAttribute('disabled') ? ' (off)' : ''}`)
const githubHeading = () => within(table()).getByRole('rowgroup', { name: 'GitHub' })
/** The bulk form's selection count, which opens and leaves the selected view. */
const bulkCount = () => within(bulkForm()).getByRole('button', { name: /selected$/ })

// The bulk form counts the selection, hidden rows included.
const selectionCount = (hidden = false) =>
  within(bulkForm(hidden)).getByText(/ selected$/).textContent

// Lets the router settle a submission's action and revalidation.
async function settle() {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
  })
}

// Catches: a provider without tools dropping out of the table and
// taking its settings with it.
test('lists every provider in one table, and keeps an empty one with its settings', () => {
  const empty = provider('00000000-0000-4000-8000-000000000003', 'Empty API', 'empty')
  renderPage({ providers: [github, empty] })
  expect(providerGroups()).toEqual(['Empty API', 'GitHub'])
  expect(within(table()).getByRole('rowgroup', { name: 'Empty API' })).toHaveTextContent(
    'No tools yet. Settings remain available.',
  )
  expect(screen.getByLabelText('Empty API settings')).toBeInTheDocument()
})

// Catches: a tool's calls shown on another tool's row, a bar that leaves a screen
// reader no count, a removed tool that sets the bars' scale, or a Usage column
// shown when Coral cannot count calls.
test.each([
  [
    'a count',
    {
      days: 30,
      tools: {
        [selectionKey(github.id, 'repos.get')]: { calls: 1234, daily: [234, 1000], callers: [] },
        // A removed tool keeps its calls in the summary, but has no row to scale.
        [selectionKey(github.id, 'removed')]: { calls: 99_999, daily: [99_999], callers: [] },
      },
    },
    ['0 calls', '1,234 calls', '0 calls'],
  ],
  ['no count', null, []],
])('shows the calls to each tool under the Usage column, given %s', (_, toolCalls, counts) => {
  renderPage({ toolCalls })
  const headers = within(table()).getAllByRole('columnheader')
  const column = headers.findIndex((header) => header.textContent === 'Usage')
  const cells =
    column < 0
      ? []
      : ['issues.list', 'repos.get', 'getForecast'].map((name) => {
          const row = within(table()).getByRole('link', { name }).closest('[role="row"]')!
          return within(row as HTMLElement).getAllByRole('cell')[column]!.textContent
        })
  expect(cells).toEqual(counts)
  // The busiest tool in the catalog fills its bar, whatever a removed tool called.
  if (column >= 0) {
    const row = within(table()).getByRole('link', { name: 'repos.get' }).closest('[role="row"]')!
    const cell = within(row as HTMLElement).getAllByRole('cell')[column]!
    expect(cell.querySelector('[style]')).toHaveStyle({ width: '100%' })
  }
})

// Catches: a usage sort that ranks tools only within their provider or group,
// leaves a ranked row without its provider's name, keeps no way back to the
// headings, or reports the wrong order to a screen reader.
const usage = (calls: number) => ({ calls, daily: [calls], callers: [] })

test('ranks every tool by usage in one list, then goes back to the headings', async () => {
  const grouped = provider(github.id, 'GitHub', 'github', [
    ['issues.list', 'issues'],
    ['issues.create', 'issues'],
    ['repos.get', 'repos'],
  ])
  renderPage({
    providers: [grouped, weather],
    toolCalls: {
      days: 30,
      tools: {
        [selectionKey(github.id, 'issues.create')]: usage(5),
        [selectionKey(github.id, 'repos.get')]: usage(9),
        [selectionKey(weather.id, 'getForecast')]: usage(7),
      },
    },
  })
  const header = (name: string) =>
    within(table())
      .getAllByRole('columnheader')
      .find((cell) => cell.textContent?.startsWith(name))!
  // Grouped rows are not in one name order, so only Usage reports a sort.
  const sorts = () =>
    [header('Tool name'), header('Usage')].map((cell) => cell.getAttribute('aria-sort'))
  const headings = () => within(table()).queryAllByRole('rowgroup', { name: /GitHub|Weather/ })
  const byName = ['issues.create', 'issues.list', 'repos.get', 'getForecast']
  expect(rowNames()).toEqual(byName)
  expect(sorts()).toEqual([null, 'none'])

  await user.click(within(table()).getByRole('button', { name: 'Sort by usage' }))
  expect(rowNames()).toEqual(['repos.get', 'getForecast', 'issues.create', 'issues.list'])
  expect(sorts()).toEqual([null, 'descending'])
  expect(headings()).toEqual([])
  expect(rowOf('getForecast')).toHaveTextContent('Weather')
  expect(rowOf('repos.get')).toHaveTextContent('GitHub')

  await user.click(within(table()).getByRole('button', { name: 'Sort by usage' }))
  expect(rowNames()).toEqual(byName)
  expect(headings()).toHaveLength(2)
})

// Catches: toolbar counts that ignore the tab or the search, a choice with no
// tools left choosable, or an Unavailable choice shown when nothing is.
test('the toolbar filter counts each policy in the tab and the search', async () => {
  const [list, get] = github.tools
  const mixed = { ...github, tools: [{ ...list!, policy: 'enabled' as const }, get!] }
  const blocked = { ...weather, tools: weather.tools.map((o) => ({ ...o, available: false })) }
  const { show } = renderPage({ providers: [mixed, blocked] })
  expect(filterChoices()).toEqual(['All 3', 'Allow 1', 'Ask 2', 'Block 0 (off)', 'Unavailable 1'])
  await filterTools('get')
  expect(filterChoices()).toEqual([
    'All 2',
    'Allow 0 (off)',
    'Ask 2',
    'Block 0 (off)',
    'Unavailable 1',
  ])
  await filterTools('')
  await show(null, mixed.id)
  expect(filterChoices()).toEqual(['All 2', 'Allow 1', 'Ask 1', 'Block 0 (off)'])
})

// Catches: an unavailable count that still filters from the heading, or a
// destructive tool that runs without approval and no warning, as saved or while
// a change is in flight.
test('a provider heading shows its unavailable tools and destructive risk', async () => {
  const [list, get] = github.tools
  const risky = {
    ...github,
    readyCount: 1,
    tools: [
      { ...list!, hints: { destructive: true }, policy: 'enabled' as const },
      { ...get!, available: false },
    ],
  }
  renderPage({ providers: [risky], action: () => new Promise(() => {}) })
  expect(githubHeading()).toHaveTextContent('1 unavailable')
  expect(within(githubHeading()).queryByRole('button', { name: /unavailable/ })).toBeNull()
  expect(githubHeading()).toHaveTextContent('1 destructive tool allowed')

  await choosePolicy('issues.list', 'Ask')
  expect(githubHeading()).not.toHaveTextContent('destructive')
})

// Catches: the page drawing its own sticky provider and group rows instead of
// nesting Wax groups, which loses the two-level sticky stack.
test('nests each tool group inside its provider group', () => {
  const grouped = provider(github.id, 'GitHub', 'github', [
    ['repos.get', 'repos'],
    ['issues.list', 'issues'],
  ])
  renderPage({ providers: [grouped, weather] })
  // Each heading row, provider or tool group, carries its level.
  const levels = within(table())
    .getAllByRole('row')
    .map((row) => row.getAttribute('aria-level'))
    .filter(Boolean)
  expect(levels).toEqual(['1', '2', '2', '1', '2'])
})

// Catches: a collapsed heading that still lists its rows or lets the cursor
// into them, a heading checkbox that drops the members it hides, or a filter
// that opens a collapsed provider or drops its match count.
test('a collapsed heading hides its rows but still counts and selects them', async () => {
  const grouped = provider(github.id, 'GitHub', 'github', [
    ['repos.get', 'repos'],
    ['issues.list', 'issues'],
    ['issues.create', 'issues'],
  ])
  renderPage({ providers: [grouped, weather] })
  await user.click(screen.getByRole('button', { name: 'Collapse issues' }))
  expect(rowNames()).toEqual(['repos.get', 'getForecast'])
  expect(screen.getByRole('button', { name: 'Expand issues' })).toHaveAttribute(
    'aria-expanded',
    'false',
  )
  // The cursor stops on the collapsed heading and steps over its rows.
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursorName()).toBe('issues')
  await user.keyboard('{ArrowDown}')
  expect(cursorName()).toBe('repos')

  await user.click(checkbox('Select issues tools'))
  expect(selectionCount()).toBe('2 selected')

  await user.click(screen.getByRole('button', { name: 'Collapse GitHub' }))
  await filterTools('issues')
  expect(rowNames()).toEqual([])
  const heading = within(table()).getByRole('rowgroup', { name: 'GitHub' })
  expect(within(heading).getByText('2 of 3 tools')).toBeInTheDocument()

  await filterTools('')
  await user.click(screen.getByRole('button', { name: 'Expand GitHub' }))
  await user.click(screen.getByRole('button', { name: 'Expand issues' }))
  expect(rowNames()).toEqual(['issues.create', 'issues.list', 'repos.get', 'getForecast'])
})

// Catches: a workspace with nothing to call showing an empty table instead
// of the way to add a first provider, because its private group is always there.
test('asks for a first provider while the only group is the empty private one', () => {
  renderPage({ providers: [privateProvider(mine.id)] })
  expect(screen.getByText('No providers yet')).toBeInTheDocument()
  expect(screen.queryByRole('table', { name: 'Tools' })).toBeNull()
})

// Catches: a collapse-all that misses the groups of a collapsed provider, or
// an expand-all that leaves a heading the reader collapsed by hand.
test('collapse all closes every heading and expand all opens them', async () => {
  const grouped = provider(github.id, 'GitHub', 'github', [
    ['repos.get', 'repos'],
    ['issues.list', 'issues'],
  ])
  renderPage({ providers: [grouped, weather] })
  await user.click(screen.getByRole('button', { name: 'Collapse issues' }))
  await user.click(screen.getByRole('button', { name: 'Collapse all' }))
  expect(rowNames()).toEqual([])

  // An opened provider keeps its groups collapsed.
  await user.click(screen.getByRole('button', { name: 'Expand GitHub' }))
  expect(rowNames()).toEqual([])
  expect(screen.getByRole('button', { name: 'Expand repos' })).toBeInTheDocument()

  // While no rows show, the button expands, so it opens every heading.
  await user.click(screen.getByRole('button', { name: 'Collapse GitHub' }))
  await user.click(screen.getByRole('button', { name: 'Expand all' }))
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  expect(screen.getByRole('button', { name: 'Collapse all' })).toBeInTheDocument()
})

// Catches: a tool route into a collapsed provider that leaves its row
// out, so the reveal finds nothing, or that clears the collapse for good.
test('opening a tool shows its row inside a collapsed provider', async () => {
  const { show } = renderPage()
  await user.click(screen.getByRole('button', { name: 'Collapse GitHub' }))
  expect(rowNames()).toEqual(['getForecast'])

  await show(github.tools[1]!)
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])

  await show(null, null)
  expect(rowNames()).toEqual(['getForecast'])
})

// Catches: tool groups sorted by code point, which puts "Beta" before
// "apple" and splits the list by case.
test('tool groups sort alphabetically without regard to case', () => {
  const mixed = provider(github.id, 'GitHub', 'github', [
    'misc.first',
    ['zebra.first', 'zebra'],
    ['apple.first', 'apple'],
    ['beta.first', 'Beta'],
  ])
  renderPage({ providers: [mixed] })
  // Each group heading's checkbox is named after its group.
  const boxes = screen.getAllByRole('checkbox')
  const positions = ['apple', 'Beta', 'Other', 'zebra'].map((group) =>
    boxes.indexOf(checkbox(`Select ${group} tools`)),
  )
  expect(positions).toEqual(positions.toSorted((a, b) => a - b))
})

// Catches: Coral's own groups sorted among the providers you add, instead of
// Coral then the private tools leading the rail and the table, or either
// linking to settings it does not have.
test('Coral then the private tools lead the providers you add, and neither has settings', () => {
  const coral = builtinProvider('00000000-0000-4000-8000-000000000003')
  const arcade = provider('00000000-0000-4000-8000-000000000004', 'Arcade', 'arcade')
  renderPage({ providers: [weather, mine, arcade, coral, github] })
  const gears = screen.getAllByRole('link', { name: / settings$/ })
  expect(gears.map((gear) => gear.getAttribute('aria-label'))).toEqual([
    'Arcade settings',
    'GitHub settings',
    'Weather settings',
  ])
  expect(providerGroups()).toEqual(['Coral', 'Private tools', 'Arcade', 'GitHub', 'Weather'])
  const entries = within(rail()).getAllByRole('link')
  const positions = ['All tools', 'Coral', 'Private tools', 'Arcade', 'GitHub', 'Weather'].map(
    (name) => entries.indexOf(tabLink(name)),
  )
  expect(positions).toEqual(positions.toSorted((a, b) => a - b))
})

const groupHeading = (name: string) => within(table()).getByRole('rowgroup', { name })

// Catches: a heading without its count or the way to its group's own page,
// a filter that misses a tool in some group, or a private tool that repeats
// its namespace under the heading that already shows it.
test('each group heading counts its tools and opens its page, and one filter searches them all', async () => {
  renderPage({ providers: [github, weather, mine] })
  expect(providerGroups()).toEqual(['Private tools', 'GitHub', 'Weather'])
  expect(
    ['GitHub', 'Weather', 'Private tools'].map(
      // The group's own count comes first; its tool groups follow with theirs.
      (name) => within(groupHeading(name)).getAllByText(/^\d+ tools?$/)[0]!.textContent,
    ),
  ).toEqual(['2 tools', '1 tool', '3 tools'])
  // The first rail entry counts every tool, the private ones included.
  expect(tabLink('All tools')).toHaveTextContent('All tools6')
  expect(within(groupHeading('GitHub')).getByRole('link', { name: 'GitHub' })).toHaveAttribute(
    'href',
    github.path,
  )
  expect(
    within(groupHeading('Private tools')).getByRole('link', { name: 'Private tools' }),
  ).toHaveAttribute('href', '/workspaces/demo/tools/private')
  // Under `people` and `reports`, each private tool shows the rest of its path.
  expect(rowNames()).toEqual(['greet', 'greet', 'total', 'issues.list', 'repos.get', 'getForecast'])
  expect(screen.getByRole('link', { name: 'people.greet' }).textContent).toBe('greet')

  await filterTools('get')
  expect(rowNames()).toEqual(['repos.get', 'getForecast'])
  expect(within(groupHeading('GitHub')).getByText('1 of 2 tools')).toBeInTheDocument()
  // A match can span the namespace the heading shows and the name the row shows.
  await filterTools('people.gr')
  expect(providerGroups()).toEqual(['Private tools'])
  expect(rowNames()).toEqual(['greet', 'greet'])
})

// Catches: an agent's private tool opening your own at the same path, so an
// owner would review and publish the wrong source; two private tools at one
// path sharing a row or a control's name; or your own private tool opening at
// a provider's address.
test('lists an agent’s private tool beside your own, and opens each at its own address', async () => {
  const { router, show } = renderPage({ providers: [github, mine] })
  await show(null, mine.id)
  expect(rowNames()).toEqual(['greet', 'greet', 'total'])
  const rows = within(groupHeading('Private tools'))
    .getAllByRole('row')
    .filter((row) => !row.hasAttribute('aria-level'))
  // The agent's tool follows your own at the same path, and names its agent.
  expect(rows.map((row) => within(row).queryByText('Worker') !== null)).toEqual([
    false,
    true,
    false,
  ])
  // Each control names the one tool it acts on.
  expect(checkbox('Select people.greet')).toBeInTheDocument()
  expect(checkbox('Select people.greet of agent Worker')).toBeInTheDocument()
  expect(
    within(rows[1]!).getByRole('radiogroup', { name: 'Policy for people.greet of agent Worker' }),
  ).toBeInTheDocument()

  expect(within(rows[1]!).getByRole('link')).toHaveAccessibleName('people.greet of agent Worker')

  await user.click(rows[1]!)
  expect(router.state.location.pathname).toBe(
    '/workspaces/demo/tools/private/people.greet/agents/worker',
  )
  await user.click(rows[0]!)
  expect(router.state.location.pathname).toBe('/workspaces/demo/tools/private/people.greet')
})

const greet: ToolDetailView = {
  kind: 'private',
  agent: null,
  relativePath: 'people.greet',
  description: 'Greets **one** person.',
  declaration: 'declare function greet(name: string): Promise<string>',
  source: 'export default async (name: string) => `Hello ${name}`',
  diagnostics: [{ category: 'warning', code: 6133, message: 'Unused.', line: 2, column: null }],
  diagnosticsTruncated: false,
  savedAt: '2026-11-09T14:27:03.000Z',
}

// Catches: a private tool's drawer without what Coral saved of it, with a
// provider tool's call path, or offering a delete an archived workspace refuses.
test.each([
  ['an active', 'active', true],
  ['an archived', 'archived', false],
] as const)('the drawer shows your private tool in %s workspace', (_, state, deletes) => {
  renderPage({
    providers: [github, mine],
    entry: '/workspaces/demo/tools/private/people.greet',
    selectedTool: mine.tools[0]!,
    selectedToolDetail: greet,
    workspace: { ...workspaceView, state },
  })
  const drawer = screen.getByRole('dialog')
  expect(within(drawer).getByRole('heading', { name: 'people.greet' })).toBeInTheDocument()
  expect(within(drawer).getByText('one').tagName).toBe('STRONG')
  expect(within(drawer).getByText('coral.private.people.greet')).toBeInTheDocument()
  expect(within(drawer).getByText('2026-11-09 14:27 UTC')).toBeInTheDocument()
  for (const section of ['Policy', 'Declaration', 'Source', 'Diagnostics'])
    expect(within(drawer).getByRole('heading', { name: section })).toBeInTheDocument()
  expect(within(drawer).getByText('TS6133: Unused.')).toBeInTheDocument()
  expect(within(drawer).queryAllByRole('button', { name: 'Delete private tool' })).toHaveLength(
    deletes ? 1 : 0,
  )
})

// Catches: a member offered a publish Coral refuses, with nothing to say an
// owner can; an owner with no way to publish their own or an agent's private
// tool; or a publish offered, or an owner named as able to publish, in an
// archived workspace, which accepts none.
test.each([
  ['an owner', { effectiveRole: 'owner' }, mine.tools[0]!, true, false],
  ['an owner, of an agent’s private tool', { effectiveRole: 'owner' }, mine.tools[1]!, true, false],
  ['a member', { effectiveRole: 'member' }, mine.tools[0]!, false, true],
  ['an owner of an archived workspace', { state: 'archived' }, mine.tools[0]!, false, false],
  // Nobody publishes until an owner restores the workspace.
  [
    'a member of an archived workspace',
    { effectiveRole: 'member', state: 'archived' },
    mine.tools[0]!,
    false,
    false,
  ],
] as const)('the drawer offers a publish to %s', (_, role, tool, publishes, says) => {
  renderPage({
    providers: [github, mine],
    entry: toolPath(mine, tool),
    selectedTool: tool,
    selectedToolDetail: { ...greet, agent: tool.agent?.id ?? null },
    workspace: { ...workspaceView, ...role },
  })
  const drawer = screen.getByRole('dialog')
  expect(within(drawer).queryAllByRole('button', { name: 'Publish' })).toHaveLength(
    publishes ? 1 : 0,
  )
  expect(within(drawer).queryAllByText('Only an owner can publish to shared tools.')).toHaveLength(
    says ? 1 : 0,
  )
})

const shared = sharedProvider('00000000-0000-4000-8000-0000000000ab', ['mail.remind'])
const ada = { user: 'users/ada', name: 'Ada', email: 'ada@example.test', local: false }
const remind: SharedToolDetailView = {
  kind: 'shared',
  relativePath: 'mail.remind',
  description: 'Reminds **one** person.',
  declaration: 'export default function remind(name: string): Promise<string>;',
  source: 'export default async (name: string) => `Remember, ${name}`',
  diagnostics: [],
  diagnosticsTruncated: false,
  revision: 2,
  publisher: ada,
  publishedAt: '2026-11-09T14:27:03.000Z',
  revisions: [
    {
      revision: 2,
      description: 'Reminds **one** person.',
      publisher: ada,
      publishedAt: '2026-11-09T14:27:03.000Z',
      restoredFrom: null,
    },
    {
      revision: 1,
      description: 'Reminds a person.',
      publisher: null,
      publishedAt: '2026-11-02T09:00:00.000Z',
      restoredFrom: null,
    },
  ],
  events: [
    { id: 'event-2', action: 'published', revision: 2, actor: ada, at: '2026-11-09T14:27:03.000Z' },
    {
      id: 'event-1',
      action: 'published',
      revision: 1,
      actor: null,
      at: '2026-11-02T09:00:00.000Z',
    },
  ],
  eventsTruncated: false,
}
const openShared = (options: Parameters<typeof renderPage>[0] = {}) => {
  const rendered = renderPage({
    providers: [github, mine, shared],
    entry: '/workspaces/demo/tools/shared/mail.remind',
    selectedTool: shared.tools[0]!,
    selectedToolDetail: remind,
    ...options,
  })
  return { ...rendered, drawer: screen.getByRole('dialog') }
}

// Catches: the shared and the private group out of their place under Coral's
// tools, the shared group named as a provider, or an empty shared group that
// reads as a provider with no tools, or tells an owner to ask an owner.
test('lists the shared tools under Coral’s, and says when there are none', async () => {
  const coral = builtinProvider('00000000-0000-4000-8000-0000000000c0')
  const { rerenderProviders } = renderPage({ providers: [github, mine, shared, coral] })
  expect(providerGroups()).toEqual(['Coral', 'Shared tools', 'Private tools', 'GitHub'])
  expect(tabLink('Shared tools')).toHaveAttribute('href', '/workspaces/demo/tools/shared')
  expect(screen.getByRole('link', { name: 'mail.remind' })).toHaveAttribute(
    'href',
    '/workspaces/demo/tools/shared/mail.remind',
  )
  await rerenderProviders([github, sharedProvider(shared.id)])
  expect(
    within(groupHeading('Shared tools')).getByText(
      'No shared tools yet. Publish one from a private tool.',
    ),
  ).toBeInTheDocument()
})

// Catches: a member told to publish, which Coral refuses them.
test('tells a member that only an owner publishes a shared tool', () => {
  renderPage({
    providers: [github, sharedProvider(shared.id)],
    workspace: { ...workspaceView, effectiveRole: 'member' },
  })
  expect(
    within(groupHeading('Shared tools')).getByText(
      'No shared tools yet. Only an owner can publish one.',
    ),
  ).toBeInTheDocument()
})

// Catches: a shared tool's drawer that does not say who published it and at
// which revision, that names a person who is no longer an owner by an
// identifier, or that leaves out its revisions or its history.
test('the drawer shows a shared tool with who published it, its revisions and its history', async () => {
  const { drawer } = openShared()
  expect(within(drawer).getByRole('heading', { name: 'mail.remind' })).toBeInTheDocument()
  expect(within(drawer).getByText('coral.shared.mail.remind')).toBeInTheDocument()
  expect(within(drawer).getByText('Published by Ada · Revision 2')).toBeInTheDocument()
  for (const section of ['Policy', 'Declaration', 'Source', 'Diagnostics'])
    expect(within(drawer).getByRole('heading', { name: section })).toBeInTheDocument()

  await user.click(within(drawer).getByRole('tab', { name: 'Revisions' }))
  const revisions = within(within(drawer).getByRole('table', { name: 'Revisions' }))
    .getAllByRole('row')
    .slice(1)
    .map((row) => row.textContent)
  expect(revisions).toEqual([
    expect.stringMatching(/^Revision 2Current.*Ada.*2026-11-09 14:27 UTC/),
    // Whoever published revision 1 is no longer an owner of the workspace.
    expect.stringMatching(/^Revision 1A former owner2026-11-02 09:00 UTC/),
  ])

  await user.click(within(drawer).getByRole('tab', { name: 'History' }))
  const history = within(within(drawer).getByRole('table', { name: 'History' }))
    .getAllByRole('row')
    .slice(1)
    .map((row) => row.textContent)
  expect(history).toEqual([
    expect.stringMatching(/^Published revision 2Ada/),
    expect.stringMatching(/^Published revision 1A former owner/),
  ])
})

// Catches: a publisher who left shown by name or by identifier on the line
// that heads the drawer.
test('the drawer names a publisher who is no longer an owner as a former owner', () => {
  const { drawer } = openShared({ selectedToolDetail: { ...remind, publisher: null } })
  expect(within(drawer).getByText('Published by a former owner · Revision 2')).toBeInTheDocument()
})

const restoreReview: PublishReviewResult = {
  refusal: null,
  review: {
    origin: { kind: 'restore', revision: 1 },
    path: 'mail.remind',
    description: 'Reminds a person.',
    source: 'export default async () => "first"',
    declaration: 'export default function (): Promise<string>;',
    diagnostics: [],
    diagnosticsTruncated: false,
    previous: {
      revision: 2,
      description: remind.description,
      source: remind.source,
      declaration: remind.declaration,
    },
    calledTools: [],
    sharedTools: [],
    runtimeBuiltPaths: [],
    defaultPolicy: 'enabled',
  },
}

// Catches: an owner with no way to restore an earlier revision, a restore
// offered for the revision that is already current, a restore that reviews
// another revision or path, or a member offered a restore or a removal.
test('an owner restores an earlier revision of a shared tool from its revisions', async () => {
  const reviewed: string[] = []
  const { drawer } = openShared({
    routes: [
      {
        path: '/workspaces/demo/shared-tools/publish',
        loader: ({ request }) => {
          reviewed.push(new URL(request.url).search)
          return restoreReview
        },
      },
    ],
  })
  await user.click(within(drawer).getByRole('tab', { name: 'Revisions' }))
  expect(within(drawer).queryByRole('button', { name: 'Restore revision 2' })).toBeNull()
  await user.click(within(drawer).getByRole('button', { name: 'Restore revision 1' }))

  expect(await screen.findByRole('heading', { name: 'Restore revision 1' })).toBeInTheDocument()
  expect(await screen.findByRole('button', { name: 'Publish revision 3' })).toBeEnabled()
  expect(reviewed).toEqual(['?path=mail.remind&restore=1'])
})

// Catches: a member offered a restore or a removal that Coral refuses.
test('a member reads a shared tool and changes nothing', async () => {
  const { drawer } = openShared({ workspace: { ...workspaceView, effectiveRole: 'member' } })
  expect(within(drawer).queryByRole('button', { name: 'Remove shared tool' })).toBeNull()
  await user.click(within(drawer).getByRole('tab', { name: 'Revisions' }))
  expect(within(drawer).queryByRole('button', { name: /^Restore/ })).toBeNull()
})

// Catches: a removal at a revision other than the one on screen; a refused
// removal that closes its dialog or does not say which shared tool the owner
// must deal with first; or a dialog that takes the revision of the reload a
// refusal brings, so a second click removes a revision the owner never saw.
test('a refused removal of a shared tool stays in its dialog with the reason', async () => {
  const refusal =
    'The shared tool coral.shared.mail.digest calls this one. Change or remove that shared tool first.'
  const posted: FormData[] = []
  const { drawer, reloadDetail } = openShared({
    action: async (request) => {
      posted.push(await request.formData())
      return { error: refusal, credential: null }
    },
  })
  await user.click(within(drawer).getByRole('button', { name: 'Remove shared tool' }))
  const dialog = await screen.findByRole('dialog', { name: 'Remove shared tool' })
  await user.click(within(dialog).getByRole('button', { name: 'Remove shared tool' }))

  expect(await within(dialog).findByText(refusal)).toBeInTheDocument()
  expect(Object.fromEntries(posted[0]!)).toMatchObject({
    intent: 'removeSharedTool',
    path: 'mail.remind',
    expectedRevision: '2',
  })

  // Another owner published revision 3, and the page reloads with it.
  await reloadDetail({ ...remind, revision: 3 })
  await user.click(within(dialog).getByRole('button', { name: 'Remove shared tool' }))
  await act(async () => {})
  expect(posted.map((form) => form.get('expectedRevision'))).toEqual(['2', '2'])
})

// Catches: a member offered a policy change on their private tools that
// only an owner may make.
test.each([
  ['an owner', 'owner', false],
  ['a member', 'member', true],
] as const)('on the private page, %s reads each policy', async (_, role, readOnly) => {
  const { show } = renderPage({
    providers: [github, mine],
    workspace: { ...workspaceView, effectiveRole: role },
  })
  await show(null, mine.id)
  for (const radio of within(groupHeading('Private tools')).getAllByRole('radio'))
    expect(radio.hasAttribute('disabled')).toBe(readOnly)
})

// Catches: a policy form naming the provider's installation instead of its
// workspace-local identifier, which the policy action looks up.
test('policy forms submit the workspace-local provider identifier, not an installation', () => {
  renderPage()
  for (const owner of [github, weather]) {
    const group = within(screen.getByRole('table', { name: 'Tools' })).getByRole('rowgroup', {
      name: owner.displayName,
    })
    const forms = [...group.querySelectorAll('form')]
      .map((form) => new FormData(form))
      .filter((data) => data.get('intent') === 'policy')
    expect(forms).toHaveLength(owner.tools.length)
    for (const data of forms) {
      expect(data.get('provider')).toBe(owner.id)
      expect(data.has('installation')).toBe(false)
    }
  }
})

const rail = () => screen.getByRole('navigation', { name: 'installed providers' })
const tabLink = (name: string) =>
  within(rail()).getByRole('link', { name: new RegExp(`${name}( \\d+)?$`) })

// Catches: a tab showing other providers, a change of tab dropping the
// selection, or the rail filter narrowing the table instead of the rail.
test('each rail entry is a tab, and all tools lists every group', async () => {
  const { router } = renderPage()
  expect(tabLink('All tools')).toHaveAttribute('aria-current', 'page')
  expect(providerGroups()).toEqual(['GitHub', 'Weather'])
  await user.click(checkbox('Select repos.get'))

  await user.click(tabLink('Weather'))
  expect(router.state.location.pathname).toBe(weather.path)
  expect(tabLink('Weather')).toHaveAttribute('aria-current', 'page')
  expect(providerGroups()).toEqual(['Weather'])
  expect(rowNames()).toEqual(['getForecast'])
  expect(selectionCount()).toBe('1 selected')

  await filterProviders('git')
  expect(within(rail()).queryByRole('link', { name: /Weather$/ })).toBeNull()
  expect(providerGroups()).toEqual(['Weather'])

  await user.click(tabLink('All tools'))
  expect(providerGroups()).toEqual(['GitHub', 'Weather'])
  expect(checkbox('Select repos.get')).toBeChecked()
})

// The rail's message when its filter matches no provider, beside its entries.
const railStatus = () =>
  within(screen.getByRole('complementary', { name: 'installed providers' })).getByRole('status')

const recipe = (id: string, displayName: string): SourceChoice => ({
  id,
  displayName,
  description: '',
  kind: 'openapi',
  document: `${id}.json`,
})

// Catches: one catalog match that hides which of its sources it adds, or
// a source added from the rail other than as the catalog adds it.
test('a filter that matches one catalog service offers each of its sources', async () => {
  const added: FormData[] = []
  renderPage({
    providers: [weather],
    recipes: [
      recipe('discord', 'Discord'),
      {
        id: 'discord_mcp',
        service: 'discord',
        displayName: 'Discord',
        description: '',
        kind: 'mcp',
        url: 'https://mcp.discord.example/mcp',
      },
      recipe('dropbox', 'Dropbox'),
    ],
    action: async (request) => {
      added.push(await request.formData())
      return null
    },
  })
  await filterProviders('disc')
  const empty = railStatus()
  expect(empty).toHaveTextContent('No providers match “disc”Add it from the catalog.')
  const source = (name: string) => within(empty).getByRole('button', { name })
  expect(within(empty).getAllByRole('button')).toEqual([
    source('Add Discord MCP'),
    source('Add Discord API'),
  ])

  await user.click(within(empty).getByRole('button', { name: 'Add Discord API' }))
  expect(added.map((body) => [body.get('intent'), body.get('recipe')])).toEqual([
    ['prepare', 'discord'],
  ])
})

// Catches: a filter that finds no installed provider ending at a dead end, or
// the add dialog dropping what the reader already typed.
test.each([
  {
    query: 'd',
    hint: 'The catalog has 2 matches.',
    button: 'Show matches',
    opens: { tab: 'Catalog', search: 'd' },
  },
  {
    query: 'notion',
    hint: 'It is not in the catalog.',
    button: 'Add custom provider',
    opens: { tab: 'Add custom' },
  },
])(
  'a filter that matches no provider offers what the catalog has for “$query”',
  async ({ query, hint, button, opens }) => {
    renderPage({
      providers: [weather],
      recipes: [recipe('discord', 'Discord'), recipe('dropbox', 'Dropbox')],
    })
    await filterProviders(query)
    const empty = railStatus()
    expect(empty).toHaveTextContent(`No providers match “${query}”${hint}`)

    await user.click(within(empty).getByRole('button', { name: button }))
    const dialog = screen.getByRole('dialog', { name: 'Add a provider' })
    expect(within(dialog).getByRole('tab', { name: opens.tab })).toHaveAttribute(
      'aria-selected',
      'true',
    )
    if (opens.search)
      expect(within(dialog).getByRole('searchbox', { name: 'Search the catalog' })).toHaveValue(
        opens.search,
      )
  },
)

// Catches: a member offered an action only an owner can take.
test('a filter that matches no provider tells a member who can add it', async () => {
  renderPage({
    workspace: { ...workspaceView, effectiveRole: 'member' },
    providers: [weather],
    recipes: [recipe('discord', 'Discord')],
  })
  await filterProviders('disc')
  const empty = railStatus()
  expect(empty).toHaveTextContent('Ask a workspace owner to add it.')
  expect(within(empty).queryByRole('button')).toBeNull()
})

// Catches: a tool route from another provider opening its drawer over a
// tab that leaves its row out.
test('a tool route keeps a tab that lists it, and otherwise opens its provider', async () => {
  const { show } = renderPage()
  await show(null, github.id)
  await show(github.tools[0]!)
  expect(rowNames()).toEqual(['issues.list', 'repos.get'])

  await show(weather.tools[0]!)
  expect(rowNames()).toEqual(['getForecast'])

  await show(null, null)
  await show(github.tools[0]!)
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
})

// Catches: the filter dropping selections it hides, or Apply leaving out
// the selected tools the filter hides.
test('selection survives filtering', async () => {
  renderPage()
  await user.click(checkbox('Select getForecast'))
  await user.click(checkbox('Select repos.get'))
  expect(selectionCount()).toBe('2 selected')

  await filterTools('forecast')
  expect(rowNames()).toEqual(['getForecast'])
  expect(selectionCount()).toBe('2 selected')

  await filterTools('')
  expect(checkbox('Select repos.get')).toBeChecked()
  expect(checkbox('Select getForecast')).toBeChecked()

  // The header checkbox reaches only what the filter shows; Clear selection
  // also clears what it hides.
  await filterTools('forecast')
  await user.click(within(bulkForm()).getByRole('button', { name: 'Clear selection' }))
  expect(screen.queryByRole('form', { name: 'Bulk policy' })).not.toBeInTheDocument()
  await filterTools('')
  expect(checkbox('Select repos.get')).not.toBeChecked()
})

// Catches: a later filter dropping what an earlier one selected, or the
// selected view, from the no-match status or the bulk form, missing a
// selection the searches hide.
test('selection builds up across filters and the selected view shows all of it', async () => {
  renderPage()
  const all = () => checkbox('Select all matching tools')
  await filterTools('get')
  await user.click(all())
  await filterTools('list')
  await user.click(all())
  expect(rowNames()).toEqual(['issues.list'])
  expect(selectionCount()).toBe('3 selected')

  await filterTools('nothing matches')
  expect(table()).toHaveTextContent(
    'No tools match “nothing matches”. The filter hides 3 selected tools.',
  )
  await filterProviders('Weather')
  await user.click(within(table()).getByRole('button', { name: 'Show all selected tools' }))
  expect(bulkCount()).toHaveAttribute('aria-pressed', 'true')
  expect(screen.getByLabelText('Filter tools')).toHaveValue('')
  expect(screen.getByLabelText('Filter providers')).toHaveValue('')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])

  // An unchecked row stays in the view until the reader leaves it.
  await user.click(checkbox('Select repos.get'))
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])

  await user.click(bulkCount())
  expect(selectionCount()).toBe('2 selected')

  // The bulk form's count opens the same view from under any search, and leaves it.
  await filterTools('nothing matches')
  await user.click(bulkCount())
  expect(bulkCount()).toHaveAttribute('aria-pressed', 'true')
  expect(screen.getByLabelText('Filter tools')).toHaveValue('')
  expect(rowNames()).toEqual(['issues.list', 'getForecast'])
  await user.click(bulkCount())
  expect(bulkCount()).toHaveAttribute('aria-pressed', 'false')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  // It types four filters and renders the whole catalog after each. That takes
  // under 1 s here, but longer than the default 5 s on a starved CI runner.
}, 15_000)

// Catches: a filter that leaves no provider showing an empty table, a status
// that drops the query, or one that offers a hidden selection when none is.
test.each([
  { by: 'search', selects: false, status: 'No tools match “nothing”.' },
  {
    by: 'search',
    selects: true,
    status: 'No tools match “nothing”. The filter hides 2 selected tools.',
  },
  { by: 'policy', selects: false, status: 'No matching tools.' },
  { by: 'policy', selects: true, status: 'No matching tools. The filter hides 1 selected tool.' },
])('a $by that leaves no provider says so once', async ({ by, selects, status }) => {
  const [list, get] = github.tools
  const mixed = { ...github, tools: [{ ...list!, policy: 'enabled' as const }, get!] }
  const { show } = renderPage({ providers: [mixed, weather] })
  if (selects) {
    await user.click(checkbox('Select repos.get'))
    await user.click(checkbox('Select getForecast'))
  }
  if (by === 'search') {
    await filterTools('nothing')
  } else {
    // Weather lists no tool the Allow filter keeps. The tab, not the filter,
    // hides the selected GitHub tool, so the status leaves it out.
    await choosePolicyFilter('Allow')
    await show(null, weather.id)
  }
  for (const name of ['GitHub', 'Weather'])
    expect(within(table()).queryByRole('rowgroup', { name })).toBeNull()
  // One row says so, in place of every provider's.
  expect(
    within(table())
      .getAllByRole('row')
      .filter((row) => row.textContent?.startsWith(status)),
  ).toHaveLength(1)
  expect(within(table()).queryByRole('button', { name: 'Show all selected tools' }) !== null).toBe(
    selects,
  )
})

// Catches: the filter matching text the row does not show, such as the call
// path's namespace, not marking where the name matches, or not reading the
// shared filter syntax.
test('the tool filter matches only the name and bolds each match', async () => {
  renderPage()
  await filterTools('weather')
  expect(rowNames()).toEqual([])

  await filterTools('E')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  expect(boldIn('repos.get')).toEqual(['e', 'e'])
  expect(boldIn('getForecast')).toEqual(['e', 'e'])
})

// Catches: one member's selection not rolling up to its provider and the
// header, or the header selecting only some of the tools that match.
test('selection rolls up to its provider and the header selects every match', async () => {
  renderPage()
  const all = () => checkbox('Select all matching tools')
  await user.click(checkbox('Select repos.get'))
  expect(checkbox('Select GitHub tools')).toBePartiallyChecked()
  expect(all()).toBePartiallyChecked()
  expect(selectionCount()).toBe('1 selected')

  await user.click(checkbox('Select GitHub tools'))
  expect(checkbox('Select GitHub tools')).toBeChecked()
  expect(selectionCount()).toBe('2 selected')

  await filterTools('forecast')
  await user.click(all())
  await filterTools('')
  expect(selectionCount()).toBe('3 selected')
  expect(all()).toBeChecked()
  expect(all()).not.toBePartiallyChecked()

  await filterTools('nothing matches')
  expect(all()).toBeDisabled()
  expect(selectionCount()).toBe('3 selected')

  await filterTools('')
  await user.click(all())
  expect(all()).not.toBeChecked()
  expect(screen.queryByRole('form', { name: 'Bulk policy' })).toBeNull()
})

// Catches: closing the drawer scrolling the table back to the provider heading,
// which loses the reader's place. Tool names carry the workspace prefix,
// as Coral's do.
test('closing the drawer leaves the table where the reader left it', async () => {
  const linked = github.tools[1]!
  const { show } = renderPage()
  await show(linked)
  // The table's own scroll maths is covered beside `Table.useVirtualRows`;
  // this counts only the scrolls the page asks of it.
  const port = screen.getByRole('table', { name: 'Tools', hidden: true })
  const scrolls: number[] = []
  port.scrollTo = ((options: ScrollToOptions) => {
    scrolls.push(options.top!)
  }) as typeof port.scrollTo

  await show(null, null)
  expect(scrolls).toEqual([])
  expect(providerGroups()).toEqual(['GitHub', 'Weather'])

  // Opening another tool's drawer reveals its row.
  await show(weather.tools[0]!)
  expect(scrolls).toHaveLength(1)
})

// Catches: "select all matching" taking the drawer's tool, which the
// filter did not match, and the summary counting it as hidden.
test('the drawer tool stays listed when the filter would hide it', async () => {
  const linked = github.tools[1]!
  const { show } = renderPage()
  await show(linked)
  await filterTools('forecast')
  expect(rowNames()).toEqual(['repos.get', 'getForecast'])

  await user.click(checkbox('Select all matching tools', true))
  expect(selectionCount(true)).toBe('1 selected')
  expect(checkbox('Select getForecast', true)).toBeChecked()
  expect(checkbox('Select repos.get', true)).not.toBeChecked()
})

// Coral refuses getForecast and saves the rest.
const mixedOutcome: Action = async (request) => {
  const data = await request.formData()
  const selected = data
    .getAll('selectedTool')
    .map((value) => JSON.parse(String(value)) as { provider: string; tool: string })
  return {
    error: null,
    message: 'ok',
    tools: selected.map((target) =>
      target.tool.endsWith('getForecast')
        ? { ...target, status: 'failed', reason: 'Coral refused.' }
        : { ...target, status: 'updated' },
    ),
  }
}

// Catches: a bulk save clearing every selected tool on any success, or a
// row hiding its own refusal.
test('a mixed bulk outcome keeps only the failed tools selected', async () => {
  renderPage({ action: mixedOutcome })
  await user.click(checkbox('Select getForecast'))
  await user.click(checkbox('Select repos.get'))
  await user.click(within(bulkForm()).getByRole('button', { name: 'Apply' }))
  await settle()

  expect(selectionCount()).toBe('1 selected')
  expect(checkbox('Select getForecast')).toBeChecked()
  expect(checkbox('Select repos.get')).not.toBeChecked()
  // The refused row says so; the saved row does not.
  expect(rowOf('getForecast')).toHaveTextContent('Not saved')
  expect(rowOf('repos.get')).not.toHaveTextContent('Not saved')
})

// Catches: the bulk form accepting a submission while one of its selected
// tools already has a change in flight from its own row.
test('a selected row change in flight holds the bulk form', async () => {
  let release!: () => void
  const held = new Promise<void>((resolve) => {
    release = resolve
  })
  renderPage({
    action: async (request) => {
      await held
      return mixedOutcome(request)
    },
  })
  await user.click(checkbox('Select repos.get'))
  await choosePolicy('repos.get', 'Block')
  const apply = within(bulkForm()).getByRole('button', { name: 'Apply' })
  expect(apply).toBeDisabled()

  release()
  await settle()
  expect(apply).toBeEnabled()
})

// Catches: two changes that arrive before React re-renders the disabled
// toggle both reaching Coral, where the later one could win.
test('a second change made before the page re-renders is not sent', async () => {
  const sent: string[] = []
  renderPage({
    action: async (request) => {
      sent.push(String((await request.formData()).get('access')))
      return { error: null, message: 'ok' }
    },
  })
  const disable = within(rowOf('repos.get')).getByRole('radio', { name: 'Block' })
  const enable = within(rowOf('repos.get')).getByRole('radio', { name: 'Allow' })
  // Both clicks land in one batch, so the second sees the toggle still enabled.
  act(() => {
    disable.click()
    enable.click()
  })
  await settle()
  expect(sent).toEqual(['disabled'])
})

// Catches: a lost response read as a refusal or a success, when the policy
// may or may not have been saved.
test('a change whose response is lost says the policy is not confirmed', async () => {
  renderPage({ action: async () => undefined })
  await choosePolicy('repos.get', 'Block')
  await settle()
  expect(rowOf('repos.get')).toHaveTextContent('Not confirmed')
})

// Catches: a row keeping the refusal of an earlier attempt after a later one
// saved the policy.
test('changing a refused tool again clears its refusal', async () => {
  let answer = mixedOutcome
  renderPage({ action: (request) => answer(request) })
  await choosePolicy('getForecast', 'Block')
  await settle()
  expect(rowOf('getForecast')).toHaveTextContent('Not saved')

  answer = async () => ({ error: null, message: 'ok' })
  await choosePolicy('getForecast', 'Block')
  await settle()
  expect(rowOf('getForecast')).not.toHaveTextContent('Not saved')
})

// Catches: a toolbar filter that does not reach every provider, a selection
// that reaches past the filter, a row that vanishes while its own change is in
// flight, a filter the reader cannot clear, or one that keeps the selected view.
test('a toolbar filter narrows every provider to its tools until cleared', async () => {
  const [list, get] = github.tools
  const mixed = { ...github, tools: [{ ...list!, policy: 'enabled' as const }, get!] }
  const blocked = {
    ...weather,
    readyCount: 0,
    tools: weather.tools.map((o) => ({ ...o, available: false })),
  }
  // The policy change stays in flight, so only the page's own state moves.
  renderPage({ providers: [mixed, blocked], action: () => new Promise(() => {}) })

  await choosePolicyFilter('Allow')
  expect(rowNames()).toEqual(['issues.list'])
  // A provider with no match leaves the table, and a heading counts what the
  // filter leaves of its tools.
  expect(within(table()).queryByRole('rowgroup', { name: 'Weather' })).toBeNull()
  expect(within(table()).getByRole('rowgroup', { name: 'GitHub' })).toHaveTextContent(
    '1 of 2 tools',
  )
  await user.click(checkbox('Select all matching tools'))
  expect(selectionCount()).toBe('1 selected')
  await user.click(within(bulkForm()).getByRole('button', { name: 'Clear selection' }))

  // The changed row stays, and its choice stays choosable while it applies.
  await choosePolicy('issues.list', 'Block')
  expect(rowNames()).toEqual(['issues.list'])
  expect(policyChoice('Allow')).toHaveTextContent('Allow 0')
  expect(policyChoice('Allow')).toBeEnabled()

  await choosePolicyFilter('All')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  await choosePolicyFilter('Unavailable')
  expect(rowNames()).toEqual(['getForecast'])

  // A filter leaves the selected view.
  await choosePolicyFilter('All')
  await user.click(checkbox('Select issues.list'))
  await user.click(checkbox('Select repos.get'))
  await user.click(within(bulkForm()).getByRole('button', { name: '2 selected' }))
  await choosePolicyFilter('Block')
  expect(rowNames()).toEqual(['issues.list'])
  expect(within(bulkForm()).getByRole('button', { name: '2 selected' })).toHaveAttribute(
    'aria-pressed',
    'false',
  )
})

// Catches: a choice that misses a provider, or an "All" that keeps a filter.
test.each([
  ['Allow', ['issues.list']],
  ['Ask', ['repos.get', 'getForecast']],
  ['Unavailable', ['getForecast']],
  ['All', ['issues.list', 'repos.get', 'getForecast']],
])('the toolbar filter shows only %s tools', async (choice, names) => {
  const [list, get] = github.tools
  const mixed = { ...github, tools: [{ ...list!, policy: 'enabled' as const }, get!] }
  const blocked = {
    ...weather,
    readyCount: 0,
    tools: weather.tools.map((o) => ({ ...o, available: false })),
  }
  renderPage({ providers: [mixed, blocked] })
  await choosePolicyFilter(choice)
  expect(rowNames()).toEqual(names)
  expect(policyChoice(choice)).toBeChecked()
})

// Catches: a row that does not open its drawer, a control click that does, or
// a name click that the link and the row both navigate, which pushes two
// history entries.
// A click opens the row where the reader clicked it: the table does not scroll
// the row to the top as it does for a shared link.
test('clicking a row opens its tool, but its controls do not', async () => {
  const { router } = renderPage({ action: () => new Promise(() => {}) })
  const port = screen.getByRole('table', { name: 'Tools' })
  const scrolls: number[] = []
  port.scrollTo = ((options: ScrollToOptions) => {
    scrolls.push(options.top!)
  }) as typeof port.scrollTo

  await user.click(within(rowOf('repos.get')).getByRole('link', { name: 'repos.get' }))
  expect(router.state.location.pathname).toBe(pathOf(github.tools[1]!))
  await act(async () => router.navigate(-1))
  expect(router.state.location.pathname).toBe('/workspaces/demo/tools')
  await user.click(checkbox('Select issues.list'))
  await choosePolicy('issues.list', 'Block')
  expect(router.state.location.pathname).not.toMatch(/issues/)
  await user.click(within(rowOf('issues.list')).getAllByRole('cell')[1]!)
  expect(router.state.location.pathname).toBe(pathOf(github.tools[0]!))
  expect(scrolls).toEqual([])
})

// Catches: the browser's find taking the shortcut, which cannot reach the
// tools the virtualized table has not mounted.
test('the find shortcut focuses the tool filter', async () => {
  renderPage()
  await user.keyboard('{Control>}f{/Control}')
  expect(screen.getByLabelText('Filter tools')).toHaveFocus()
})

const cursorRow = () => table().querySelector<HTMLElement>('[data-active]')
// A heading names its row by its toggle; a tool by its link.
const cursorName = () => {
  const row = cursorRow()
  if (!row) return null
  if (!row.hasAttribute('aria-level')) return row.querySelector('a')?.textContent ?? null
  const toggle = row.querySelector('button[aria-expanded]')
  return toggle?.getAttribute('aria-label')?.replace(/^(Collapse|Expand) /, '') ?? null
}
// Catches: the arrow keys skipping tools, stopping in a heading, losing
// the search box's typing, or acting through a control that reads the key.
test('arrow keys move a cursor through the listed tools', async () => {
  // A policy change stays in flight; only the keys matter here.
  renderPage({ action: () => new Promise(() => {}) })
  expect(cursorName()).toBeNull()
  // Each heading is a stop, before the rows under it.
  const stops: (string | null)[] = []
  for (let i = 0; i < 7; i++) {
    await user.keyboard('{ArrowDown}')
    stops.push(cursorName())
  }
  expect(stops).toEqual([
    'GitHub',
    'Other',
    'issues.list',
    'repos.get',
    'Weather',
    'Other',
    'getForecast',
  ])
  // The last row holds the cursor.
  await user.keyboard('{ArrowDown}')
  expect(cursorName()).toBe('getForecast')

  // From the search box, ↑ and ↓ move through what the filter leaves.
  await user.click(screen.getByLabelText('Filter tools'))
  await user.keyboard('repos')
  // A provider with no match leaves the table, so the last stop is a tool.
  await user.keyboard('{ArrowUp}')
  expect(cursorName()).toBe('repos.get')
  expect(screen.getByLabelText('Filter tools')).toHaveValue('repos')

  // A policy toggle keeps its own arrow keys.
  await filterTools('')
  await user.click(within(rowOf('repos.get')).getByRole('radio', { name: 'Ask' }))
  await user.keyboard('{ArrowDown}')
  expect(cursorName()).toBe('repos.get')
})

// Catches: Space or ← and → acting on a row other than the cursor's, or ←
// and → taking the caret keys of the search box.
test('keys act on the cursor tool', async () => {
  const sent: string[] = []
  renderPage({
    action: async (request) => {
      const data = await request.formData()
      sent.push(`${String(data.get('selectedTool'))} ${String(data.get('access'))}`)
      return { error: null, message: 'ok', tools: [] }
    },
  })
  await user.keyboard('{ArrowDown>4/}')
  await user.keyboard(' ')
  expect(checkbox('Select repos.get')).toBeChecked()
  expect(checkbox('Select issues.list')).not.toBeChecked()

  await user.keyboard('{ArrowLeft}')
  await settle()
  expect(sent).toEqual([
    `${JSON.stringify({ provider: github.id, tool: github.tools[1]!.key })} enabled`,
  ])

  await user.click(screen.getByLabelText('Filter tools'))
  await user.keyboard('{ArrowRight}')
  await settle()
  expect(sent).toHaveLength(1)
})

// Catches: a cursor left by a closed drawer that a click elsewhere cannot clear.
test('closing the drawer keeps the cursor until a click elsewhere', async () => {
  renderPage()
  await user.keyboard('{ArrowDown>3/}{Enter}')
  await settle()
  await user.keyboard('{Escape}')
  await settle()
  expect(cursorName()).toBe('issues.list')
  // A click in the cursor's own row keeps it.
  await user.click(checkbox('Select issues.list'))
  expect(cursorName()).toBe('issues.list')
  await user.click(screen.getByRole('heading', { name: 'Tools' }))
  expect(cursorName()).toBeNull()
})

// Catches: an arrow key pressed while the closing drawer still animates out
// moving the drawer to the next tool and opening it again.
test('an arrow key right after Escape moves the cursor, not the drawer', async () => {
  const { router } = renderPage()
  await user.keyboard('{ArrowDown>3/}{Enter}')
  await settle()
  await user.keyboard('{Escape}{ArrowDown}')
  await settle()
  expect(router.state.location.pathname).toBe('/workspaces/demo/tools')
  expect(screen.queryByRole('dialog')).toBeNull()
  expect(cursorName()).toBe('repos.get')
})

// Catches: a drawer closed with the pointer losing the row it showed.
test('closing the drawer with the pointer keeps its row as the cursor', async () => {
  const { router } = renderPage()
  await user.click(within(rowOf('issues.list')).getByRole('link', { name: 'issues.list' }))
  await settle()
  // The drawer moves on to the next tool.
  await user.keyboard('{ArrowDown}')
  await settle()
  await user.click(screen.getByRole('button', { name: 'Close' }))
  await settle()
  expect(router.state.location.pathname).toBe('/workspaces/demo/tools')
  expect(cursorName()).toBe('repos.get')
  expect(cursorRow()).toHaveFocus()
  await user.keyboard('{ArrowDown}')
  expect(cursorName()).toBe('Weather')
})

// Catches: Enter not opening the cursor's tool, or the open drawer
// trapping ↑ and ↓ instead of moving to the next tool, or stopping on a
// heading it cannot show.
test('Enter opens the cursor tool and arrows move the open drawer', async () => {
  const { router } = renderPage()
  await user.keyboard('{ArrowDown>3/}{Enter}')
  await settle()
  expect(router.state.location.pathname).toBe(pathOf(github.tools[0]!))
  await user.keyboard('{ArrowDown}')
  await settle()
  expect(router.state.location.pathname).toBe(pathOf(github.tools[1]!))
})

// Catches: a heading the keys cannot collapse or expand, or a key on a
// heading that acts on a tool instead.
test('keys collapse and expand the cursor heading', async () => {
  renderPage()
  await user.keyboard('{ArrowDown}')
  expect(cursorName()).toBe('GitHub')
  await user.keyboard(' ')
  expect(rowNames()).toEqual(['getForecast'])
  await user.keyboard('{ArrowRight}')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  // → on an open heading leaves it open.
  await user.keyboard('{ArrowRight}')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  await user.keyboard('{ArrowLeft}')
  expect(rowNames()).toEqual(['getForecast'])
  await user.keyboard('{Enter}')
  expect(rowNames()).toEqual(['issues.list', 'repos.get', 'getForecast'])
  expect(cursorName()).toBe('GitHub')
})

// Catches: the cursor moving while focus stays on the control the reader
// last clicked, so Enter and Space act on that control instead.
test('the cursor row takes focus, so Enter and Space reach it', async () => {
  const { router } = renderPage()
  await user.click(screen.getByRole('link', { name: /All tools/ }))
  await user.keyboard('{ArrowDown>3/}')
  expect(cursorRow()).toHaveFocus()
  await user.keyboard(' ')
  expect(checkbox('Select issues.list')).toBeChecked()
  await user.keyboard('{Enter}')
  await settle()
  expect(router.state.location.pathname).toBe(pathOf(github.tools[0]!))
})

// Catches: Enter on a focused button opening the cursor's drawer instead of
// activating the button.
test('Enter on a focused button activates it, not the cursor tool', async () => {
  const { router } = renderPage()
  await user.keyboard('{ArrowDown}')
  within(table()).getByRole('button', { name: 'Collapse all' }).focus()
  await user.keyboard('{Enter}')
  expect(rowNames()).toEqual([])
  expect(router.state.location.pathname).not.toMatch(/\/tools\//)
})

// Catches: the focused table, which scrolls and so takes a tabindex, keeping
// Space and ← and → as if it were a control in a row.
test('keys act on the cursor tool while the table has focus', async () => {
  const sent: string[] = []
  renderPage({
    action: async (request) => {
      const data = await request.formData()
      sent.push(String(data.get('access')))
      return { error: null, message: 'ok', tools: [] }
    },
  })
  table().focus()
  await user.keyboard('{ArrowDown>3/} ')
  expect(checkbox('Select issues.list')).toBeChecked()
  await user.keyboard('{ArrowLeft}')
  await settle()
  expect(sent).toEqual(['enabled'])
})

const disabledRadio = () => within(rowOf('only')).getByRole('radio', { name: 'Block' })

// Catches: a saved policy that a catalog no longer reloading would snap back
// on, or a provider whose last tool went disabled keeping its old status.
test('a saved policy stays, and its blockers apply, without a catalog reload', async () => {
  const solo = provider('00000000-0000-4000-8000-000000000003', 'Solo', 'solo', ['only'])
  const { rerenderProviders } = renderPage({
    providers: [solo],
    action: async (request) => {
      const data = await request.formData()
      const target = JSON.parse(data.get('selectedTool') as string) as {
        provider: string
        tool: string
      }
      return {
        error: null,
        message: 'Tool policy saved for 1 tool.',
        credential: null,
        tools: [{ ...target, status: 'updated' }],
        blockers: { [solo.id]: ['No tools ready'] },
      }
    },
  })
  await choosePolicy('only', 'Block')
  await settle()
  expect(disabledRadio()).toBeChecked()
  expect(screen.getByRole('img', { name: 'No tools ready' })).toBeInTheDocument()

  // A reload is Coral's own word, which takes over from the saved change.
  await rerenderProviders([{ ...solo, tools: [...solo.tools] }])
  expect(disabledRadio()).not.toBeChecked()
  expect(screen.queryByRole('img', { name: 'No tools ready' })).not.toBeInTheDocument()
})

// Catches: a rejected change undoing the saved one, which no reload would restore.
test('a rejected policy change falls back to the saved policy, not the loaded one', async () => {
  const solo = provider('00000000-0000-4000-8000-000000000004', 'Solo', 'solo', ['only'])
  renderPage({
    providers: [solo],
    action: async (request) => {
      const data = await request.formData()
      const target = JSON.parse(data.get('selectedTool') as string) as object
      return data.get('access') === 'disabled'
        ? {
            error: null,
            message: 'Saved.',
            credential: null,
            tools: [{ ...target, status: 'updated' }],
            blockers: {},
          }
        : { error: 'Coral refused.', credential: null }
    },
  })
  await choosePolicy('only', 'Block')
  await settle()
  await choosePolicy('only', 'Allow')
  await settle()
  expect(disabledRadio()).toBeChecked()
  expect(rowOf('only')).toHaveTextContent('Not saved')
})

// Catches: an older response overwriting the blockers of a later change, which
// no reload would correct.
test('blockers from an older policy change do not replace a later one', async () => {
  const pair = provider('00000000-0000-4000-8000-000000000005', 'Pair', 'pair', ['one', 'two'])
  const answers = new Map<string, (blockers: string[]) => void>()
  renderPage({
    providers: [pair],
    action: async (request) => {
      const data = await request.formData()
      const target = JSON.parse(data.get('selectedTool') as string) as { tool: string }
      const blockers = await new Promise<string[]>((resolve) => {
        answers.set(target.tool.split('/').at(-1)!, resolve)
      })
      return {
        error: null,
        message: 'Saved.',
        credential: null,
        tools: [{ ...target, status: 'updated' }],
        blockers: { [pair.id]: blockers },
      }
    },
  })
  await choosePolicy('one', 'Block')
  await settle()
  await choosePolicy('two', 'Block')
  await settle()
  answers.get('two')!(['No tools ready'])
  await settle()
  answers.get('one')!([])
  await settle()
  expect(screen.getByRole('img', { name: 'No tools ready' })).toBeInTheDocument()
})
