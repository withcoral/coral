/** @vitest-environment happy-dom */
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import type { AgentView, OwnerView, PersonView, WorkspaceView } from './types'
import { WorkspaceOverview, type ProviderSummary } from './workspace-overview'

const ada: PersonView = { user: 'users/ada', name: 'Ada', email: '', local: false }
const workspace: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: ada.user,
  profile: ada,
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  connectEndpoint: 'https://mcp.example.com/mcp',
  path: '/workspaces/demo',
}
const agents = (count: number) =>
  Array.from({ length: count }, (_, i): AgentView => ({
    name: `workspaces/demo/agents/a${i}`,
    displayName: `Automation ${i + 1}`,
    state: 'enabled',
    etag: `a${i}`,
    createdBy: ada.user,
    creator: ada,
    accessBlockers: [],
    path: `/workspaces/demo/agents/a${i}`,
    settingsPath: `/workspaces/demo/agents/a${i}/settings`,
  }))
const providers = (count: number) =>
  Array.from({ length: count }, (_, i): ProviderSummary => ({
    id: `p${i}`,
    displayName: `Provider ${i + 1}`,
    namespace: `p${i}`,
    path: `/workspaces/demo/providers/p${i}`,
    blockers: [],
  }))

function render(props: {
  workspace?: Partial<WorkspaceView>
  agents?: AgentView[]
  providers?: ProviderSummary[]
  owners?: OwnerView[] | null
}) {
  renderRoute(
    <WorkspaceOverview
      workspace={{ ...workspace, ...props.workspace }}
      agents={props.agents ?? []}
      providers={props.providers ?? []}
      owners={
        props.owners === undefined
          ? [{ user: ada.user, profile: ada, enabled: true }]
          : props.owners
      }
      ownMembership={null}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      recipes={[]}
    />,
    { entry: workspace.path },
  )
}
const button = (name: string) => screen.queryByRole('button', { name })

it.each<[string, Partial<WorkspaceView>, string[], string[]]>([
  ['an Owner of an active workspace', {}, ['Add provider', 'Create agent'], []],
  [
    'an Owner of an archived workspace',
    { state: 'archived' },
    [],
    ['Add provider', 'Create agent'],
  ],
  ['a member', { effectiveRole: 'member' }, [], ['Add provider', 'Create agent']],
])('offers %s only the empty-card actions it can take', (_, change, offered, withheld) => {
  render({ workspace: change })
  for (const name of offered) expect(button(name), name).not.toBeNull()
  for (const name of withheld) expect(button(name), name).toBeNull()
})

it('hides agents from a member and sends them to an owner for providers', () => {
  render({ workspace: { effectiveRole: 'member' }, agents: agents(2) })
  expect(screen.queryByRole('heading', { name: 'Agents' })).toBeNull()
  expect(screen.queryByRole('link', { name: 'Automation 1' })).toBeNull()
  screen.getByText('Ask a workspace owner to add a provider so Coral can talk to its API.')
})

// A list one over the limit shows whole; a longer one shows three and links to the rest.
it.each([
  [4, 4, false],
  [5, 3, true],
])('lists %i providers and agents as %i rows', (count, shown, more) => {
  render({ agents: agents(count), providers: providers(count) })
  for (const [label, link] of [
    ['Provider', 'View all providers'],
    ['Automation', 'View all agents'],
  ]) {
    const rows = screen.getAllByRole('link', { name: new RegExp(`^${label} \\d$`) })
    expect(rows.map((row) => row.textContent)).toEqual(
      Array.from({ length: shown }, (_, i) => `${label} ${i + 1}`),
    )
    // The heading's chevron always links to the full list; the text link only when rows are hidden.
    expect(screen.getAllByRole('link', { name: link })).toHaveLength(more ? 2 : 1)
  }
})

// Each client shows its own setup, built from this workspace's endpoint.
it.each([
  [
    'Claude Code',
    [
      ['Run in a terminal', `claude mcp add --transport http coral ${workspace.connectEndpoint}`],
      ['Then sign in', 'claude mcp login coral'],
    ],
  ],
  [
    'Codex',
    [
      ['Run in a terminal', `codex mcp add coral --url ${workspace.connectEndpoint}`],
      ['Then sign in', 'codex mcp login coral'],
    ],
  ],
  ['Other', [['MCP URL', workspace.connectEndpoint]]],
])('quick start shows the %s setup', async (client, steps) => {
  render({})
  const card = within(screen.getByRole('region', { name: 'Connect your MCP client' }))
  await userEvent.click(card.getByRole('tab', { name: client }))
  for (const [field, value] of steps)
    expect(within(card.getByRole('group', { name: field })).getByText(value)).toBeVisible()
})

// Catches: setup offered for work the workspace cannot start.
it.each<[string, Partial<WorkspaceView>, boolean]>([
  ['in an active workspace', {}, true],
  ['in an archived workspace', { state: 'archived' }, false],
  ['without an MCP endpoint', { connectEndpoint: '' }, false],
])('quick start %s', (_, ws, shown) => {
  render({ workspace: ws })
  expect(screen.queryByRole('region', { name: 'Connect your MCP client' }) !== null).toBe(shown)
})

// Catches: a disabled owner shown like any other, so a member asks them for
// help, or the marker landing on an enabled owner.
it('marks a disabled owner in words on the members card', () => {
  const cy: PersonView = { user: 'users/cy', name: 'Cy', email: '', local: false }
  render({
    owners: [
      { user: ada.user, profile: ada, enabled: true },
      { user: cy.user, profile: cy, enabled: false },
    ],
  })
  const rows = screen.getAllByRole('listitem').filter((row) => /Ada|Cy/.test(row.textContent))
  expect(rows.map((row) => [/Cy/.test(row.textContent), /Disabled/.test(row.textContent)])).toEqual(
    [
      [false, false],
      [true, true],
    ],
  )
})

// Catches: a local deployment, which has no other people, showing a Members
// card that links to a page it does not serve.
it.each([
  ['a signed-in deployment', true, undefined],
  ['a local deployment', false, null],
])('%s shows the Members card: %s', (_, shown, owners) => {
  render({ owners })
  const heading = screen.queryByRole('heading', { name: 'Members' })
  expect(heading !== null).toBe(shown)
})
