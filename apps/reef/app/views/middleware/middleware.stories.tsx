import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, screen, within } from 'storybook/test'
import { MiddlewarePage } from './middleware-page'
import { MiddlewareSourceDialog } from './middleware-source-dialog'
import type { MiddlewareDetailView, MiddlewareView } from './types'

const workspace = {
  name: 'workspaces/demo',
  displayName: 'Operations team',
  state: 'active',
  etag: 'workspace-etag',
  effectiveRole: 'owner',
  currentUser: 'users/jordan',
  profile: { user: 'users/jordan', name: 'Jordan Lee', email: 'jordan@example.com', local: false },
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  path: '/workspaces/demo',
}
const form = { csrf: 'csrf', requestId: 'request' }
const item = (
  displayName: string,
  position: number,
  enabled: boolean,
  scopes: MiddlewareView['scopes'],
  description: string,
): MiddlewareView => ({
  name: `workspaces/demo/middleware/${displayName}`,
  path: `/workspaces/demo/middleware/${displayName}`,
  displayName,
  description,
  enabled,
  scopes,
  position,
  updatedAt: '2026-10-14T09:12:00.000Z',
})
const chain = [
  item(
    'sentry',
    0,
    true,
    { run: true, function: false, operation: true },
    'Report execution timing and failures to Sentry.',
  ),
  item(
    'jev-review',
    1,
    false,
    { run: false, function: false, operation: true },
    'Ask JEV to review operation inputs and results before they return.',
  ),
  item(
    'operation-count',
    2,
    true,
    { run: true, function: true, operation: true },
    'Count operations per execution and inside or outside function calls.',
  ),
]
const detail: MiddlewareDetailView = {
  ...chain[1],
  source: `export default async function review(
  ctx: CoralMiddleware.Invocation,
  next: CoralMiddleware.Next,
) {
  if (ctx.kind !== 'operation') return next()
  const verdict = await coral.providers.jev.review({ path: ctx.path, input: ctx.args })
  if (verdict.data.allow) ctx.currentPolicy = 'enabled'
  return next()
}
`,
  diagnostics: [
    {
      category: 'warning',
      code: 6133,
      message: "'verdict' is declared but its value is never read.",
      line: 6,
      column: 9,
    },
  ],
}

const meta = {
  title: 'Middleware/Workspace middleware',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub initialEntries={['/workspaces/demo/middleware']} />
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

export const Chain: Story = {
  render: () => <MiddlewarePage workspace={workspace} middleware={chain} canChange form={form} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const table = canvas.getByRole('table', { name: 'Middleware' })
    await expect(within(table).getAllByRole('row')).toHaveLength(4)
    // The ends of the chain cannot move past themselves.
    await expect(canvas.getByRole('button', { name: 'Move sentry up' })).toBeDisabled()
    await expect(canvas.getByRole('button', { name: 'Move sentry down' })).toBeEnabled()
    await expect(canvas.getByRole('button', { name: 'Move operation-count down' })).toBeDisabled()
    // Each scope and activation control has its own accessible name and text state.
    await expect(canvas.getByRole('checkbox', { name: 'Operations for jev-review' })).toBeChecked()
    await expect(
      canvas.getByRole('checkbox', { name: 'Function calls for jev-review' }),
    ).not.toBeChecked()
    await expect(canvas.getByRole('switch', { name: 'Enable jev-review' })).not.toBeChecked()
    await expect(canvas.getByRole('switch', { name: 'Enable sentry' })).toBeChecked()
    await expect(canvas.getByRole('link', { name: 'jev-review' })).toHaveAttribute(
      'href',
      '/workspaces/demo/middleware/jev-review',
    )
  },
}

export const ArchivedWorkspace: Story = {
  render: () => (
    <MiddlewarePage
      workspace={{ ...workspace, state: 'archived' }}
      middleware={chain}
      canChange={false}
      form={form}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    for (const control of [
      ...canvas.getAllByRole('checkbox'),
      canvas.getByRole('button', { name: 'Move jev-review up' }),
    ])
      await expect(control).toBeDisabled()
    // The switch is a span with role switch, so it states its disabled state in ARIA.
    for (const control of canvas.getAllByRole('switch'))
      await expect(control).toHaveAttribute('aria-disabled', 'true')
  },
}

export const Empty: Story = {
  render: () => <MiddlewarePage workspace={workspace} middleware={[]} canChange form={form} />,
  play: async ({ canvasElement }) => {
    await expect(within(canvasElement).getByText('No middleware yet')).toBeVisible()
  },
}

export const Source: Story = {
  render: () => (
    <MiddlewarePage workspace={workspace} middleware={chain} canChange form={form}>
      <MiddlewareSourceDialog listPath="/workspaces/demo/middleware" middleware={detail} />
    </MiddlewarePage>
  ),
  play: async () => {
    const dialog = await screen.findByRole('dialog', { name: 'jev-review' })
    await expect(within(dialog).getByText(/To change the code, use save_middleware/)).toBeVisible()
    await expect(within(dialog).getByText(/Disabled\. It runs in position 2/)).toBeVisible()
    await expect(within(dialog).getByRole('table', { name: 'Diagnostics' })).toBeVisible()
    // The modal shows source; it offers no editor or save control.
    await expect(within(dialog).queryByRole('textbox')).toBeNull()
    await expect(within(dialog).queryByRole('button', { name: /save/i })).toBeNull()
  },
}
