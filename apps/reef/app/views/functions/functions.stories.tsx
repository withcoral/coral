import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useLocation, useParams } from 'react-router'
import { expect, screen, userEvent, waitFor, within } from 'storybook/test'
import { FunctionDrawer, FunctionDrawerError } from './function-drawer'
import { dismissAllToasts, ToastContainer } from '@/wax/components/toast'
import { FunctionsPage } from './functions-page'
import type { SavedFunctionView } from './types'

const workspace = {
  name: 'workspaces/demo',
  displayName: 'Operations team',
  state: 'active',
  etag: 'workspace-etag',
  effectiveRole: 'member',
  currentUser: 'users/jordan',
  profile: { user: 'users/jordan', name: 'Jordan Lee', email: 'jordan@example.com', local: false },
  mcpEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  connectEndpoint: 'https://mcp.example.com/workspaces/demo/mcp',
  path: '/workspaces/demo',
}
const summary = (path: string, description: string, savedAt = '2026-09-24T14:05:11.000Z') => ({
  path,
  description,
  savedAt,
  href: `/workspaces/demo/functions/${path}`,
})
const summaries = [
  summary(
    'finance.monthlyTotals',
    'Sum paid invoices for each calendar month. Input: { year: number }. Returns [{ month, total }].',
  ),
  summary('finance.overdueInvoices', 'List unpaid invoices past their due date, oldest first.'),
  summary(
    'linear.completedSince',
    'List Linear issues completed in the last N days, grouped by project, for standups and weekly updates. Optional input: { days?: number (default 7), assignee?: string, team?: string }.',
    '2026-09-25T15:28:00.000Z',
  ),
  summary('linear.issueBrief', 'Build a compact brief of one Linear issue with its discussion.'),
  summary('linear.myOpenIssues', 'List open Linear issues assigned to the current user.'),
  summary(
    'linear.staleIssues',
    'Find open Linear issues that nobody has updated in a number of days.',
  ),
  summary('weeklyDigest', 'Combine finance and Linear summaries into one weekly digest.'),
  summary(
    'reports.quarterly.revenueByRegionAndProductLineIncludingRefundsAndCreditNotesForTheBoard',
    'A long path that truncates.',
  ),
]
const savedFunction: SavedFunctionView = {
  path: 'finance.monthlyTotals',
  description: 'Sum paid invoices for each calendar month.',
  declaration: `interface Input {
    year: number;
}
interface MonthlyTotal {
    month: string;
    total: number;
}
declare const _default: (input: Input) => Promise<MonthlyTotal[]>;
export default _default;
`,
  source: `interface Input { year: number }
interface MonthlyTotal { month: string; total: number }

export default async function monthlyTotals(input: Input): Promise<MonthlyTotal[]> {
  const invoices = await coral.billing.listInvoices({ year: input.year, status: 'paid' })
  const totals = new Map<string, number>()
  for (const invoice of invoices.items) {
    const month = invoice.paidAt.slice(0, 7)
    totals.set(month, (totals.get(month) ?? 0) + invoice.amount)
  }
  return [...totals].map(([month, total]) => ({ month, total }))
}
`,
  diagnostics: [],
  diagnosticsTruncated: false,
  savedAt: '2026-09-24T14:05:11.000Z',
}
const diagnostics = [
  {
    category: 'error' as const,
    code: 2339,
    message: "Property 'paidAt' does not exist on type 'Invoice'.",
    line: 8,
    column: 27,
  },
  {
    category: 'warning' as const,
    code: 6133,
    message: "'input' is declared but its value is never read.",
    line: 4,
    column: null,
  },
  {
    category: 'suggestion' as const,
    code: 80006,
    message: 'This may be converted to an async function.',
    line: null,
    column: null,
  },
]

const meta = {
  title: 'Functions/Saved functions',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story, context) => {
      // The page reads its tab from the route, so the stub matches the real ones.
      const Stub = createRoutesStub([
        {
          path: '/workspaces/:workspace/functions',
          Component: Story,
          children: [{ path: ':path' }, { path: 'namespaces/:namespace' }],
        },
      ])
      return (
        <Stub initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo/functions']} />
      )
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

const listPath = '/workspaces/demo/functions'
const form = { csrf: 'csrf', requestId: 'request' }
const detail = (f: SavedFunctionView, canDelete = true) => (
  <FunctionsPage workspace={workspace} functions={summaries}>
    <FunctionDrawer listPath={listPath} savedFunction={f} form={form} canDelete={canDelete} />
  </FunctionsPage>
)

export const Listing: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={summaries} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A row names the function inside its namespace, and opens its drawer.
    await expect(canvas.getByRole('link', { name: 'issueBrief' })).toHaveAttribute(
      'href',
      '/workspaces/demo/functions/linear.issueBrief',
    )
    // Coral runs every function until function policy exists.
    const policy = within(canvas.getByRole('radiogroup', { name: 'Policy for linear.issueBrief' }))
    await expect(policy.getByRole('radio', { name: 'Always allow' })).toBeChecked()
    for (const radio of policy.getAllByRole('radio')) await expect(radio).toBeDisabled()
  },
}
export const Filtered: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={summaries} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // Names match; descriptions do not, so "compact brief" leaves issueBrief out.
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter functions' }), 'comp')
    await expect(canvas.getByRole('link', { name: 'completedSince' })).toBeVisible()
    await expect(canvas.queryByRole('link', { name: 'issueBrief' })).toBeNull()
    // The rail filter narrows the rail only; All namespaces stays.
    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter namespaces' }), 'fin')
    await expect(canvas.getByRole('link', { name: 'completedSince' })).toBeVisible()
    await expect(
      within(canvas.getByRole('navigation', { name: 'function namespaces' }))
        .getAllByRole('link')
        .map((link) => link.textContent),
    ).toEqual(['All namespaces8', 'finance2'])
    // A namespace is not part of the name a row shows, so it matches no row.
    const filter = canvas.getByRole('searchbox', { name: 'Filter functions' })
    await userEvent.clear(filter)
    await userEvent.type(filter, 'finance')
    await expect(canvas.getByText('No functions match.')).toBeVisible()
  },
}
export const NamespaceTabs: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={twoTabs} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // All namespaces lists a top-level function outside every namespace group.
    const other = canvas.getByRole('rowgroup', { name: 'other' })
    await expect(within(other).getByRole('link', { name: 'imported' })).toBeVisible()
    await expect(within(other).queryByRole('link', { name: 'weeklyDigest' })).toBeNull()
    await expect(canvas.getByRole('link', { name: 'weeklyDigest' })).toBeVisible()
    // A namespace's tab shows only that namespace.
    const rail = canvas.getByRole('navigation', { name: 'function namespaces' })
    await userEvent.click(within(rail).getByRole('link', { name: /^other/ }))
    await expect(canvas.queryByRole('link', { name: 'weeklyDigest' })).toBeNull()
    await expect(canvas.getByRole('link', { name: 'imported' })).toBeVisible()
    await userEvent.click(within(rail).getByRole('link', { name: /^All namespaces/ }))
    await expect(canvas.getByRole('link', { name: 'weeklyDigest' })).toBeVisible()
  },
}
const twoTabs = [
  summary('other.imported', 'A function in the other namespace.'),
  summary('weeklyDigest', 'A function without a namespace.'),
]
const railLink = (canvas: ReturnType<typeof within>, name: RegExp) =>
  within(canvas.getByRole('navigation', { name: 'function namespaces' })).getByRole('link', {
    name,
  })
export const NamespaceAddress: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={twoTabs} />,
  parameters: { initialEntry: '/workspaces/demo/functions/namespaces/other' },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A namespace's address opens its tab, so a shared link keeps it.
    await expect(railLink(canvas, /^other/)).toHaveAttribute('aria-current', 'page')
    await expect(railLink(canvas, /^other/)).toHaveAttribute(
      'href',
      '/workspaces/demo/functions/namespaces/other',
    )
    await expect(canvas.getByRole('link', { name: 'imported' })).toBeVisible()
    await expect(canvas.queryByRole('link', { name: 'weeklyDigest' })).toBeNull()
  },
}
export const MissingNamespace: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={twoTabs} />,
  parameters: { initialEntry: '/workspaces/demo/functions/namespaces/gone' },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A namespace with no functions left falls back to All.
    await expect(railLink(canvas, /^All namespaces/)).toHaveAttribute('aria-current', 'page')
    await expect(canvas.getByRole('link', { name: 'weeklyDigest' })).toBeVisible()
  },
}
export const CollapseAll: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={summaries} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    // A namespace's toggle hides its rows and keeps its heading.
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse linear' }))
    await expect(canvas.queryByRole('link', { name: 'issueBrief' })).toBeNull()
    await expect(canvas.getByRole('button', { name: 'Expand linear' })).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'monthlyTotals' })).toBeVisible()
    // The header toggle collapses every namespace, then expands them all.
    await userEvent.click(canvas.getByRole('button', { name: 'Collapse all' }))
    await expect(canvas.queryByRole('link', { name: 'monthlyTotals' })).toBeNull()
    await expect(canvas.getByRole('link', { name: 'weeklyDigest' })).toBeVisible()
    await userEvent.click(canvas.getByRole('button', { name: 'Expand all' }))
    await expect(canvas.getByRole('link', { name: 'monthlyTotals' })).toBeVisible()
    await expect(canvas.getByRole('link', { name: 'issueBrief' })).toBeVisible()
  },
}
export const Deleted: Story = {
  render: () => (
    <>
      <FunctionsPage workspace={workspace} functions={summaries} />
      <ToastContainer />
    </>
  ),
  parameters: { initialEntry: '/workspaces/demo/functions?functionDeleted=1' },
  beforeEach: () => {
    dismissAllToasts()
    return () => dismissAllToasts()
  },
  play: async () => {
    // A delete returns to the list, which says it worked. The toast bounces in from zero opacity.
    await waitFor(() => expect(screen.getByText('Function deleted')).toBeVisible())
  },
}
export const Empty: Story = {
  render: () => <FunctionsPage workspace={workspace} functions={[]} />,
}
export const Detail: Story = {
  render: () => detail(savedFunction),
  parameters: { initialEntry: '/workspaces/demo/functions/finance.monthlyTotals' },
  play: async ({ canvasElement }) => {
    await expect(await screen.findByRole('dialog', { name: 'finance.monthlyTotals' })).toBeVisible()
    // A function's address opens the drawer over its namespace's tab. The open
    // drawer hides the page from the accessibility tree.
    await expect(
      within(canvasElement)
        .getAllByRole('link', { current: 'page', hidden: true })
        .map((link) => link.textContent),
    ).toEqual(['finance2'])
    // Reef reads and deletes Saved Functions, but cannot author or run them.
    await expect(screen.queryByRole('button', { name: /save|replace|run/i })).toBeNull()
    // Delete asks first, and names what breaks.
    await userEvent.click(screen.getByRole('button', { name: 'Delete function' }))
    const confirm = await screen.findByRole('dialog', { name: 'Delete function' })
    await expect(confirm).toHaveAccessibleDescription(
      'After you delete it, programs that call coral.functions.finance.monthlyTotals fail. You cannot undo this.',
    )
    await userEvent.click(within(confirm).getByRole('button', { name: 'Cancel' }))
    await expect(screen.queryByRole('dialog', { name: 'Delete function' })).toBeNull()
  },
}
// The drawer follows the route, as the function route renders it. The story
// shows the address, so a play can wait for the close to navigate.
function RoutedDrawer() {
  const { path } = useParams()
  const address = <output aria-label="Address">{useLocation().pathname}</output>
  if (path !== savedFunction.path) return address
  return (
    <>
      {address}
      <FunctionDrawer listPath={listPath} savedFunction={savedFunction} form={form} canDelete />
    </>
  )
}
export const CloseToTab: Story = {
  render: () => (
    <FunctionsPage workspace={workspace} functions={summaries}>
      <RoutedDrawer />
    </FunctionsPage>
  ),
  parameters: { initialEntry: '/workspaces/demo/functions/namespaces/finance' },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const current = () =>
      canvas.getAllByRole('link', { current: 'page' }).map((link) => link.textContent)
    // Closing a drawer returns to the tab it opened over: a namespace, then All.
    for (const [tab, address] of [
      [/^finance/, `${listPath}/namespaces/finance`],
      [/^All namespaces/, listPath],
    ] as const) {
      await userEvent.click(railLink(canvas, tab))
      await userEvent.click(canvas.getByRole('link', { name: 'monthlyTotals' }))
      const drawer = await screen.findByRole('dialog', { name: 'finance.monthlyTotals' })
      await userEvent.click(within(drawer).getByRole('button', { name: 'Close' }))
      await waitFor(() =>
        expect(canvas.getByRole('status', { name: 'Address' }).textContent).toBe(address),
      )
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
      await expect(current()).toEqual([expect.stringMatching(tab)])
    }
  },
}
export const ArchivedWorkspace: Story = {
  render: () => detail(savedFunction, false),
  parameters: { initialEntry: '/workspaces/demo/functions/finance.monthlyTotals' },
  play: async () => {
    await expect(await screen.findByRole('dialog', { name: 'finance.monthlyTotals' })).toBeVisible()
    // An archived workspace accepts no changes, so delete is absent.
    await expect(screen.queryByRole('button', { name: 'Delete function' })).toBeNull()
  },
}
export const Diagnostics: Story = {
  render: () => detail({ ...savedFunction, diagnostics }),
  parameters: { initialEntry: '/workspaces/demo/functions/finance.monthlyTotals' },
}
export const TruncatedDiagnostics: Story = {
  render: () => detail({ ...savedFunction, diagnostics, diagnosticsTruncated: true }),
  parameters: { initialEntry: '/workspaces/demo/functions/finance.monthlyTotals' },
  play: async () => {
    await expect(
      await screen.findByText(`Showing the first ${diagnostics.length} diagnostics only.`),
    ).toBeVisible()
  },
}
export const Missing: Story = {
  render: () => (
    <FunctionsPage workspace={workspace} functions={summaries}>
      <FunctionDrawerError status={404} listPath={listPath} />
    </FunctionsPage>
  ),
  parameters: { initialEntry: '/workspaces/demo/functions/finance.deleted' },
}
