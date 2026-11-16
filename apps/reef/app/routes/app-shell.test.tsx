// @vitest-environment happy-dom

import { render, screen, within } from '@testing-library/react'
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router'
import { expect, onTestFinished, test, vi } from 'vitest'
import { PageLayout } from '@/wax/components/page-layout'
import AppShell from './app-shell'

// The shell's live counts need a server; these tests are about its navigation.
vi.mock('@/lib/activity-stream', () => ({ subscribeActivityStream: () => ({ close() {} }) }))

const ops = {
  path: '/workspaces/ops',
  displayName: 'Operations',
  effectiveRole: 'owner',
  mcpEndpoint: 'https://coral.example.com/mcp/ops',
  etag: 'etag',
  state: 'active',
}
const shell = {
  actionCounts: null,
  organizationOwner: true,
  newerRelease: undefined,
  auth: { mode: 'disabled' as const },
  userName: undefined,
  userEmail: undefined,
  workspaces: [ops],
  form: { csrf: 'test-csrf', requestId: 'test-request' },
}

const props = { loaderData: shell } as unknown as Parameters<typeof AppShell>[0]

function Page() {
  return <PageLayout breadcrumbs={[{ id: 'current', type: 'text', text: 'Page' }]}>page</PageLayout>
}
const inWorkspace = { loader: () => ({ workspace: ops }), Component: Page }

/** The shell around the app's two kinds of page, as `routes.ts` nests them. */
function open(entry: string) {
  const router = createMemoryRouter(
    [
      {
        id: 'routes/app-shell',
        Component: () => <AppShell {...props} />,
        children: [
          { path: 'workspaces/:workspace/tools', ...inWorkspace },
          {
            // The settings layout marks its pages, whatever their path.
            handle: { area: 'settings' },
            Component: Outlet,
            children: [
              { path: 'organization/workspaces', Component: Page },
              { path: 'usage-data', Component: Page },
            ],
          },
        ],
      },
    ],
    { initialEntries: [entry] },
  )
  onTestFinished(() => router.dispose())
  render(<RouterProvider router={router} />)
}

const sidebar = () => within(screen.getByRole('navigation', { name: 'Coral' }))
// Catches: a page of the organization's settings showing the workspace's
// switcher and pages, or a work page showing the settings nav. The route's place
// under the settings layout decides, not its path.
test.each([
  ['/workspaces/ops/tools', false],
  ['/organization/workspaces', true],
  ['/usage-data', true],
])('at %s the sidebar shows the settings nav: %s', async (entry, settings) => {
  open(entry)
  await screen.findByText('page')
  expect(sidebar().queryByRole('link', { name: 'Back to workspace' }) !== null).toBe(settings)
  expect(sidebar().queryByRole('button', { name: /^Workspace/ }) === null).toBe(settings)
})
