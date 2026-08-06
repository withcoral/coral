import { Outlet, useRouteLoaderData } from 'react-router'

import type { Route } from './+types/app-shell'

import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import type { WorkspaceMembership } from '@/generated/coral/v1/workspaces_pb'
import { listWorkspacesForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'
import { ToastContainer } from '@/wax/components/toast'

import * as styles from './app-shell.css'

interface RootLoaderData {
  sidebarIsMinimized: boolean
}

interface AppShellLoaderData {
  memberships: WorkspaceMembership[]
}

/**
 * Loads the current user's memberships verbatim. The role travels with each workspace so
 * policy-aware surfaces can read it; consumers derive `membership.workspace` at the point they
 * navigate.
 */
export async function loader({ request }: Route.LoaderArgs): Promise<AppShellLoaderData> {
  if (isWorkspaceRedirectRoute(request)) return { memberships: [] }

  try {
    return { memberships: await listWorkspacesForRequest(request) }
  } catch (error) {
    console.error('Failed to load sidebar workspaces:', error)
    return { memberships: [] }
  }
}

function isWorkspaceRedirectRoute(request: Request): boolean {
  return new URL(request.url).pathname === routePath('home')
}

export default function AppShell({ loaderData }: Route.ComponentProps) {
  const rootData = useRouteLoaderData('root') as RootLoaderData | undefined

  return (
    <div className={styles.layout}>
      <Sidebar
        initialIsMinimized={rootData?.sidebarIsMinimized ?? false}
        workspaces={loaderData.memberships.flatMap(({ workspace }) =>
          workspace ? [workspace] : [],
        )}
      />
      <ContentContainer>
        <ToastContainer />
        <Outlet />
      </ContentContainer>
    </div>
  )
}
