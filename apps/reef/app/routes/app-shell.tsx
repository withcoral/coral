import { Outlet, useRouteLoaderData } from 'react-router'

import type { Route } from './+types/app-shell'

import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { listWorkspacesForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'
import { ToastContainer } from '@/wax/components/toast'

import * as styles from './app-shell.css'

interface RootLoaderData {
  sidebarIsMinimized: boolean
}

export async function loader({ request }: Route.LoaderArgs) {
  if (isWorkspaceRedirectRoute(request)) return { workspaces: [] }

  try {
    const memberships = await listWorkspacesForRequest(request)
    return {
      workspaces: memberships.flatMap(({ workspace }) => (workspace ? [workspace] : [])),
    }
  } catch (error) {
    console.error('Failed to load sidebar workspaces:', error)
    return { workspaces: [] }
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
        workspaces={loaderData.workspaces}
      />
      <ContentContainer>
        <ToastContainer />
        <Outlet />
      </ContentContainer>
    </div>
  )
}
