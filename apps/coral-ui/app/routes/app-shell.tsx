import { Outlet, useRouteLoaderData } from 'react-router'

import type { Route } from './+types/app-shell'

import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { requestAuthContext } from '@/auth/server-context'
import { listWorkspacesForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'
import { ToastContainer } from '@/wax/components/toast'
import type { BrowserAuth, RequestAuth } from '@/auth/types'

import * as styles from './app-shell.css'

interface RootLoaderData {
  sidebarIsMinimized: boolean
}

export async function loader({ context, request }: Route.LoaderArgs) {
  const auth = browserAuth(context.get(requestAuthContext))
  if (isWorkspaceRedirectRoute(request)) return { auth, workspaces: [] }

  try {
    return {
      auth,
      workspaces: await listWorkspacesForRequest(
        request,
        context.get(requestAuthContext).accessToken,
      ),
    }
  } catch (error) {
    if (error instanceof Response) throw error
    console.error('Failed to load sidebar workspaces:', error)
    return { auth, workspaces: [] }
  }
}

function browserAuth(auth: RequestAuth): BrowserAuth {
  return auth.mode === 'required'
    ? { csrfToken: auth.csrfToken, mode: 'required' }
    : { mode: 'disabled' }
}

function isWorkspaceRedirectRoute(request: Request): boolean {
  return new URL(request.url).pathname === routePath('home')
}

export default function AppShell({ loaderData }: Route.ComponentProps) {
  const rootData = useRouteLoaderData('root') as RootLoaderData | undefined

  return (
    <div className={styles.layout}>
      <Sidebar
        auth={loaderData.auth}
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
