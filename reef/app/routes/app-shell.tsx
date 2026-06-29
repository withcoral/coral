import { Outlet } from 'react-router'
import type { Route } from './+types/app-shell'

import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { readSidebarCollapsedCookie } from '@/components/sidebar/sidebar-state.server'
import { ToastContainer } from '@/wax/components/toast'

import * as styles from './app-shell.css'

export async function loader({ request }: Route.LoaderArgs) {
  return {
    sidebarIsMinimized: readSidebarCollapsedCookie(request),
  }
}

export default function AppShell({ loaderData }: Route.ComponentProps) {
  return (
    <div className={styles.layout}>
      <Sidebar initialIsMinimized={loaderData.sidebarIsMinimized} />
      <ContentContainer>
        <ToastContainer />
        <Outlet />
      </ContentContainer>
    </div>
  )
}
