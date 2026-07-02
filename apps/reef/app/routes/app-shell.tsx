import { Outlet, useRouteLoaderData } from 'react-router'

import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { ToastContainer } from '@/wax/components/toast'

import * as styles from './app-shell.css'

interface RootLoaderData {
  sidebarIsMinimized: boolean
}

export default function AppShell() {
  const rootData = useRouteLoaderData('root') as RootLoaderData | undefined

  return (
    <div className={styles.layout}>
      <Sidebar initialIsMinimized={rootData?.sidebarIsMinimized ?? false} />
      <ContentContainer>
        <ToastContainer />
        <Outlet />
      </ContentContainer>
    </div>
  )
}
