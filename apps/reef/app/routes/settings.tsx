import type { Route } from './+types/settings'

import { McpClientsSettings, useDesktopMcpClients } from '@/components/mcp-clients-settings'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { Typography } from '@/wax/components'

import * as styles from './settings.css'

export default function SettingsRoute({ matches }: Pick<Route.ComponentProps, 'matches'>) {
  const desktop = isCoralDesktopBuild()
  const mcpClients = useDesktopMcpClients(desktop)
  const workspaces = matches[1].loaderData.workspaces

  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <Typography.HeadingLarge as="h1">Settings</Typography.HeadingLarge>
        {desktop && <McpClientsSettings {...mcpClients} workspaces={workspaces} />}
      </div>
    </main>
  )
}
