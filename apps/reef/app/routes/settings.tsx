import { useRouteLoaderData } from 'react-router'
import type { loader as appShellLoader } from './app-shell'

import { McpClientsList, useDesktopMcpClients } from '@/components/mcp-clients-list'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { Banner, Button, Typography } from '@/wax/components'

import * as styles from './settings.css'

export default function SettingsRoute() {
  const desktop = isCoralDesktopBuild()
  const mcpClients = useDesktopMcpClients(desktop)
  const memberships =
    useRouteLoaderData<typeof appShellLoader>('routes/app-shell')?.memberships ?? []
  // MCP clients are pointed at a workspace, so derive the workspace here and leave the role behind.
  const workspaces = memberships.flatMap(({ workspace }) => (workspace ? [workspace] : []))

  return (
    <main className={styles.page}>
      <div className={styles.container}>
        {desktop && (
          <section className={styles.section}>
            <header className={styles.sectionHeader}>
              <Typography.HeadingLarge as="h1">MCP Clients</Typography.HeadingLarge>
              <Typography.Body variant="secondary">
                Choose the Coral workspace each MCP client can access.{' '}
                <Button.ExternalLink
                  href="https://withcoral.com/docs/guides/use-coral-over-mcp"
                  size="small"
                >
                  Learn more
                </Button.ExternalLink>
              </Typography.Body>
            </header>

            <Banner>
              This page shows only global MCP configurations. Project-specific and other connections
              will not appear here.
            </Banner>

            <McpClientsList {...mcpClients} workspaces={workspaces} />
          </section>
        )}
      </div>
    </main>
  )
}
