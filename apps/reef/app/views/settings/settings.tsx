import { McpClientsList } from '@/components/mcp-clients-list'
import type { DesktopSettingsLoaderData } from '@/routes/settings-loader'
import { Banner, Button, Typography } from '@/wax/components'

import * as styles from './settings.css'

export function Settings({
  loaderData,
  onWorkspaceChange,
  pendingClientIds,
  workspaces,
}: {
  readonly loaderData: DesktopSettingsLoaderData
  readonly onWorkspaceChange: (clientId: string, workspaceName?: string) => void
  readonly pendingClientIds: ReadonlyArray<string>
  readonly workspaces: ReadonlyArray<{ name: string }>
}) {
  return (
    <main className={styles.page}>
      <div className={styles.container}>
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

          <McpClientsList
            clients={loaderData.mcpClients.clients}
            error={loaderData.mcpClients.error}
            onWorkspaceChange={onWorkspaceChange}
            pendingClientIds={pendingClientIds}
            workspaces={workspaces}
          />
        </section>
      </div>
    </main>
  )
}

export function SettingsHydrateFallback() {
  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <section className={styles.section}>
          <Typography.BodySmall role="status" variant="tertiary">
            Loading MCP clients…
          </Typography.BodySmall>
        </section>
      </div>
    </main>
  )
}
