import { McpClientInstallList, McpClientsList } from '@/components/mcp-clients-list'
import type { SettingsLoaderData } from '@/routes/settings-loader'
import { Banner, Button, Typography } from '@/wax/components'

import * as styles from './settings.css'

export function Settings({
  loaderData,
  onWorkspaceChange,
  pendingClientIds,
  workspaces,
}: {
  readonly loaderData: SettingsLoaderData
  readonly onWorkspaceChange: (clientId: string, workspaceName?: string) => void
  readonly pendingClientIds: ReadonlyArray<string>
  readonly workspaces: ReadonlyArray<{ name: string }>
}) {
  const desktop = loaderData.runtime === 'desktop'

  return (
    <section className={styles.section}>
      <header className={styles.sectionHeader}>
        <Typography.HeadingLarge as="h1">MCP Clients</Typography.HeadingLarge>
        <Typography.Body variant="secondary">
          {desktop
            ? 'Choose the Coral workspace each MCP client can access. '
            : 'Copy a command to add Coral to a supported MCP client in your user-wide configuration. '}
          <Button.ExternalLink
            href="https://withcoral.com/docs/guides/use-coral-over-mcp"
            size="small"
          >
            Learn more
          </Button.ExternalLink>
        </Typography.Body>
      </header>

      <Banner>
        {desktop
          ? 'This page shows only global MCP configurations. Project-specific and other connections will not appear here.'
          : 'Each command updates one user-global MCP client configuration. It will not replace an existing Coral entry, and project configuration may override it.'}
      </Banner>

      {desktop ? (
        <McpClientsList
          clients={loaderData.mcpClients.clients}
          error={loaderData.mcpClients.error}
          onWorkspaceChange={onWorkspaceChange}
          pendingClientIds={pendingClientIds}
          workspaces={workspaces}
        />
      ) : (
        <McpClientInstallList clients={loaderData.mcpClients} />
      )}
    </section>
  )
}

export function SettingsHydrateFallback() {
  return (
    <section className={styles.section}>
      <Typography.BodySmall role="status" variant="tertiary">
        Loading MCP clients…
      </Typography.BodySmall>
    </section>
  )
}
