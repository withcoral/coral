import { useCallback, useRef, useState } from 'react'

import { McpClientInstallList, McpClientsList } from '@/components/mcp-clients-list'
import type { SettingsLoaderData } from '@/routes/settings-loader'
import { Banner, Button, Typography } from '@/wax/components'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'

import { SettingsPage } from './settings-page'
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
  const [search, setSearch] = useState('')
  const searchInputRef = useRef<HTMLInputElement>(null)

  const onSearchShortcut = useCallback((event: KeyboardEvent) => {
    const input = searchInputRef.current
    if (!input) return

    event.preventDefault()
    input.focus()
    input.select()
  }, [])

  return (
    <SettingsPage
      header={
        <>
          <div className={styles.headerText}>
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
          </div>

          <div className={styles.searchBar}>
            <TextInput
              ariaLabel="Search MCP clients"
              icon="Search"
              onChange={setSearch}
              placeholder="Search MCP clients"
              ref={searchInputRef}
              type="search"
              value={search}
            />
          </div>
        </>
      }
    >
      <KeyboardShortcut handler={onSearchShortcut} shortcut="$mod+f" />

      <Banner>
        {desktop
          ? 'This page shows only global MCP configurations. Project-specific and other connections will not appear here.'
          : 'These settings affect only global MCP configurations. Project-specific and other connections will not be replaced.'}
      </Banner>

      {desktop ? (
        <McpClientsList
          clients={loaderData.mcpClients.clients}
          error={loaderData.mcpClients.error}
          onWorkspaceChange={onWorkspaceChange}
          pendingClientIds={pendingClientIds}
          search={search}
          workspaces={workspaces}
        />
      ) : (
        <McpClientInstallList
          clients={loaderData.mcpClients}
          search={search}
          workspaces={workspaces}
        />
      )}
    </SettingsPage>
  )
}

export function SettingsHydrateFallback() {
  return (
    <SettingsPage header={<Typography.HeadingLarge as="h1">MCP Clients</Typography.HeadingLarge>}>
      <Typography.BodySmall role="status" variant="tertiary">
        Loading MCP clients…
      </Typography.BodySmall>
    </SettingsPage>
  )
}
