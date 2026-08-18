import { useState } from 'react'

import { Button, Menu, Table, Typography } from '@/wax/components'

import * as styles from './mcp-clients-list.css'

interface McpClientInstallListItemBase {
  readonly id: string
  readonly name: string
}

export type McpClientInstallListItem =
  | (McpClientInstallListItemBase & {
      readonly installCommand: string
      readonly workspaceInstallCommand?: string
    })
  | (McpClientInstallListItemBase & { readonly setupInstructions: string })

// One list, so a column that goes unrendered cannot leave a track behind.
function installColumns(hasWorkspaceSelection: boolean): Table.Column[] {
  return [
    { label: 'MCP client', width: `var(${styles.CLIENT_WIDTH_PROPERTY})` },
    ...(hasWorkspaceSelection
      ? [{ label: 'Workspace', width: `var(${styles.WORKSPACE_WIDTH_PROPERTY})` } as Table.Column]
      : []),
    { label: 'Install', width: 'fill' },
  ]
}

export function McpClientInstallList({
  clients,
  workspaces = [],
}: {
  readonly clients: readonly McpClientInstallListItem[]
  readonly workspaces?: ReadonlyArray<{ name: string }>
}) {
  const [selectedWorkspaces, setSelectedWorkspaces] = useState<Readonly<Record<string, string>>>({})
  const hasWorkspaceSelection = clients.length > 0

  return (
    <Table.Container
      className={styles.responsiveWidths}
      columns={installColumns(hasWorkspaceSelection)}
      layout="fixed"
      variant="card"
    >
      <Table.Head />
      <Table.Body>
        {clients.map((client) => {
          const workspace = selectedWorkspaces[client.id]
          const canSelectWorkspace =
            'workspaceInstallCommand' in client && client.workspaceInstallCommand
          const installCommand =
            'workspaceInstallCommand' in client && client.workspaceInstallCommand && workspace
              ? `${client.workspaceInstallCommand} --args ${quotePosix(`--workspace=${workspace}`)}`
              : 'installCommand' in client
                ? client.installCommand
                : undefined

          return (
            <Table.Row key={client.id}>
              <Table.Cell>
                <Typography.BodyStrong variant="primary">{client.name}</Typography.BodyStrong>
              </Table.Cell>
              {hasWorkspaceSelection ? (
                <Table.Cell>
                  <Menu.Container>
                    <Menu.Trigger
                      className={styles.workspaceTrigger}
                      render={<Button.Container fullWidth variant="secondary" />}
                    >
                      <Button.Text>{workspace ?? 'Default workspace'}</Button.Text>
                      <Button.Icon name="ChevronDown" />
                    </Menu.Trigger>
                    <Menu.Content align="end" className={styles.workspaceMenu}>
                      <Menu.RadioGroup
                        onValueChange={(value) => {
                          if (!canSelectWorkspace) return
                          setSelectedWorkspaces((current) => {
                            const next = { ...current }
                            if (value === '') delete next[client.id]
                            else next[client.id] = value
                            return next
                          })
                        }}
                        value={workspace ?? ''}
                      >
                        <Menu.RadioItem value="">Default workspace</Menu.RadioItem>
                        {workspaces.map((entry) => (
                          <Menu.RadioItem
                            disabled={!canSelectWorkspace}
                            key={entry.name}
                            value={entry.name}
                          >
                            {entry.name}
                          </Menu.RadioItem>
                        ))}
                      </Menu.RadioGroup>
                    </Menu.Content>
                  </Menu.Container>
                </Table.Cell>
              ) : null}
              {/* Manual remote-client setup carries the endpoint and every
                      step, so it wraps instead of truncating. */}
              <Table.Cell wrap={!installCommand}>
                {installCommand ? (
                  <div className={styles.installCommand}>
                    <code>{installCommand}</code>
                    <Button.CopyButton
                      ariaLabel={`Copy the Coral install command for ${client.name}`}
                      className={styles.copyButton}
                      textToCopy={installCommand}
                      variant="bare"
                    />
                  </div>
                ) : (
                  <Typography.BodySmall variant="secondary">
                    {'setupInstructions' in client ? client.setupInstructions : null}
                  </Typography.BodySmall>
                )}
              </Table.Cell>
            </Table.Row>
          )
        })}
      </Table.Body>
    </Table.Container>
  )
}

function quotePosix(value: string): string {
  return `'${value.replaceAll("'", "'\"'\"'")}'`
}
