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
      readonly workspaceInstallShell?: 'posix' | 'powershell'
    })
  | (McpClientInstallListItemBase & { readonly setupInstructions: string })

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
    <div className={styles.tableContainer}>
      <Table.Wrapper>
        <Table.Root className={styles.table}>
          <Table.Head>
            <Table.Row>
              <Table.HeaderCell className={styles.clientColumn}>MCP client</Table.HeaderCell>
              {hasWorkspaceSelection ? (
                <Table.HeaderCell className={styles.workspaceColumn}>Workspace</Table.HeaderCell>
              ) : null}
              <Table.HeaderCell className={styles.installColumn}>Install</Table.HeaderCell>
            </Table.Row>
          </Table.Head>
          <Table.Body>
            {clients.map((client) => {
              const workspace = selectedWorkspaces[client.id]
              const canSelectWorkspace =
                'workspaceInstallCommand' in client && client.workspaceInstallCommand
              const installCommand =
                'workspaceInstallCommand' in client && client.workspaceInstallCommand && workspace
                  ? `${client.workspaceInstallCommand} --args ${quoteShell(`--workspace=${workspace}`, client.workspaceInstallShell)}`
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
                  <Table.Cell className={installCommand ? undefined : styles.setupCell}>
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
        </Table.Root>
      </Table.Wrapper>
    </div>
  )
}

function quoteShell(value: string, shell: 'posix' | 'powershell' = 'posix'): string {
  return shell === 'powershell'
    ? `'${value.replaceAll("'", "''")}'`
    : `'${value.replaceAll("'", "'\"'\"'")}'`
}
