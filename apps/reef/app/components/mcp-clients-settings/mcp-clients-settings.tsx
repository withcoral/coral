import { Banner, Button, Menu, Table, Typography } from '@/wax/components'

import * as styles from './mcp-clients-settings.css'

const NOT_CONFIGURED = 'not-configured'
const WORKSPACE_ACCESS_PREFIX = 'workspace:'

export interface McpClientSettingsItem {
  readonly configuredWorkspace?: string
  readonly id: string
  readonly name: string
}

export interface McpClientsSettingsProps {
  readonly clients: ReadonlyArray<McpClientSettingsItem>
  readonly error?: string
  readonly loading?: boolean
  readonly onWorkspaceChange: (clientId: string, workspaceName?: string) => void
  readonly pendingClientIds?: ReadonlyArray<string>
  readonly workspaces: ReadonlyArray<{ name: string }>
}

export function McpClientsSettings({
  clients,
  error,
  loading = false,
  onWorkspaceChange,
  pendingClientIds = [],
  workspaces,
}: McpClientsSettingsProps) {
  const status = loading ? (
    <Typography.BodySmall role="status" variant="tertiary">
      Loading MCP clients…
    </Typography.BodySmall>
  ) : error ? (
    <Typography.BodySmall role="alert" variant="error">
      {error}
    </Typography.BodySmall>
  ) : clients.length === 0 ? (
    <Typography.BodySmall variant="tertiary">
      No supported MCP clients available.
    </Typography.BodySmall>
  ) : null

  return (
    <section className={styles.section}>
      <header className={styles.header}>
        <Typography.HeadingLarge as="h2">MCP Clients</Typography.HeadingLarge>
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
        This page shows only global MCP configurations. Project-specific and other connections will
        not appear here.
      </Banner>

      <div className={styles.tableContainer}>
        <Table.Wrapper>
          <Table.Root className={styles.table}>
            <Table.Head>
              <Table.Row>
                <Table.HeaderCell>MCP client</Table.HeaderCell>
                <Table.HeaderCell className={styles.workspaceColumn}>Workspace</Table.HeaderCell>
              </Table.Row>
            </Table.Head>
            <Table.Body>
              {status ? (
                <Table.Row>
                  <td className={styles.statusCell} colSpan={2}>
                    {status}
                  </td>
                </Table.Row>
              ) : (
                clients.map((client) => {
                  const pending = pendingClientIds.includes(client.id)
                  const access =
                    client.configuredWorkspace === undefined
                      ? NOT_CONFIGURED
                      : workspaceAccessValue(client.configuredWorkspace)
                  const accessLabel =
                    client.configuredWorkspace === undefined
                      ? 'Not configured'
                      : client.configuredWorkspace

                  return (
                    <Table.Row key={client.id}>
                      <Table.Cell>
                        <Typography.BodyStrong variant="primary">
                          {client.name}
                        </Typography.BodyStrong>
                      </Table.Cell>
                      <Table.Cell>
                        <Menu.Container>
                          <Menu.Trigger
                            className={styles.workspaceTrigger}
                            render={
                              <Button.Container disabled={pending} fullWidth variant="secondary" />
                            }
                          >
                            <Button.Text>{accessLabel}</Button.Text>
                            <Button.Icon name="ChevronDown" />
                          </Menu.Trigger>
                          <Menu.Content align="end" className={styles.workspaceMenu}>
                            <Menu.RadioGroup
                              onValueChange={(value) => {
                                onWorkspaceChange(
                                  client.id,
                                  value === NOT_CONFIGURED
                                    ? undefined
                                    : value.slice(WORKSPACE_ACCESS_PREFIX.length),
                                )
                              }}
                              value={access}
                            >
                              <Menu.RadioItem value={NOT_CONFIGURED}>Not configured</Menu.RadioItem>
                              {workspaces.map((workspace) => (
                                <Menu.RadioItem
                                  key={workspace.name}
                                  value={workspaceAccessValue(workspace.name)}
                                >
                                  {workspace.name}
                                </Menu.RadioItem>
                              ))}
                            </Menu.RadioGroup>
                          </Menu.Content>
                        </Menu.Container>
                      </Table.Cell>
                    </Table.Row>
                  )
                })
              )}
            </Table.Body>
          </Table.Root>
        </Table.Wrapper>
      </div>
    </section>
  )
}

function workspaceAccessValue(workspaceName: string): string {
  return `${WORKSPACE_ACCESS_PREFIX}${workspaceName}`
}
