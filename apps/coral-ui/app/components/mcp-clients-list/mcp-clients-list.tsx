import { Table, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'

import { filterMcpClients } from './filter-mcp-clients'
import * as styles from './mcp-clients-list.css'

// A workspace name is lowercase letters, digits and inner hyphens, so a
// leading colon cannot collide with one. Option values are then the bare
// names, and a configured workspace the list no longer carries still reads as
// itself rather than as a raw sentinel.
const NOT_CONFIGURED = ':not-configured'

export interface McpClientListItem {
  readonly configuredWorkspace?: string
  readonly id: string
  readonly name: string
}

export interface McpClientsListProps {
  readonly clients: ReadonlyArray<McpClientListItem>
  readonly error?: string
  readonly loading?: boolean
  /**
   * Scrolls the rows under the sticky header once they exceed this height, in pixels.
   * Leave it unset to let the surrounding page scroll instead.
   */
  readonly maxHeight?: number
  readonly onWorkspaceChange: (clientId: string, workspaceName?: string) => void
  readonly pendingClientIds?: ReadonlyArray<string>
  readonly search?: string
  readonly workspaces: ReadonlyArray<{ name: string }>
}

export type McpClientsConnectionState = Pick<
  McpClientsListProps,
  'clients' | 'error' | 'loading' | 'onWorkspaceChange' | 'pendingClientIds'
>

// The workspace column narrows on mobile, so its width comes from a property the
// stylesheet sets rather than from a number here.
const CLIENT_COLUMNS: Table.Column[] = [
  { label: 'MCP client', width: 'fill' },
  { label: 'Workspace', width: `var(${styles.WORKSPACE_WIDTH_PROPERTY})` },
]

export function McpClientsList({
  clients,
  error,
  loading = false,
  maxHeight,
  onWorkspaceChange,
  pendingClientIds = [],
  search = '',
  workspaces,
}: McpClientsListProps) {
  const visibleClients = filterMcpClients(clients, search)
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
  ) : visibleClients.length === 0 ? (
    <Typography.BodySmall variant="tertiary">No results for "{search}"</Typography.BodySmall>
  ) : null

  return (
    <Table.Container
      className={styles.responsiveWidths}
      columns={CLIENT_COLUMNS}
      layout="fixed"
      maxHeight={maxHeight}
      variant="card"
    >
      <Table.Head />
      <Table.Body>
        {status ? (
          <Table.Status>{status}</Table.Status>
        ) : (
          visibleClients.map((client) => {
            const pending = pendingClientIds.includes(client.id)
            const access = client.configuredWorkspace ?? NOT_CONFIGURED
            return (
              <Table.Row key={client.id}>
                <Table.Cell>
                  <Typography.BodyStrong variant="primary">{client.name}</Typography.BodyStrong>
                </Table.Cell>
                <Table.Cell>
                  <Picker
                    disabled={pending}
                    fullWidth
                    onChange={(value) => {
                      onWorkspaceChange(client.id, value === NOT_CONFIGURED ? undefined : value)
                    }}
                    options={[
                      { label: 'Not configured', value: NOT_CONFIGURED },
                      ...workspaces.map(({ name }) => ({ label: name, value: name })),
                    ]}
                    value={access}
                  />
                </Table.Cell>
              </Table.Row>
            )
          })
        )}
      </Table.Body>
    </Table.Container>
  )
}
