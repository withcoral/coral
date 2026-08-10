import { Button, Table, Typography } from '@/wax/components'

import * as styles from './mcp-clients-list.css'

export interface McpClientInstallListItem {
  readonly id: string
  readonly installCommand: string
  readonly name: string
}

export function McpClientInstallList({
  clients,
}: {
  readonly clients: readonly McpClientInstallListItem[]
}) {
  return (
    <div className={styles.tableContainer}>
      <Table.Wrapper>
        <Table.Root className={styles.table}>
          <Table.Head>
            <Table.Row>
              <Table.HeaderCell>MCP client</Table.HeaderCell>
              <Table.HeaderCell className={styles.installColumn}>Install</Table.HeaderCell>
            </Table.Row>
          </Table.Head>
          <Table.Body>
            {clients.map((client) => (
              <Table.Row key={client.id}>
                <Table.Cell>
                  <Typography.BodyStrong variant="primary">{client.name}</Typography.BodyStrong>
                </Table.Cell>
                <Table.Cell>
                  <div className={styles.installCommand}>
                    <code>{client.installCommand}</code>
                    <Button.CopyButton
                      ariaLabel={`Copy the Coral install command for ${client.name}`}
                      textToCopy={client.installCommand}
                    >
                      Copy
                    </Button.CopyButton>
                  </div>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      </Table.Wrapper>
    </div>
  )
}
