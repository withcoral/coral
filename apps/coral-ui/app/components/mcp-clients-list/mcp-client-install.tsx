import { useId, useState } from 'react'

import { Button, Menu, Typography } from '@/wax/components'

import * as styles from './mcp-clients-list.css'

const ADD_MCP = 'npx --yes add-mcp@1.11.0'

interface McpClientInstallItemBase {
  readonly id: string
  readonly name: string
}

export type McpInstall =
  | { readonly transport: 'http'; readonly url: string }
  | { readonly shell: 'posix' | 'powershell'; readonly transport: 'stdio' }

export type McpClientInstallItem = McpClientInstallItemBase &
  ({ readonly install: McpInstall } | { readonly setupInstructions: string })

/**
 * The workspace is a parameter rather than an option because nothing resolves
 * an unscoped install any more: a remote endpoint serves only
 * `/mcp/workspace/<name>`, and `coral mcp-stdio` without `--workspace` takes
 * the caller's sole membership or refuses to choose.
 */
function installCommand(agent: string, install: McpInstall, workspace: string): string {
  const target =
    install.transport === 'http'
      ? workspaceMcpUrl(install.url, workspace)
      : install.shell === 'powershell'
        ? '(Get-Command coral).Source'
        : '"$(command -v coral)"'
  const transport =
    install.transport === 'http'
      ? '--transport http'
      : `--args mcp-stdio --args ${quoteShell(`--workspace=${workspace}`, install.shell)}`
  return `${ADD_MCP} ${target} --global --agent ${agent} --name coral ${transport} --yes`
}

export function McpClientInstall({
  clients,
  workspaces = [],
}: {
  readonly clients: readonly McpClientInstallItem[]
  readonly workspaces?: ReadonlyArray<{ name: string }>
}) {
  const [clientId, setClientId] = useState<string>()
  const [workspaceName, setWorkspaceName] = useState<string>()
  // Resolved against the current lists rather than seeded into state, so a
  // workspace that disappears cannot keep naming itself in a command.
  // `ListWorkspaces` sorts by name, so the first is the sidebar's first too.
  const client = clients.find(({ id }) => id === clientId) ?? clients[0]
  const workspace = workspaces.find(({ name }) => name === workspaceName) ?? workspaces[0]

  if (!client) {
    return (
      <Typography.BodySmall variant="tertiary">
        No supported MCP clients available.
      </Typography.BodySmall>
    )
  }

  const install = 'install' in client ? client.install : undefined
  const command =
    install && workspace ? installCommand(client.id, install, workspace.name) : undefined

  return (
    <div className={styles.installPanel}>
      <div className={styles.installSelects}>
        <InstallSelect
          label="MCP client"
          onChange={setClientId}
          options={clients.map(({ id, name }) => ({ label: name, value: id }))}
          value={client.id}
        />
        {/* Disabled rather than gone, so the panel keeps its shape as the
            reader moves between clients. */}
        {workspace ? (
          <InstallSelect
            disabled={!install}
            label="Workspace"
            onChange={setWorkspaceName}
            options={workspaces.map(({ name }) => ({ label: name, value: name }))}
            value={workspace.name}
          />
        ) : null}
      </div>

      {command ? (
        <div className={styles.installCommandContainer}>
          {install?.transport === 'stdio' && install.shell === 'powershell' ? (
            <Typography.BodySmall variant="secondary">Requires PowerShell</Typography.BodySmall>
          ) : null}
          <div className={styles.installField}>
            <pre className={styles.installCommand}>{command}</pre>
            <Button.CopyButton
              ariaLabel={`Copy the Coral install command for ${client.name}`}
              className={styles.installCopyButton}
              textToCopy={command}
              variant="bare"
            />
          </div>
        </div>
      ) : (
        <Typography.Body as="p" variant="secondary">
          {'setupInstructions' in client
            ? client.setupInstructions
            : 'Create a workspace to install Coral in this client.'}
        </Typography.Body>
      )}
    </div>
  )
}

function InstallSelect({
  disabled = false,
  label,
  onChange,
  options,
  value,
}: {
  readonly disabled?: boolean
  readonly label: string
  readonly onChange: (value: string) => void
  readonly options: ReadonlyArray<{ label: string; value: string }>
  readonly value: string
}) {
  const labelId = useId()

  return (
    <div className={styles.installSelect}>
      <Typography.BodySmallStrong id={labelId} variant="tertiary">
        {label}
      </Typography.BodySmallStrong>
      <Menu.Container>
        <Menu.Trigger
          className={styles.selectTrigger}
          render={
            <Button.Container
              aria-labelledby={labelId}
              disabled={disabled}
              fullWidth
              variant="secondary"
            />
          }
        >
          <Button.Text>
            {options.find((option) => option.value === value)?.label ?? value}
          </Button.Text>
          <Button.Icon name="ChevronDown" />
        </Menu.Trigger>
        <Menu.Content className={styles.selectMenu}>
          <Menu.RadioGroup onValueChange={onChange} value={value}>
            {options.map((option) => (
              <Menu.RadioItem key={option.value} value={option.value}>
                {option.label}
              </Menu.RadioItem>
            ))}
          </Menu.RadioGroup>
        </Menu.Content>
      </Menu.Container>
    </div>
  )
}

function workspaceMcpUrl(remoteMcpUrl: string, workspace: string): string {
  const url = new URL(remoteMcpUrl)
  url.pathname = `${url.pathname.replace(/\/$/, '')}/workspace/${encodeURIComponent(workspace)}`
  return url.toString()
}

function quoteShell(value: string, shell: 'posix' | 'powershell'): string {
  return shell === 'powershell'
    ? `'${value.replaceAll("'", "''")}'`
    : `'${value.replaceAll("'", "'\"'\"'")}'`
}
