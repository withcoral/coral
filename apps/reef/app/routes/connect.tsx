import { useEffect, useMemo, useState } from 'react'

import {
  coralDesktopApi,
  desktopErrorMessage,
  type McpClientDescriptor,
  type McpClientId,
} from '@/lib/coral-desktop'
import { ProviderLogo } from '@/components/sources/provider-logo'
import { Button, Typography } from '@/wax/components'
import { CopyButton } from '@/wax/components/button/copy-button'
import { addToast } from '@/wax/components/toast'

import * as styles from './connect.css'

type PendingAction = { clientId: McpClientId; kind: 'connect' | 'test' } | null

type ClientStatus = Partial<Record<McpClientId, string>>

function isPending(pending: PendingAction, kind: 'connect' | 'test', clientId: McpClientId) {
  return pending?.kind === kind && pending.clientId === clientId
}

export default function ConnectRoute() {
  const desktop = useMemo(() => coralDesktopApi(), [])
  const [clients, setClients] = useState<McpClientDescriptor[]>([])
  const [addCommand, setAddCommand] = useState<string | null>(null)
  const [clientStatus, setClientStatus] = useState<ClientStatus>({})
  const [loadError, setLoadError] = useState<string | null>(null)
  const [pending, setPending] = useState<PendingAction>(null)
  const hasPendingAction = pending !== null
  const isDesktopAvailable = Boolean(desktop)

  useEffect(() => {
    if (!desktop) {
      setLoadError('Desktop bridge unavailable.')
      return
    }

    let cancelled = false
    desktop
      .listMcpClients()
      .then((nextClients) => {
        if (!cancelled) setClients(nextClients)
      })
      .catch((error: unknown) => {
        if (!cancelled) setLoadError(desktopErrorMessage(error))
      })
    desktop
      .mcpAddCommand()
      .then((command) => {
        if (!cancelled) setAddCommand(command)
      })
      .catch((error: unknown) => {
        if (!cancelled) setLoadError(desktopErrorMessage(error))
      })

    return () => {
      cancelled = true
    }
  }, [desktop])

  async function handleConnectMcp(client: McpClientDescriptor) {
    if (!desktop || hasPendingAction) return

    setPending({ clientId: client.id, kind: 'connect' })
    try {
      const result = await desktop.configureMcp(client.id)
      setClientStatus((current) => ({
        ...current,
        [client.id]: result.configPath,
      }))
      addToast('success', {
        description: result.configPath,
        title: `${client.name} connected`,
      })
    } catch (error) {
      addToast('error', {
        description: desktopErrorMessage(error),
        title: `${client.name} connection failed`,
      })
    } finally {
      setPending(null)
    }
  }

  async function handleTestMcp(client: McpClientDescriptor) {
    if (!desktop || hasPendingAction) return

    setPending({ clientId: client.id, kind: 'test' })
    try {
      await desktop.testMcpClient(client.id)
    } catch (error) {
      addToast('error', {
        description: `${desktopErrorMessage(error)} Make sure the ${client.name} app is installed.`,
        title: `Could not open ${client.name}`,
      })
    } finally {
      setPending(null)
    }
  }

  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <header className={styles.header}>
          <Typography.HeadingLarge as="h1">Connect your agents</Typography.HeadingLarge>
        </header>

        <section className={styles.connectCard}>
          <div className={styles.connectCardHead}>
            <Typography.HeadingXSmall as="h2">Connect an agent</Typography.HeadingXSmall>
            <Typography.BodySmall variant="tertiary">
              Paste this into a terminal to add Coral to Claude Code, Cursor, or any MCP client —
              your agent gets every source you connect here.
            </Typography.BodySmall>
          </div>
          <div className={styles.commandRow}>
            <code className={styles.commandBox}>{addCommand ?? 'Loading command…'}</code>
            <CopyButton
              disabled={!addCommand}
              onCopy={() => addToast('success', { title: 'Command copied' })}
              textToCopy={addCommand ?? ''}
            />
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.sectionHead}>
            <Typography.HeadingXSmall as="h2">One-click setup</Typography.HeadingXSmall>
          </div>

          {loadError && (
            <Typography.BodySmall className={styles.status} variant="error">
              {loadError}
            </Typography.BodySmall>
          )}

          <div className={styles.cardGrid}>
            {clients.map((client) => {
              const connectedPath = clientStatus[client.id]
              const configPath = connectedPath ?? client.configPath
              const connectPending = isPending(pending, 'connect', client.id)

              return (
                <article className={styles.clientCard} key={client.id}>
                  <div className={styles.cardHeader}>
                    <ProviderLogo name={client.id} size="medium" />
                    <Typography.BodyLargeStrong className={styles.cardTitle} truncate>
                      {client.name}
                    </Typography.BodyLargeStrong>
                  </div>

                  <Typography.CodeSmallInline className={styles.path} title={configPath}>
                    {configPath}
                  </Typography.CodeSmallInline>

                  <div className={styles.cardFooter}>
                    {client.testable && (
                      <Button.Container
                        disabled={!isDesktopAvailable || hasPendingAction}
                        onClick={() => handleTestMcp(client)}
                        size="32"
                        variant="bare"
                      >
                        <Button.Icon name="MessageCircle" />
                        <Button.Text>Test connection</Button.Text>
                      </Button.Container>
                    )}
                    <Button.Container
                      disabled={!isDesktopAvailable || hasPendingAction}
                      onClick={() => handleConnectMcp(client)}
                      size="32"
                      variant="secondary"
                    >
                      <Button.Icon name="Link" />
                      <Button.Text>{connectPending ? 'Connecting' : 'Connect'}</Button.Text>
                    </Button.Container>
                  </div>
                </article>
              )
            })}
          </div>
        </section>
      </div>
    </main>
  )
}
