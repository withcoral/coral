import { useEffect, useMemo, useState } from 'react'

import {
  coralDesktopApi,
  desktopErrorMessage,
  type McpClientDescriptor,
  type McpClientId,
} from '@/lib/coral-desktop'
import { Button, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { addToast } from '@/wax/components/toast'

import * as styles from './settings.css'

type PendingAction =
  | { clientId: McpClientId; kind: 'connect' }
  | { clientId: McpClientId; kind: 'test' }
  | null

type ClientStatus = Partial<Record<McpClientId, string>>
type PendingKind = 'connect' | 'test'

const CLIENT_ICONS = {
  'claude-desktop': 'Bot',
  codex: 'Terminal',
  cursor: 'MousePointer2',
  opencode: 'Braces',
  vscode: 'Code',
} satisfies Record<McpClientId, IconName>

function isPending(pending: PendingAction, kind: PendingKind, clientId?: McpClientId) {
  if (!pending || pending.kind !== kind) return false
  return 'clientId' in pending && pending.clientId === clientId
}

export default function SettingsRoute() {
  const desktop = useMemo(() => coralDesktopApi(), [])
  const [clients, setClients] = useState<McpClientDescriptor[]>([])
  const [clientStatus, setClientStatus] = useState<ClientStatus>({})
  const [loadError, setLoadError] = useState<string | null>(null)
  const [pending, setPending] = useState<PendingAction>(null)
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

    return () => {
      cancelled = true
    }
  }, [desktop])

  async function handleConnectMcp(client: McpClientDescriptor) {
    if (!desktop) return

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
    if (!desktop) return

    setPending({ clientId: client.id, kind: 'test' })
    try {
      const result = await desktop.testMcp(client.id)
      addToast('success', {
        description: result.message,
        title: `${client.name} test opened`,
      })
    } catch (error) {
      addToast('error', {
        description: desktopErrorMessage(error),
        title: `${client.name} test failed`,
      })
    } finally {
      setPending(null)
    }
  }

  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <header className={styles.header}>
          <Typography.HeadingLarge as="h1">Settings</Typography.HeadingLarge>
        </header>

        <section className={styles.section}>
          <div className={styles.sectionHead}>
            <Typography.HeadingXSmall as="h2">MCP clients</Typography.HeadingXSmall>
            <span className={styles.sectionCount}>{clients.length}</span>
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
              const canTest = client.id === 'claude-desktop'
              const connectPending = isPending(pending, 'connect', client.id)
              const testPending = isPending(pending, 'test', client.id)

              return (
                <article className={styles.clientCard} key={client.id}>
                  <div className={styles.cardHeader}>
                    <div className={styles.clientLogo}>
                      <Icon color="tertiary" name={CLIENT_ICONS[client.id]} size="18" />
                    </div>
                    <Typography.BodyLargeStrong className={styles.cardTitle} truncate>
                      {client.name}
                    </Typography.BodyLargeStrong>
                  </div>

                  <Typography.CodeSmallInline className={styles.path} title={configPath}>
                    {configPath}
                  </Typography.CodeSmallInline>

                  <div className={styles.cardFooter}>
                    {connectedPath && (
                      <Pill className={styles.connectedPill} color="green">
                        Connected
                      </Pill>
                    )}

                    <div className={styles.cardActions}>
                      {canTest && (
                        <Button.Container
                          disabled={!isDesktopAvailable || testPending}
                          onClick={() => handleTestMcp(client)}
                          size="32"
                          variant="bare"
                        >
                          <Button.Icon name="Play" />
                          <Button.Text>{testPending ? 'Opening' : 'Test'}</Button.Text>
                        </Button.Container>
                      )}

                      <Button.Container
                        disabled={!isDesktopAvailable || connectPending}
                        onClick={() => handleConnectMcp(client)}
                        size="32"
                        variant="secondary"
                      >
                        <Button.Icon name="Link" />
                        <Button.Text>{connectPending ? 'Connecting' : 'Connect'}</Button.Text>
                      </Button.Container>
                    </div>
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
