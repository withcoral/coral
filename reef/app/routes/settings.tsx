import { useEffect, useMemo, useState } from 'react'

import {
  coralDesktopApi,
  desktopErrorMessage,
  type McpClientDescriptor,
  type McpClientId,
} from '@/lib/coral-desktop'
import { Button, Typography } from '@/wax/components'
import { addToast } from '@/wax/components/toast'

import * as styles from './settings.css'

type PendingAction =
  | { clientId: McpClientId; kind: 'connect' }
  | { clientId: McpClientId; kind: 'test' }
  | null

type ClientStatus = Partial<Record<McpClientId, string>>
type PendingKind = 'connect' | 'test'

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
      <header className={styles.header}>
        <Typography.HeadingMedium as="h1">Settings</Typography.HeadingMedium>
      </header>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Typography.HeadingXSmall as="h2">MCP clients</Typography.HeadingXSmall>
        </div>

        {loadError && (
          <Typography.BodySmall className={styles.status} variant="error">
            {loadError}
          </Typography.BodySmall>
        )}

        <div className={styles.clientList}>
          {clients.map((client) => {
            const connectedPath = clientStatus[client.id]
            const canTest = client.id === 'claude-desktop'
            const connectPending = isPending(pending, 'connect', client.id)
            const testPending = isPending(pending, 'test', client.id)

            return (
              <div className={styles.clientRow} key={client.id}>
                <div className={styles.rowContent}>
                  <Typography.BodyStrong>{client.name}</Typography.BodyStrong>
                  <Typography.CodeSmallInline className={styles.path}>
                    {connectedPath ?? client.configPath}
                  </Typography.CodeSmallInline>
                </div>

                <div className={styles.rowActions}>
                  {canTest && (
                    <Button.Container
                      disabled={!isDesktopAvailable || testPending}
                      onClick={() => handleTestMcp(client)}
                      variant="bare"
                    >
                      <Button.Icon name="Play" />
                      <Button.Text>{testPending ? 'Opening' : 'Test'}</Button.Text>
                    </Button.Container>
                  )}
                  <Button.Container
                    disabled={!isDesktopAvailable || connectPending}
                    onClick={() => handleConnectMcp(client)}
                    variant="primary"
                  >
                    <Button.Icon name="Link" />
                    <Button.Text>{connectPending ? 'Connecting' : 'Connect'}</Button.Text>
                  </Button.Container>
                </div>
              </div>
            )
          })}
        </div>
      </section>
    </main>
  )
}
