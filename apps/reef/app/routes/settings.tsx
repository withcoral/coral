import { useEffect, useMemo, useState } from 'react'

import {
  coralDesktopApi,
  desktopErrorMessage,
  isCoralDesktopBuild,
  type McpClientDescriptor,
  type McpClientId,
} from '@/lib/coral-desktop'
import { Button, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { addToast } from '@/wax/components/toast'

import * as styles from './settings.css'

type PendingAction = { clientId: McpClientId; kind: 'connect' } | null

type ClientStatus = Partial<Record<McpClientId, string>>
type PendingKind = 'connect'

const CLIENT_ICONS = {
  'claude-code': 'Bot',
  codex: 'Terminal',
} satisfies Record<McpClientId, IconName>

function isPending(pending: PendingAction, kind: PendingKind, clientId?: McpClientId) {
  if (!pending || pending.kind !== kind) return false
  return 'clientId' in pending && pending.clientId === clientId
}

export default function SettingsRoute() {
  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <header className={styles.header}>
          <Typography.HeadingLarge as="h1">Settings</Typography.HeadingLarge>
        </header>

        {isCoralDesktopBuild() && <McpClientsSettings />}
      </div>
    </main>
  )
}

function McpClientsSettings() {
  const desktop = useMemo(() => coralDesktopApi(), [])
  const [clients, setClients] = useState<McpClientDescriptor[]>([])
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

  return (
    <section className={styles.section}>
      <header className={styles.header}>
        <Typography.HeadingXSmall as="h2">MCP Clients</Typography.HeadingXSmall>
        <Typography.Body variant="secondary">
          Configure Coral as an MCP server for supported clients on this device.
        </Typography.Body>
      </header>

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
                <div className={styles.cardActions}>
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
              </div>
            </article>
          )
        })}
      </div>
    </section>
  )
}
