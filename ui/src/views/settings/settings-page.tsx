import { useCallback, useEffect, useMemo, useState } from 'react'

import {
  configureDesktopMcpClient,
  installDesktopCliAlias,
  isDesktopBridgeLikelyAvailable,
  listDesktopMcpClients,
  testDesktopMcpClient,
  type DesktopCliInstallResult,
  type DesktopMcpClientDescriptor,
  type DesktopMcpClientId,
  type DesktopMcpConfigureResult,
  type DesktopMcpTestResult,
} from '@/lib/desktop-bridge'
import {
  Container as Button,
  Icon as ButtonIcon,
  Text as ButtonText,
} from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import * as styles from './settings-page.css'

type ActionStatus =
  | { state: 'idle' }
  | { detail: string; state: 'done' }
  | { detail: string; state: 'error' }
  | { state: 'running' }

const CLIENT_RANK: Partial<Record<DesktopMcpClientId, number>> = {
  'claude-desktop': 0,
  codex: 1,
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function resultPath(
  result: DesktopCliInstallResult | DesktopMcpConfigureResult | DesktopMcpTestResult,
): string {
  return 'commandPath' in result
    ? (result.shellConfigPath ?? result.commandPath)
    : 'configPath' in result
      ? result.configPath
      : result.launchUrl
}

function statusText(status: ActionStatus | undefined) {
  if (!status || status.state === 'idle' || status.state === 'running') return null
  return (
    <Typography.CodeSmallInline
      as="p"
      className={status.state === 'done' ? styles.success : styles.error}
      variant={status.state === 'done' ? 'secondary' : 'error'}
    >
      {status.detail}
    </Typography.CodeSmallInline>
  )
}

function sortClients(clients: DesktopMcpClientDescriptor[]) {
  return clients.toSorted((a, b) => {
    const rankA = CLIENT_RANK[a.id] ?? 10
    const rankB = CLIENT_RANK[b.id] ?? 10
    return rankA - rankB || a.name.localeCompare(b.name)
  })
}

export function SettingsPage() {
  const [desktopAvailable, setDesktopAvailable] = useState(isDesktopBridgeLikelyAvailable)
  const [clients, setClients] = useState<DesktopMcpClientDescriptor[] | null>(null)
  const [clientsError, setClientsError] = useState<string | null>(null)
  const [cliStatus, setCliStatus] = useState<ActionStatus>({ state: 'idle' })
  const [mcpStatuses, setMcpStatuses] = useState<Partial<Record<DesktopMcpClientId, ActionStatus>>>(
    {},
  )
  const [mcpTestStatuses, setMcpTestStatuses] = useState<
    Partial<Record<DesktopMcpClientId, ActionStatus>>
  >({})

  useEffect(() => {
    if (!desktopAvailable) return

    let cancelled = false
    listDesktopMcpClients()
      .then((nextClients) => {
        if (cancelled) return
        setClients(sortClients(nextClients))
        setClientsError(null)
      })
      .catch((error) => {
        if (cancelled) return
        setClientsError(errorMessage(error))
        setDesktopAvailable(false)
      })

    return () => {
      cancelled = true
    }
  }, [desktopAvailable])

  const sortedClients = useMemo(() => (clients ? sortClients(clients) : []), [clients])

  const installCli = useCallback(async () => {
    setCliStatus({ state: 'running' })
    try {
      const result = await installDesktopCliAlias()
      const detail =
        result.installKind === 'alias'
          ? `Alias added to ${resultPath(result)}`
          : `Installed at ${resultPath(result)}`
      setCliStatus({ detail, state: 'done' })
      addToast('success', { title: 'Coral alias installed', description: detail })
    } catch (error) {
      const detail = errorMessage(error)
      setCliStatus({ detail, state: 'error' })
      addToast('error', { title: 'CLI install failed', description: detail })
    }
  }, [])

  const configureMcp = useCallback(async (client: DesktopMcpClientDescriptor) => {
    setMcpStatuses((current) => ({ ...current, [client.id]: { state: 'running' } }))
    try {
      const result = await configureDesktopMcpClient(client.id)
      const detail = `Configured ${result.client.name} at ${resultPath(result)}`
      setMcpStatuses((current) => ({ ...current, [client.id]: { detail, state: 'done' } }))
      addToast('success', {
        title: `${result.client.name} connected`,
        description: result.configPath,
      })
    } catch (error) {
      const detail = errorMessage(error)
      setMcpStatuses((current) => ({ ...current, [client.id]: { detail, state: 'error' } }))
      addToast('error', { title: `${client.name} connection failed`, description: detail })
    }
  }, [])

  const testMcp = useCallback(async (client: DesktopMcpClientDescriptor) => {
    setMcpTestStatuses((current) => ({ ...current, [client.id]: { state: 'running' } }))
    try {
      const result = await testDesktopMcpClient(client.id)
      setMcpTestStatuses((current) => ({
        ...current,
        [client.id]: { detail: result.message, state: 'done' },
      }))
      addToast('success', {
        title: `${result.client.name} test opened`,
        description: result.message,
      })
    } catch (error) {
      const detail = errorMessage(error)
      setMcpTestStatuses((current) => ({ ...current, [client.id]: { detail, state: 'error' } }))
      addToast('error', { title: `${client.name} test failed`, description: detail })
    }
  }, [])

  if (!desktopAvailable) {
    return (
      <div className={styles.root}>
        <div className={styles.container}>
          <div className={styles.header}>
            <Typography.HeadingLarge as="h1">Settings</Typography.HeadingLarge>
          </div>
          <div className={styles.unavailable}>
            <Icon name="Settings" size="24" color="tertiary" />
            <Typography.Body variant="secondary">
              Desktop integrations are available in Coral Desktop.
            </Typography.Body>
            {clientsError ? (
              <Typography.CodeSmallInline as="p" className={styles.error} variant="error">
                {clientsError}
              </Typography.CodeSmallInline>
            ) : null}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className={styles.root}>
      <div className={styles.container}>
        <div className={styles.header}>
          <Typography.HeadingLarge as="h1">Settings</Typography.HeadingLarge>
          <Typography.Body variant="secondary">
            Local command and agent connections.
          </Typography.Body>
        </div>

        <section className={styles.section} aria-labelledby="settings-cli-title">
          <div className={styles.sectionHeader}>
            <Typography.HeadingSmall as="h2" id="settings-cli-title">
              Command line
            </Typography.HeadingSmall>
          </div>
          <div className={styles.row}>
            <div className={styles.rowMain}>
              <div className={styles.inlineTitle}>
                <Typography.BodyStrong as="span" variant="primary">
                  <code className={styles.code}>coral</code>
                </Typography.BodyStrong>
              </div>
              {statusText(cliStatus) ?? (
                <Typography.CodeSmallInline as="p" className={styles.meta} variant="tertiary">
                  App-bundled CLI alias
                </Typography.CodeSmallInline>
              )}
            </div>
            <div className={styles.actionSlot}>
              <Button
                disabled={cliStatus.state === 'running'}
                onClick={() => void installCli()}
                size="32"
                variant={cliStatus.state === 'done' ? 'secondary' : 'primary'}
              >
                <ButtonIcon name={cliStatus.state === 'running' ? 'Loader' : 'Plus'} />
                <ButtonText>
                  {cliStatus.state === 'running' ? 'Installing' : 'Install alias'}
                </ButtonText>
              </Button>
            </div>
          </div>
        </section>

        <section className={styles.section} aria-labelledby="settings-mcp-title">
          <div className={styles.sectionHeader}>
            <Typography.HeadingSmall as="h2" id="settings-mcp-title">
              Agent connections
            </Typography.HeadingSmall>
          </div>

          {clients === null ? (
            <div className={styles.row}>
              <div className={styles.rowMain}>
                <div className={styles.inlineTitle}>
                  <Icon name="Loader" size="16" color="tertiary" className={styles.spinAnimation} />
                  <Typography.BodyStrong as="span" variant="secondary">
                    Loading clients
                  </Typography.BodyStrong>
                </div>
              </div>
            </div>
          ) : null}

          {sortedClients.map((client) => {
            const status = mcpStatuses[client.id] ?? { state: 'idle' }
            const testStatus = mcpTestStatuses[client.id] ?? { state: 'idle' }
            const visibleStatus = testStatus.state === 'idle' ? status : testStatus
            const canTest = client.id === 'claude-desktop'
            return (
              <div className={styles.row} key={client.id}>
                <div className={styles.rowMain}>
                  <div className={styles.inlineTitle}>
                    <Typography.BodyStrong as="span" variant="primary">
                      {client.name}
                    </Typography.BodyStrong>
                  </div>
                  {statusText(visibleStatus) ?? (
                    <Typography.CodeSmallInline as="p" className={styles.meta} variant="tertiary">
                      {client.configPath}
                    </Typography.CodeSmallInline>
                  )}
                </div>
                <div className={styles.actionSlot}>
                  <Button
                    disabled={status.state === 'running'}
                    onClick={() => void configureMcp(client)}
                    size="32"
                    variant={status.state === 'done' ? 'secondary' : 'primary'}
                  >
                    <ButtonIcon name={status.state === 'running' ? 'Loader' : 'Plus'} />
                    <ButtonText>
                      {status.state === 'running' ? 'Connecting' : 'Connect MCP'}
                    </ButtonText>
                  </Button>
                  {canTest ? (
                    <Button
                      disabled={testStatus.state === 'running'}
                      onClick={() => void testMcp(client)}
                      size="32"
                      variant="secondary"
                    >
                      <ButtonIcon
                        name={testStatus.state === 'running' ? 'Loader' : 'ExternalLink'}
                      />
                      <ButtonText>{testStatus.state === 'running' ? 'Opening' : 'Test'}</ButtonText>
                    </Button>
                  ) : null}
                </div>
              </div>
            )
          })}
        </section>
      </div>
    </div>
  )
}
