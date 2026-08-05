import { useEffect, useRef, useState } from 'react'

import { coralDesktopApi, desktopErrorMessage, type McpClientDescriptor } from '@/lib/coral-desktop'
import { addToast } from '@/wax/components/toast'

import type { McpClientsListProps } from './mcp-clients-list'

export type DesktopMcpClientsState = Pick<
  McpClientsListProps,
  'clients' | 'error' | 'loading' | 'onWorkspaceChange' | 'pendingClientIds'
>

export function useDesktopMcpClients(enabled: boolean): DesktopMcpClientsState {
  const [clients, setClients] = useState<McpClientDescriptor[]>([])
  const [loadError, setLoadError] = useState<string>()
  const [loading, setLoading] = useState(true)
  const [pendingClientIds, setPendingClientIds] = useState<string[]>([])
  const pendingClientIdsRef = useRef(new Set<string>())

  useEffect(() => {
    if (!enabled) return

    const desktop = coralDesktopApi()
    if (!desktop) {
      setLoadError('Desktop bridge unavailable.')
      setLoading(false)
      return
    }

    let cancelled = false
    desktop
      .listMcpClients()
      .then((nextClients) => {
        if (!cancelled) setClients(nextClients)
      })
      .catch((reason: unknown) => {
        if (!cancelled) setLoadError(desktopErrorMessage(reason))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [enabled])

  async function handleWorkspaceChange(clientId: string, workspace?: string) {
    const client = clients.find(({ id }) => id === clientId)
    const desktop = coralDesktopApi()
    if (
      !client ||
      !desktop ||
      workspace === client.configuredWorkspace ||
      pendingClientIdsRef.current.has(clientId)
    ) {
      return
    }

    pendingClientIdsRef.current.add(clientId)
    setPendingClientIds((current) => [...current, clientId])
    try {
      if (workspace === undefined) {
        await desktop.removeMcp(client.id)
      } else {
        await desktop.configureMcp(client.id, workspace)
      }

      setClients((current) =>
        current.map((item) =>
          item.id === client.id ? { ...item, configuredWorkspace: workspace } : item,
        ),
      )

      if (workspace === undefined) {
        addToast('success', {
          description: `Restart ${client.name} to apply this change.`,
          title: `Removed Coral from ${client.name}’s config`,
        })
      } else {
        addToast('success', {
          description: `Restart ${client.name} to apply the MCP connection.`,
          title:
            client.configuredWorkspace === undefined
              ? `Added Coral to ${client.name}’s config`
              : `Updated Coral in ${client.name}’s config`,
        })
      }
    } catch (reason) {
      addToast('error', {
        description: desktopErrorMessage(reason),
        title: `Couldn’t update workspace access for ${client.name}`,
      })
    } finally {
      pendingClientIdsRef.current.delete(clientId)
      setPendingClientIds((current) => current.filter((id) => id !== clientId))
    }
  }

  return {
    clients,
    error: loadError,
    loading,
    onWorkspaceChange: (clientId, workspace) => {
      void handleWorkspaceChange(clientId, workspace)
    },
    pendingClientIds,
  }
}
