import type { Route } from './+types/settings'

import {
  coralDesktopApi,
  desktopErrorMessage,
  isCoralDesktopBuild,
  type McpClientDescriptor,
} from '@/lib/coral-desktop'
import { addToast } from '@/wax/components/toast'

interface WebSettingsLoaderData {
  readonly runtime: 'web'
}

export interface DesktopSettingsLoaderData {
  readonly mcpClients: DesktopMcpClientData
  readonly runtime: 'desktop'
}

export type SettingsLoaderData = DesktopSettingsLoaderData | WebSettingsLoaderData

export interface DesktopMcpClientData {
  readonly clients: ReadonlyArray<McpClientDescriptor>
  readonly error?: string
}

export function loader(_args: Route.LoaderArgs): WebSettingsLoaderData {
  return { runtime: 'web' }
}
}

export async function clientLoader({
  serverLoader,
}: Route.ClientLoaderArgs): Promise<SettingsLoaderData> {
  if (!isCoralDesktopBuild()) return serverLoader()

  return {
    mcpClients: await loadDesktopMcpClients(),
    runtime: 'desktop',
  }
}

export async function loadDesktopMcpClients(): Promise<DesktopMcpClientData> {
  const desktop = coralDesktopApi()
  if (!desktop) {
    return {
      clients: [],
      error: 'Desktop bridge unavailable.',
    }
  }

  try {
    return { clients: await desktop.listMcpClients() }
  } catch (reason) {
    return {
      clients: [],
      error: desktopErrorMessage(reason),
    }
  }
}

clientLoader.hydrate = true as const

export async function clientAction({ request }: Route.ClientActionArgs) {
  await updateDesktopMcpClient(await request.formData())
  return null
}

export async function updateDesktopMcpClient(formData: FormData): Promise<void> {
  const desktop = coralDesktopApi()
  if (!desktop) throw new Response('Desktop bridge unavailable.', { status: 503 })

  const clientId = formData.get('clientId')
  const workspace = formData.get('workspace')
  if (typeof clientId !== 'string' || typeof workspace !== 'string') {
    throw new Response('Invalid MCP client update.', { status: 400 })
  }

  let clientName = 'MCP client'
  try {
    const client = (await desktop.listMcpClients()).find(({ id }) => id === clientId)
    if (!client || workspace === client.configuredWorkspace) return
    clientName = client.name

    if (workspace === '') {
      await desktop.removeMcp(client.id)
      addToast('success', {
        description: `Restart ${client.name} to apply this change.`,
        title: `Removed Coral from ${client.name}’s config`,
      })
    } else {
      await desktop.configureMcp(client.id, workspace)
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
      title: `Couldn’t update workspace access for ${clientName}`,
    })
  }
}
