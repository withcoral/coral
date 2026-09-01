import type { Route } from './settings/+types/mcp-clients'

import {
  coralDesktopApi,
  desktopErrorMessage,
  isCoralDesktopBuild,
  type McpClientDescriptor,
} from '@/lib/coral-desktop'
import { remoteMcpClientInstructions, webMcpClients } from '@/lib/mcp-clients'
import { mcpConnectionFromEnv } from '@/lib/mcp-connection'
import { isWindowsRequest } from '@/lib/mcp-platform'
import type { McpClientInstallItem, McpInstall } from '@/components/mcp-clients-list'
import { addToast } from '@/wax/components/toast'

interface WebSettingsLoaderData {
  readonly runtime: 'web'
  readonly mcpClients: readonly McpClientInstallItem[]
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

export function loader({ request }: Route.LoaderArgs): WebSettingsLoaderData {
  const connection = mcpConnectionFromEnv()
  const install: McpInstall =
    connection.mode === 'remote'
      ? { transport: 'http', url: connection.url }
      : { shell: isWindowsRequest(request) ? 'powershell' : 'posix', transport: 'stdio' }

  return {
    runtime: 'web',
    mcpClients: webMcpClients.map((client) => {
      const instructions = remoteMcpClientInstructions[client.id]
      return install.transport === 'http' && instructions
        ? { ...client, setupInstructions: `${instructions} ${install.url}` }
        : { ...client, install }
    }),
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
