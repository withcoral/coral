import { Code, ConnectError } from '@connectrpc/connect'
import { redirect, type RouterContextProvider } from 'react-router'
import type { Provider } from '@/generated/coral/management/v1/provider_pb'
import { PRIVATE_ICONS } from '@/views/providers/coral-icons'
import type { ProviderView, ToolView, WorkspaceView } from '@/views/providers/types'
import { PRIVATE_TOOL_DELETED, privateToolsPath } from '@/views/tools/addresses'
import type { PrivateToolDetailView } from '@/views/tools/types'
import {
  actionFailure,
  agentName,
  field,
  management,
  privateHeaders,
  resourcePath,
  workspaceName,
  type ManagementApi,
} from './management.server'
import { privateToolView } from './management-views.server'
import { groupView, listAllToolAccess, toolView } from './providers.server'

/**
 * The private group of the Tools page: the Private Provider with the private
 * tools whose policy the reader may read. Coral decides which those are: your
 * own and, for an owner, every agent's. An agent's row names the agent.
 */
export async function privateGroup(
  api: ManagementApi,
  workspace: Pick<WorkspaceView, 'name' | 'path'>,
): Promise<ProviderView> {
  // Coral's provider list leaves the Private Provider out. Every page of the
  // private tool list names it, so the smallest page is enough.
  const { privateProvider: name } = await api.privateTools.listPrivateTools({
    parent: workspace.name,
    pageSize: 1,
  })
  const { provider, rows } = await authoredToolRows(api, name)
  // Only an owner's rows name agents, and only an owner may list the agents.
  const agents = rows.some((row) => row.agent)
    ? await agentLabels(api, workspace.name)
    : new Map<string, string>()
  const tools = rows.map((row) => {
    const id = row.agent.split('/').at(-1)!
    return {
      ...authoredToolRow(toolView(row)),
      ...(row.agent && {
        agent: { id, label: agents.get(row.agent) ?? id, path: resourcePath(row.agent) },
      }),
    }
  })
  return {
    ...authoredGroupView(provider, tools),
    kind: 'private',
    path: privateToolsPath(workspace.path),
    icons: PRIVATE_ICONS,
  }
}

/**
 * The Private or the Shared Provider, which Coral's provider list leaves out,
 * with each of its tools that Coral lists to the reader.
 */
export async function authoredToolRows(api: ManagementApi, name: string) {
  const [provider, rows] = await Promise.all([
    api.providers.getProvider({ name }),
    listAllToolAccess(api, name),
  ])
  return { provider, rows }
}

/** What the private and the shared group have in common: no accounts, and no search state. */
export function authoredGroupView(provider: Provider, tools: ToolView[]) {
  return {
    ...groupView(provider, tools),
    personalAccountAction: null,
    // Search indexes no tool from the Provider's own catalog, which holds none.
    searchIndex: null,
  }
}

/**
 * A private or a shared tool's group is the namespace its path starts with,
 * so its row shows only the name inside that namespace.
 */
export function authoredToolRow(tool: ToolView): ToolView {
  const namespace = `${tool.group}.`
  return tool.group && tool.relativePath.startsWith(namespace)
    ? { ...tool, nameInGroup: tool.relativePath.slice(namespace.length) }
    : tool
}

/** Each agent's display name by its resource name. */
async function agentLabels(api: Pick<ManagementApi, 'agents'>, parent: string) {
  const labels = new Map<string, string>()
  let pageToken = ''
  do {
    const page = await api.agents.listAgents({ parent, pageSize: 100, pageToken })
    for (const agent of page.agents) labels.set(agent.name, agent.displayName)
    pageToken = page.nextPageToken
  } while (pageToken)
  return labels
}

/**
 * Reads the one private tool the drawer shows: the reader's own, or the one
 * of `agent`, an agent's resource name, which Coral shows only to an owner.
 */
export async function privateToolDetail(
  api: Pick<ManagementApi, 'privateTools'>,
  workspace: string,
  path: string,
  agent = '',
): Promise<PrivateToolDetailView> {
  try {
    const tool = await api.privateTools.getPrivateTool({ parent: workspace, path, agent })
    return privateToolView(tool, agent ? agent.split('/').at(-1)! : null)
  } catch (error) {
    // A path that Coral cannot parse names no private tool.
    if (error instanceof ConnectError && error.code === Code.InvalidArgument)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    throw error
  }
}

/**
 * Deletes one private tool and returns to the private page: the reader's own,
 * or an agent's when the form names the agent. Every member may delete their
 * own, so this needs no owner. Coral lets only an owner delete an agent's.
 */
export async function deletePrivateTool(
  context: Readonly<RouterContextProvider>,
  workspace: string,
  form: FormData,
) {
  const parent = workspaceName(workspace)
  const path = field(form, 'path')
  const agent = form.has('agent') ? agentName(workspace, field(form, 'agent')) : ''
  try {
    await management(context).privateTools.deletePrivateTool({ parent, path, agent })
  } catch (error) {
    return actionFailure(
      error,
      'You cannot delete this private tool. ',
      'It no longer exists. Close this panel.',
    )
  }
  return redirect(`${privateToolsPath(resourcePath(parent))}?${PRIVATE_TOOL_DELETED}=1`, {
    headers: privateHeaders,
  })
}
