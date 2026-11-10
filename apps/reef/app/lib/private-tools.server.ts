import { Code, ConnectError } from '@connectrpc/connect'
import { redirect, type RouterContextProvider } from 'react-router'
import { PRIVATE_ICONS } from '@/views/providers/coral-icons'
import type { ProviderView, ToolView, WorkspaceView } from '@/views/providers/types'
import { PRIVATE_TOOL_DELETED, privateToolsPath } from '@/views/tools/addresses'
import type { PrivateToolDetailView } from '@/views/tools/types'
import {
  actionFailure,
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
  const [provider, rows] = await Promise.all([
    api.providers.getProvider({ name }),
    listAllToolAccess(api, name),
  ])
  // Only an owner's rows name agents, and only an owner may list the agents.
  const agents = rows.some((row) => row.agent)
    ? await agentLabels(api, workspace.name)
    : new Map<string, string>()
  const tools = rows.map((row) => ({
    ...privateToolRow(toolView(row)),
    ...(row.agent && {
      agent: {
        label: agents.get(row.agent) ?? row.agent.split('/').at(-1)!,
        path: resourcePath(row.agent),
      },
    }),
  }))
  return {
    ...groupView(provider, tools),
    kind: 'private',
    path: privateToolsPath(workspace.path),
    personalAccountAction: null,
    // Search indexes no tool from the Private Provider's own catalog, which holds none.
    searchIndex: null,
    icons: PRIVATE_ICONS,
  }
}

/**
 * A private tool's group is the namespace its path starts with, so its row
 * shows only the name inside that namespace.
 */
function privateToolRow(tool: ToolView): ToolView {
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

/** Reads the one private tool the drawer shows. It is always the reader's own. */
export async function privateToolDetail(
  api: Pick<ManagementApi, 'privateTools'>,
  workspace: string,
  path: string,
): Promise<PrivateToolDetailView> {
  try {
    return privateToolView(await api.privateTools.getPrivateTool({ parent: workspace, path }))
  } catch (error) {
    // A path that Coral cannot parse names no private tool.
    if (error instanceof ConnectError && error.code === Code.InvalidArgument)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    throw error
  }
}

/**
 * Deletes one of the reader's own private tools and returns to the private
 * page. Every member may delete their own, so this needs no owner.
 */
export async function deletePrivateTool(
  context: Readonly<RouterContextProvider>,
  workspace: string,
  form: FormData,
) {
  const parent = workspaceName(workspace)
  const path = field(form, 'path')
  try {
    await management(context).privateTools.deletePrivateTool({ parent, path })
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
