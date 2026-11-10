// Provider data for unit tests: providers and their tools in the Demo
// workspace, which the test user owns.

import { CORAL_ICONS, PRIVATE_ICONS, SHARED_ICONS } from './coral-icons'
import type { ToolView, ProviderView, WorkspaceView } from './types'

export const workspaceView: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: 'users/test',
  profile: { user: 'users/test', name: 'Test', email: '', local: true },
  mcpEndpoint: '',
  connectEndpoint: '',
  path: '/workspaces/demo',
}

/** An available tool of `owner` that needs approval, in `group`. */
export function tool(owner: ProviderView, relativePath: string, group = ''): ToolView {
  return {
    key: relativePath,
    relativePath,
    hints: {},
    policy: 'approval_required',
    available: true,
    reasons: [],
    group,
  }
}

/**
 * An enabled provider with no account. Each tool is a relative path, or a
 * relative path and its group; every tool is ready.
 */
export function provider(
  id: string,
  displayName: string,
  namespace: string,
  tools: Array<string | [relativePath: string, group: string]> = [],
): ProviderView {
  const view: ProviderView = {
    id,
    path: `/workspaces/demo/providers/${id}`,
    displayName,
    namespace,
    callRoot: `coral.providers.${namespace}`,
    kind: 'added',
    account: 'No account',
    blockers: [],
    personalAccountAction: null,
    searchIndex: null,
    readyCount: tools.length,
    totalCount: tools.length,
    etag: 'etag',
    enabled: true,
    tools: [],
  }
  view.tools = tools.map((entry) =>
    typeof entry === 'string' ? tool(view, entry) : tool(view, ...entry),
  )
  return view
}

/**
 * The private group as `agents` own some of its tools. Each tool is a path of
 * your own, or a path and the agent that owns it.
 */
export function privateProvider(
  id: string,
  tools: Array<string | [relativePath: string, agent: string]> = [],
): ProviderView {
  const view: ProviderView = {
    ...provider(id, 'Private', ''),
    kind: 'private',
    callRoot: 'coral.private',
    path: '/workspaces/demo/tools/private',
    icons: PRIVATE_ICONS,
  }
  view.tools = tools.map((entry) => {
    const [relativePath, agent] = typeof entry === 'string' ? [entry] : entry
    const group = relativePath.includes('.')
      ? relativePath.slice(0, relativePath.lastIndexOf('.'))
      : ''
    return {
      ...tool(view, relativePath, group),
      // Its namespace heads the group, so the row shows the name inside it.
      ...(group && { nameInGroup: relativePath.slice(group.length + 1) }),
      policy: 'enabled' as const,
      // An agent's tool at your own tool's path is another tool, with its own key.
      ...(agent && {
        key: `${agent}/${relativePath}`,
        agent: {
          id: agent.toLowerCase(),
          label: agent,
          path: `/workspaces/demo/agents/${agent.toLowerCase()}`,
        },
      }),
    }
  })
  view.readyCount = view.totalCount = view.tools.length
  return view
}

/** The shared group, with a shared tool at each path. */
export function sharedProvider(id: string, tools: string[] = []): ProviderView {
  return {
    ...privateProvider(id, tools),
    displayName: 'Shared',
    kind: 'shared',
    callRoot: 'coral.shared',
    path: '/workspaces/demo/tools/shared',
    icons: SHARED_ICONS,
  }
}

/** Coral's Built-in Provider, with its tools directly on `coral`. */
export function builtinProvider(id: string): ProviderView {
  const view: ProviderView = {
    ...provider(id, 'Coral', ''),
    kind: 'coral',
    icons: CORAL_ICONS,
    callRoot: 'coral',
  }
  view.tools = [
    tool(view, 'addProvider'),
    { ...tool(view, 'updatePolicy'), hints: { idempotent: true } },
    // Read-only, so they start enabled.
    ...['listProviders', 'getProvider', 'getPolicy'].map((key) => ({
      ...tool(view, key),
      hints: { readOnly: true, idempotent: true },
      policy: 'enabled' as const,
    })),
    tool(view, 'updateProvider'),
  ]
  view.readyCount = view.totalCount = view.tools.length
  return view
}
