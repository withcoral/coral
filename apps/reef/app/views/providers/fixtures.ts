// Provider data for unit tests: providers and their tools in the Demo
// workspace, which the test user owns.

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
  const callRoot = owner.builtin ? 'coral.workspace' : `coral.providers.${owner.namespace}`
  return {
    name: `workspaces/demo/providers/${owner.id}/tools/${relativePath}`,
    relativePath,
    callPath: `${callRoot}.${relativePath}`,
    summary: '',
    description: '',
    hints: {},
    policy: 'approval_required',
    available: true,
    reasons: [],
    group,
    path: `${owner.path}/tools/${relativePath}`,
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
    builtin: false,
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

/** Coral's Built-in Provider, with its tools under `coral.workspace`. */
export function builtinProvider(id: string): ProviderView {
  const view: ProviderView = { ...provider(id, 'Coral', ''), builtin: true }
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
