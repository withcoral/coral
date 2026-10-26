// Provider data for unit tests: providers and their operations in the Demo
// workspace, which the test user owns.

import type { OperationView, ProviderView, WorkspaceView } from './types'

export const workspaceView: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: 'users/test',
  profile: { user: 'users/test', name: 'Test', email: '', local: true },
  mcpEndpoint: '',
  path: '/workspaces/demo',
}

/** An available operation of `owner` that needs approval, in `group`. */
export function operation(owner: ProviderView, relativePath: string, group = ''): OperationView {
  const callRoot = owner.builtin ? 'coral.workspace' : `coral.providers.${owner.namespace}`
  return {
    name: `workspaces/demo/providers/${owner.id}/operations/${relativePath}`,
    relativePath,
    callPath: `${callRoot}.${relativePath}`,
    summary: '',
    description: '',
    hints: {},
    policy: 'approval_required',
    available: true,
    reasons: [],
    group,
    path: `${owner.path}/ops/${relativePath}`,
  }
}

/**
 * An enabled provider with no account. Each operation is a relative path, or a
 * relative path and its group; every operation is ready.
 */
export function provider(
  id: string,
  displayName: string,
  namespace: string,
  operations: Array<string | [relativePath: string, group: string]> = [],
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
    readyCount: operations.length,
    totalCount: operations.length,
    etag: 'etag',
    enabled: true,
    operations: [],
  }
  view.operations = operations.map((entry) =>
    typeof entry === 'string' ? operation(view, entry) : operation(view, ...entry),
  )
  return view
}

/** Coral's Built-in Provider, with its operations under `coral.workspace`. */
export function builtinProvider(id: string): ProviderView {
  const view: ProviderView = { ...provider(id, 'Coral', ''), builtin: true }
  view.operations = [
    operation(view, 'addProvider'),
    // Read-only, so they start enabled.
    ...['listProviders', 'getProvider', 'getPolicy'].map((key) => ({
      ...operation(view, key),
      hints: { readOnly: true, idempotent: true },
      policy: 'enabled' as const,
    })),
  ]
  view.readyCount = view.totalCount = view.operations.length
  return view
}
