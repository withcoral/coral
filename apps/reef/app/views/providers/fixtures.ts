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
  return {
    name: `workspaces/demo/providers/${owner.id}/operations/${relativePath}`,
    relativePath,
    callPath: `coral.providers.${owner.namespace}.${relativePath}`,
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
    account: 'No account',
    blockers: [],
    personalAccountAction: null,
    searchIndexFailure: null,
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
