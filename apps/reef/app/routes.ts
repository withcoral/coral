import { type RouteConfig, index, layout, route } from '@react-router/dev/routes'

import { routePattern } from './routing/routemap'

export default [
  // Public resources: health probes must not cross the session boundary; Coral
  // needs CIMD before authorization; and the browser needs auth routes outside it.
  route('healthz', 'routes/healthz.ts'),
  route('readyz', 'routes/readyz.ts'),
  route('.well-known/oauth-client', 'routes/oauth-client-metadata.ts'),
  route(routePattern('login'), 'routes/login.tsx'),
  route('auth/callback', 'routes/auth.callback.tsx'),
  route('logout', 'routes/logout.tsx'),
  layout('routes/_protected.tsx', [
    route('activity/observe', 'routes/activity-observe.ts'),
    route('activity/count', 'routes/activity-count.ts'),
    route('activity/resolve', 'routes/activity-resolve.ts'),
    route('provider-setup/oauth/callback', 'routes/provider-setup-oauth-callback.ts'),
    route(
      'workspaces/:workspace/account-authorizations/:authorization/progress',
      'routes/account-authorization-progress.ts',
    ),
    layout('routes/app-shell.tsx', [
      index('routes/index.tsx'),
      route('workspaces', 'routes/workspaces.tsx'),
      route('workspaces/:workspace/activity', 'routes/activity.tsx'),
      route('workspaces/:workspace/runs/:run', 'routes/run.tsx'),
      route('organization/members', 'routes/organization-members.tsx'),
      route('organization/workspaces', 'routes/organization-workspaces.tsx'),
      route('workspaces/:workspace', 'routes/workspace.tsx'),
      route('workspaces/:workspace/agents', 'routes/agents.tsx'),
      route('workspaces/:workspace/agents/:agent', 'routes/agent.tsx'),
      route('workspaces/:workspace/agents/:agent/settings', 'routes/agent-settings.tsx'),
      route('workspaces/:workspace/providers', 'routes/providers.tsx', { id: 'providers' }, [
        index('routes/providers-selection.tsx', { id: 'providers-index' }),
        route(':provider', 'routes/providers-selection.tsx', { id: 'providers-provider' }),
        route(':provider/tools/:tool', 'routes/providers-selection.tsx', {
          id: 'providers-tool',
        }),
      ]),
      route('workspaces/:workspace/providers/new', 'routes/provider-new.tsx', {
        id: 'provider-new',
      }),
      route('workspaces/:workspace/providers/:provider/update', 'routes/provider-update.tsx', {
        id: 'provider-update',
      }),
      route('workspaces/:workspace/providers/:provider/settings', 'routes/provider-settings.tsx', {
        id: 'provider-settings',
      }),
      route('workspaces/:workspace/providers/:provider/accounts', 'routes/provider-accounts.tsx', {
        id: 'provider-accounts',
      }),
      route(
        'workspaces/:workspace/account-authorizations/:authorization',
        'routes/account-authorization.tsx',
        { id: 'account-authorization' },
      ),
      route('workspaces/:workspace/accounts', 'routes/accounts.tsx'),
      route('workspaces/:workspace/accounts/:provider', 'routes/user-account.tsx'),
      // A function opens in a drawer over the list, and a namespace picks the
      // list's tab. Both URLs stay shareable.
      route('workspaces/:workspace/functions', 'routes/functions.tsx', [
        route(':path', 'routes/function.tsx'),
        route('namespaces/:namespace', 'routes/functions-namespace.tsx'),
      ]),
      route('workspaces/:workspace/members', 'routes/workspace-members.tsx'),
    ]),
  ]),
] satisfies RouteConfig
