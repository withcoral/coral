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
    route('workspaces/:workspace/approvals/history', 'routes/approvals-history.ts'),
    route('workspaces/:workspace/shared-tools/publish', 'routes/shared-tool-publish.ts'),
    route('workspaces/:workspace/providers/:provider/icon', 'routes/provider-icon.ts'),
    route('provider-setup/oauth/callback', 'routes/provider-setup-oauth-callback.ts'),
    route(
      'workspaces/:workspace/account-authorizations/:authorization/progress',
      'routes/account-authorization-progress.ts',
    ),
    layout('routes/app-shell.tsx', [
      index('routes/index.tsx'),
      route('workspaces', 'routes/workspaces.tsx'),
      route('workspaces/:workspace/activity', 'routes/activity.tsx'),
      route('workspaces/:workspace/approvals', 'routes/approvals.tsx'),
      route('workspaces/:workspace/approvals/:approval', 'routes/approval.tsx'),
      route('workspaces/:workspace/runs/:run', 'routes/run.tsx'),
      route('organization/members', 'routes/organization-members.tsx'),
      route('organization/workspaces', 'routes/organization-workspaces.tsx'),
      route('usage-data', 'routes/usage-data.tsx'),
      route('workspaces/:workspace', 'routes/workspace.tsx'),
      route('workspaces/:workspace/agents', 'routes/agents.tsx'),
      route('workspaces/:workspace/agents/:agent', 'routes/agent.tsx'),
      route('workspaces/:workspace/agents/:agent/settings', 'routes/agent-settings.tsx'),
      // One table of every tool. The address names what it reveals: every
      // group, one group, or one open tool. A provider's group and tools keep
      // its resource path; the shared and the private group each have a fixed
      // one, and an agent's private tool names the agent after its path.
      layout('routes/tools.tsx', { id: 'tools' }, [
        route('workspaces/:workspace/tools', 'routes/tools-selection.tsx', { id: 'tools-index' }),
        route('workspaces/:workspace/tools/:group', 'routes/tools-selection.tsx', {
          id: 'tools-group',
        }),
        route('workspaces/:workspace/tools/:group/:tool', 'routes/tools-selection.tsx', {
          id: 'tools-group-tool',
        }),
        route(
          'workspaces/:workspace/tools/:group/:tool/agents/:agent',
          'routes/tools-selection.tsx',
          { id: 'tools-group-agent-tool' },
        ),
        route('workspaces/:workspace/providers/:provider', 'routes/tools-selection.tsx', {
          id: 'tools-provider',
        }),
        route(
          'workspaces/:workspace/providers/:provider/tools/:tool',
          'routes/tools-selection.tsx',
          { id: 'tools-provider-tool' },
        ),
      ]),
      // The list of providers is now the Tools page.
      route('workspaces/:workspace/providers', 'routes/providers-redirect.ts'),
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
      route('workspaces/:workspace/members', 'routes/workspace-members.tsx'),
    ]),
  ]),
] satisfies RouteConfig
