import { type RouteConfig, index, layout, route } from '@react-router/dev/routes'

import { routePattern } from './routing/routemap'

export default [
  // Public resources: health probes must not cross the session boundary; Coral
  // needs CIMD before authorization; and the browser needs auth routes outside it.
  route('healthz', 'routes/healthz.ts'),
  route('.well-known/oauth-client', 'routes/oauth-client-metadata.ts'),
  route(routePattern('login'), 'routes/login.tsx'),
  route('auth/callback', 'routes/auth.callback.tsx'),
  route('logout', 'routes/logout.tsx'),
  layout('routes/_protected.tsx', [
    // Action-only resource route: OAuth/device-code install streams progress over
    // same-origin fetch. It and onboarding share the auth boundary but do not
    // render the app shell.
    route(`${routePattern('workspaceSource')}/oauth-install`, 'routes/source-oauth-install.ts'),
    route(routePattern('onboarding'), 'routes/onboarding.tsx'),
    layout('routes/app-shell.tsx', [
      index('routes/index.tsx'),
      route(routePattern('workspaces'), 'routes/workspaces-action.ts'),
      route(routePattern('workspaceSources'), 'routes/sources.tsx', [
        route('discover', 'routes/source-discovery.ts'),
        route('install', 'routes/source-install.tsx'),
        route('oauth-import', 'routes/source-oauth-import.ts'),
        route(':sourceName', 'routes/source-detail.tsx'),
      ]),
      route(routePattern('workspaceFunctions'), 'routes/functions.tsx'),
      route(routePattern('workspaceSchema'), 'routes/schema.tsx', [
        index('routes/schema-empty.tsx'),
        route(':schemaName/:tableName', 'routes/schema-table.tsx'),
        route(':schemaName/functions/:functionName', 'routes/schema-table-function.tsx'),
      ]),
      route(routePattern('workspaceTraces'), 'routes/traces.tsx', [
        route(':traceId', 'routes/trace-detail.tsx'),
      ]),
      route(routePattern('settings'), 'routes/settings.tsx'),
    ]),
  ]),
] satisfies RouteConfig
