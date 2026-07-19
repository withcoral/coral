import { type RouteConfig, index, layout, route } from '@react-router/dev/routes'

import { routePattern } from './routing/routemap'

// Settings is a desktop-only surface (it drives the Electron MCP bridge); the
// web build omits it so browser users don't hit a dead "bridge unavailable" page.
const isDesktopApp = process.env.CORAL_DESKTOP_APP === '1'

export default [
  // Action-only resource route: OAuth/device-code install streams progress over
  // same-origin fetch. It intentionally sits outside the app shell and does not
  // render a page.
  route(`${routePattern('workspaceSource')}/oauth-install`, 'routes/source-oauth-install.ts'),
  layout('routes/app-shell.tsx', [
    index('routes/index.tsx'),
    route(routePattern('workspaces'), 'routes/workspaces-action.ts'),
    route(routePattern('workspaceSources'), 'routes/sources.tsx', [
      route(':sourceName', 'routes/source-detail.tsx'),
    ]),
    route(routePattern('workspaceSchema'), 'routes/schema.tsx', [
      index('routes/schema-empty.tsx'),
      route(':schemaName/:tableName', 'routes/schema-table.tsx'),
    ]),
    route(routePattern('workspaceTraces'), 'routes/traces.tsx', [
      route(':traceId', 'routes/trace-detail.tsx'),
    ]),
    ...(isDesktopApp ? [route(routePattern('settings'), 'routes/settings.tsx')] : []),
  ]),
] satisfies RouteConfig
