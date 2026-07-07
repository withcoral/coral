import { type RouteConfig, index, layout, route } from '@react-router/dev/routes'

// Settings is a desktop-only surface (it drives the Electron MCP bridge); the
// web build omits it so browser users don't hit a dead "bridge unavailable" page.
const isDesktopApp = process.env.CORAL_DESKTOP_APP === '1'

export default [
  layout('routes/app-shell.tsx', [
    index('routes/index.tsx'),
    route('sources', 'routes/sources.tsx', [route(':sourceName', 'routes/source-detail.tsx')]),
    route('schema', 'routes/schema.tsx'),
    route('traces', 'routes/traces.tsx'),
    ...(isDesktopApp ? [route('settings', 'routes/settings.tsx')] : []),
  ]),
] satisfies RouteConfig
