import { type RouteConfig, index, layout, route } from '@react-router/dev/routes'

export default [
  layout('routes/app-shell.tsx', [
    index('routes/index.tsx'),
    route('settings', 'routes/settings.tsx'),
  ]),
] satisfies RouteConfig
