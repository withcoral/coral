import { type RouteConfig, index, layout } from '@react-router/dev/routes'

export default [layout('routes/app-shell.tsx', [index('routes/index.tsx')])] satisfies RouteConfig
