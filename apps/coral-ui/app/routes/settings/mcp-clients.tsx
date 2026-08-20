import type { Route } from './+types/mcp-clients'
import { useFetcher, useRouteLoaderData } from 'react-router'
import type { loader as appShellLoader } from '../app-shell'

import { Settings, SettingsHydrateFallback } from '@/views/settings/settings'

export { clientAction, clientLoader, loader } from '../settings-loader'

export default function SettingsMcpClientsRoute({ loaderData }: Route.ComponentProps) {
  const fetcher = useFetcher()
  const workspaces = useRouteLoaderData<typeof appShellLoader>('routes/app-shell')?.workspaces ?? []
  const pendingClientId = fetcher.formData?.get('clientId')

  return (
    <Settings
      loaderData={loaderData}
      pendingClientIds={typeof pendingClientId === 'string' ? [pendingClientId] : []}
      workspaces={workspaces}
      onWorkspaceChange={(clientId, workspace) => {
        fetcher.submit({ clientId, workspace: workspace ?? '' }, { method: 'post' })
      }}
    />
  )
}

export function HydrateFallback() {
  return <SettingsHydrateFallback />
}
