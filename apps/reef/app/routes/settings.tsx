import type { Route } from './+types/settings'
import { useFetcher, useRouteLoaderData } from 'react-router'
import type { loader as appShellLoader } from './app-shell'

import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { Settings, SettingsHydrateFallback } from '@/views/settings/settings'

export { clientAction, clientLoader, loader } from './settings-loader'

export default function SettingsRoute({ loaderData }: Route.ComponentProps) {
  if (loaderData.runtime !== 'desktop') return null

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
  return isCoralDesktopBuild() ? <SettingsHydrateFallback /> : null
}
