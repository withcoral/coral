import type { Route } from './+types/source-install'
import { useNavigate } from 'react-router'

import { SourceCreateDialog } from '@/views/sources/source-create'
import { routePath } from '@/routing/routemap'

export { action } from './sources-action'

export default function SourceInstallRoute({ actionData, params }: Route.ComponentProps) {
  const navigate = useNavigate()
  const sourcesPath = routePath('workspaceSources', { workspaceId: params.workspaceId })

  return (
    <SourceCreateDialog
      actionData={actionData}
      open
      onOpenChange={(open) => {
        if (!open) navigate(sourcesPath)
      }}
    />
  )
}
