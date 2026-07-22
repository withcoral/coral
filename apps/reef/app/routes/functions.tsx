import type { Route } from './+types/functions'

import { FunctionsIndex } from '@/views/functions/functions-index'

export { action, loader } from './functions.server'

export default function FunctionsRoute({ actionData, loaderData, params }: Route.ComponentProps) {
  return (
    <FunctionsIndex
      actionData={actionData}
      editor={loaderData.editor}
      functions={loaderData.functions}
      loadError={loaderData.loadError}
      workspaceId={params.workspaceId}
    />
  )
}
