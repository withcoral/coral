import { redirect, useRouteLoaderData } from 'react-router'
import type { Route } from './+types/index'
import type { loader as shellLoader } from './app-shell'
import { EmptyPage } from '@/components/empty-page'
import { PageLayout } from '@/wax/components/page-layout'
import {
  management,
  managementFailure,
  privateHeaders,
  resourcePath,
} from '@/lib/management.server'
import { activeWorkspaces } from '@/lib/workspace-route.server'
import { ActionDialog, NameInput } from '@/views/management/forms'
import { trail } from '@/views/management/layout'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

// Activity belongs to a Workspace, so home opens the first Workspace's Activity.
export async function loader({ request, context }: Route.LoaderArgs) {
  let workspaces
  try {
    workspaces = await activeWorkspaces(management(context), request.signal)
  } catch (error) {
    managementFailure(error)
  }
  if (!workspaces.length) return null
  return redirect(`${resourcePath(workspaces[0].name)}/activity`, { headers: privateHeaders })
}
// Home renders only for someone with no active Workspace: nothing else in
// Coral works until they create one.
export default function Home() {
  const shell = useRouteLoaderData<typeof shellLoader>('routes/app-shell')
  return (
    <PageLayout breadcrumbs={trail([], 'Home')}>
      <EmptyPage
        iconName="PanelsTopLeft"
        title="No workspaces yet"
        description="Create a workspace to add providers and run programs."
        action={
          shell && (
            <ActionDialog
              primary
              title="Create workspace"
              description="Create a workspace to manage providers and run programs. You become its first owner."
              form={shell.form}
              intent="createWorkspace"
              action="/workspaces"
            >
              <NameInput label="Workspace name" />
            </ActionDialog>
          )
        }
      />
    </PageLayout>
  )
}
