import { OrganizationRole } from '@/generated/coral/management/v1/organization_membership_pb'
import {
  data,
  Outlet,
  useLocation,
  useMatches,
  useRouteLoaderData,
  type ShouldRevalidateFunctionArgs,
} from 'react-router'
import type { Route } from './+types/app-shell'
import { requestAuthContext } from '@/auth/server-context'
import type { BrowserAuth, RequestAuth } from '@/auth/types'
import { ContentContainer } from '@/components/content-container'
import { Sidebar } from '@/components/sidebar'
import { loadActionCounts } from '@/lib/action-count.server'
import { LocalDeploymentContext } from '@/lib/deployment'
import { useActionCounts } from '@/lib/use-action-count'
import { memberWorkspaces } from '@/lib/workspace-route.server'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { ShellFormContext } from '@/views/management/forms'
import { ToastContainer } from '@/wax/components/toast'
import { BreadcrumbRoot } from '@/wax/components/breadcrumbs/breadcrumbs'
import { NavigationRoot } from '@/wax/components/navigation-frame/navigation-frame'
import { formContext, management, managementFailure, privateHeaders } from '@/lib/management.server'
import { sidebarWorkspaceView } from '@/lib/management-views.server'
import type { SidebarWorkspace } from '@/components/sidebar/sidebar'
import * as styles from './app-shell.css'

export async function loader({ request, context }: Route.LoaderArgs) {
  try {
    const api = management(request, context)
    const [all, membership, release] = await Promise.all([
      memberWorkspaces(api).catch(() => []),
      api.organizationMemberships.getCurrentOrganizationMembership({}).catch(() => null),
      api.releases.getNewerRelease({}).catch(() => null),
    ])
    const organizationOwner = membership?.role === OrganizationRole.OWNER
    const workspaces = all.filter((w) => w.state === WorkspaceState.ACTIVE)
    const archived = all.filter((w) => w.state === WorkspaceState.ARCHIVED)
    const profile = membership?.profile
    return data(
      {
        actionCounts: await loadActionCounts(api, workspaces, request.signal).catch(() => null),
        organizationOwner,
        newerRelease:
          organizationOwner && release?.release
            ? { version: release.release.version, notesUrl: release.release.notesUrl }
            : undefined,
        auth: browserAuth(context.get(requestAuthContext)),
        userName: profile?.displayName.trim() || profile?.email.trim() || undefined,
        workspaces: workspaces.map(sidebarWorkspaceView),
        archivedWorkspaces: archived.map(sidebarWorkspaceView),
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}
// Drawer selection is URL state inside one Provider. It must not reload the
// sidebar/workspace discovery; the nested route still checks Workspace access.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  actionStatus,
  defaultShouldRevalidate,
}: ShouldRevalidateFunctionArgs) {
  if ((formMethod && formMethod.toUpperCase() !== 'GET') || actionStatus !== undefined) return true
  const providerPath = /^(\/workspaces\/[^/]+\/providers\/[^/]+)(?:\/ops\/[^/]+)?$/
  const current = providerPath.exec(currentUrl.pathname)
  const next = providerPath.exec(nextUrl.pathname)
  if (current && next && current[1] === next[1] && currentUrl.search === nextUrl.search)
    return false
  return defaultShouldRevalidate
}
function browserAuth(auth: RequestAuth): BrowserAuth {
  return auth.mode === 'required'
    ? { csrfToken: auth.csrfToken, mode: 'required' }
    : { mode: 'disabled' }
}
export default function AppShell({ loaderData }: Route.ComponentProps) {
  const actionCounts = useActionCounts(loaderData.actionCounts)
  const rootData = useRouteLoaderData('root') as { sidebarIsMinimized: boolean } | undefined
  const location = useLocation()
  const matches = useMatches()
  const workspace = matches
    .map((match) => (match.loaderData as { workspace?: SidebarWorkspace } | undefined)?.workspace)
    .find((w) => w && (location.pathname === w.path || location.pathname.startsWith(`${w.path}/`)))
  return (
    <LocalDeploymentContext value={loaderData.auth.mode === 'disabled'}>
      <ShellFormContext value={loaderData.form}>
        <NavigationRoot>
          <div className={styles.layout}>
            <Sidebar
              actionCounts={actionCounts}
              auth={loaderData.auth}
              organizationOwner={loaderData.organizationOwner}
              newerRelease={loaderData.newerRelease}
              userName={loaderData.userName}
              initialIsMinimized={rootData?.sidebarIsMinimized ?? false}
              workspaces={loaderData.workspaces}
              archivedWorkspaces={loaderData.archivedWorkspaces}
              workspace={workspace}
              form={loaderData.form}
            />
            <ContentContainer>
              <ToastContainer />
              {/* Every page in a workspace names it first in its breadcrumb. */}
              <BreadcrumbRoot
                value={
                  workspace
                    ? {
                        id: 'workspace',
                        type: 'link',
                        text: workspace.displayName,
                        to: workspace.path,
                      }
                    : null
                }
              >
                <Outlet />
              </BreadcrumbRoot>
            </ContentContainer>
          </div>
        </NavigationRoot>
      </ShellFormContext>
    </LocalDeploymentContext>
  )
}
