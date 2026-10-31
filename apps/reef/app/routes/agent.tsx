import { Code, ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/agent'
import {
  actionFailure,
  agentName,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  resourcePath,
  workspaceName,
} from '@/lib/management.server'
import {
  accountHeaders,
  authorizationProgressPath,
  loadAgentProviders,
} from '@/lib/accounts.server'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import { callbackRedirectUri } from '@/lib/provider-publication-attempt/setup.server'
import { agentView, workspaceView } from '@/lib/management-views.server'
import { LiveActivity } from './activity-live'
import { PageLayout } from '@/wax/components/page-layout'
import { Banner, Button } from '@/wax/components'
import { AgentActivityPage } from '@/views/management/agent-activity-page'
import { trail } from '@/views/management/layout'
import { loadActivity } from '@/lib/activity.server'
import { loadActivityPage } from '@/lib/activity-page-fetch'
import { ACTIVITY_PAGE_SIZE } from '@/lib/activity-list'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  const api = management(context)
  const name = agentName(params.workspace, params.agent)
  try {
    const history = await api.activity.inspectAgentHistory({ name })
    if (history.deleted) {
      const [workspace, activity] = await Promise.all([
        api.workspaces.getWorkspace({ name: workspaceName(params.workspace) }),
        loadActivity(request, api, {
          scope: { workspace: workspaceName(params.workspace), agent: name },
        }),
      ])
      return data(
        {
          mode: 'history' as const,
          workspace: workspaceView(workspace),
          displayName: history.displayName,
          activity,
        },
        { headers: privateHeaders },
      )
    }
    // Settings own the Registration read; the page shows the accounts the agent needs.
    const [workspace, agent, providers, activity] = await Promise.all([
      api.workspaces.getWorkspace({ name: workspaceName(params.workspace) }),
      api.agents.getAgent({ name }),
      loadAgentProviders(context, name).catch(accountsUnavailable),
      loadActivity(request, api, {
        scope: { workspace: workspaceName(params.workspace), agent: name },
      }),
    ])
    return data(
      {
        mode: 'current' as const,
        workspace: workspaceView(workspace),
        agent: agentView(agent),
        providers:
          providers?.map((provider) => ({
            ...provider,
            icons: RECIPE_CHOICES.find((r) => r.id === provider.namespace)?.icons,
          })) ?? null,
        form: formContext(context),
        activity,
        // Only a history Coral could read shows the agent never called.
        idle: !activity.listUnavailable && !activity.rows.length && !activity.hasBefore,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    const selection = new URL(request.url).searchParams
    if (
      error instanceof ConnectError &&
      (error.code === Code.NotFound || error.code === Code.PermissionDenied) &&
      selection.get('record')
    ) {
      // Agent context and exact history have independent owning authorization.
      // Re-read both the list and selection; never retain an earlier context payload.
      try {
        const activity = await loadActivity(request, api, {
          scope: { workspace: workspaceName(params.workspace), agent: name },
        })
        return data({ mode: 'unavailable' as const, activity }, { headers: privateHeaders })
      } catch (selectionError) {
        managementFailure(selectionError)
      }
    }
    managementFailure(error)
  }
}
/**
 * The accounts are one section of the page, so Coral failing to read them
 * leaves the agent and its activity readable. A lapsed sign-in still fails
 * the page, so you sign in again.
 */
function accountsUnavailable(error: unknown): null {
  if (error instanceof ConnectError && error.code !== Code.Unauthenticated) return null
  throw error
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const api = management(context)
  if (field(form, 'intent') !== 'connectAccount')
    throw new Response('Invalid action', { status: 400 })
  const name = agentName(params.workspace, params.agent)
  try {
    const authorization = await api.accounts.startProviderAccountAuthorization({
      parent: workspaceName(params.workspace),
      requestId: field(form, 'requestId'),
      redirectUri: callbackRedirectUri(request, context),
      target: {
        case: 'agentAccount',
        value: { agent: name, provider: field(form, 'provider') },
      },
    })
    // The page's connect dialog follows the authorization's progress; sign-in returns here.
    // Not from the request URL: a fetcher posts to the route's `.data` URL.
    return data(
      {
        error: null,
        progress: authorizationProgressPath(authorization.name, resourcePath(name)),
      },
      { headers: accountHeaders },
    )
  } catch (error) {
    return actionFailure(error, 'Coral could not connect the account for this agent. ')
  }
}

// The Activity reader loads more of its list from this loader through a fetcher.
export function clientLoader({ request, serverLoader }: Route.ClientLoaderArgs) {
  return loadActivityPage(request, serverLoader)
}
export default function AgentRoute({ loaderData, params }: Route.ComponentProps) {
  if (loaderData.mode === 'unavailable')
    return (
      <PageLayout
        variant="split"
        breadcrumbs={trail([], 'Activity')}
        header={
          <Banner
            variant="warning"
            title="You cannot inspect this agent"
            action={
              <Button.InternalLink
                to={`/workspaces/${params.workspace}/activity`}
                variant="bare"
                size="x-small"
              >
                Open activity
              </Button.InternalLink>
            }
          >
            Open the workspace activity instead.
          </Banner>
        }
      >
        <LiveActivity initial={loaderData.activity} />
      </PageLayout>
    )
  if (loaderData.mode === 'history')
    return (
      <PageLayout
        variant="split"
        breadcrumbs={trail(
          [{ label: 'Agents', to: `${loaderData.workspace.path}/agents` }],
          loaderData.displayName,
        )}
        header={
          <Banner variant="info" title="This agent cannot start new work">
            Inspect its retained activity below.
          </Banner>
        }
      >
        <LiveActivity initial={loaderData.activity} />
      </PageLayout>
    )
  return (
    <AgentActivityPage
      key={loaderData.agent.name}
      workspace={loaderData.workspace}
      agent={loaderData.agent}
      providers={loaderData.providers}
      form={loaderData.form}
      activity={<LiveActivity initial={loaderData.activity} />}
      recentActivity={
        <LiveActivity
          initial={loaderData.activity}
          more={{
            to: `${loaderData.agent.path}?view=activity`,
            label: 'View all activity',
            // The card fills the page's height and scrolls, so it lists the whole first page.
            roots: ACTIVITY_PAGE_SIZE,
          }}
        />
      }
      idle={loaderData.idle}
    />
  )
}
