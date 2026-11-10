import { data } from 'react-router'
import type { Route } from './+types/shared-tool-publish'
import { managementFailure, mutationForm, privateHeaders } from '@/lib/management.server'
import { publishReview, publishSharedTool } from '@/lib/shared-tools.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'

/**
 * The dialog asks for each review itself: when it opens, when the owner
 * enters another path, and after a refused publish. No other change on the
 * page makes a review stale, and a publish that succeeds closes the dialog,
 * so a review after it would be a compile nobody reads.
 */
export function shouldRevalidate() {
  return false
}

/**
 * The publish dialog's review. Its address names what the publish copies and
 * the shared tool path. Coral computes the review and writes nothing.
 */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    const review = await publishReview(api, workspace.name, new URL(request.url).searchParams)
    return data(review, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}

/** Publishes what the dialog's review showed. */
export async function action({ request, context, params }: Route.ActionArgs) {
  return publishSharedTool(context, params.workspace, await mutationForm(request, context))
}
