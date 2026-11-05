import { redirect } from 'react-router'
import type { Route } from './+types/approval'
import { privateHeaders } from '@/lib/management.server'
import { REVIEW_PARAM } from '@/views/actions/review'

/**
 * The short address of one Approval Request, the one a Run Update links. It
 * opens the Approvals Page with that request in review.
 */
export function loader({ params }: Route.LoaderArgs) {
  const name = `workspaces/${params.workspace}/approvals/${params.approval}`
  const query = new URLSearchParams({ [REVIEW_PARAM]: name })
  return redirect(`/workspaces/${params.workspace}/approvals?${query}`, {
    headers: privateHeaders,
  })
}
