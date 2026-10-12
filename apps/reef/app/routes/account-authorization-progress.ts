import { Code, ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/account-authorization-progress'
import { accountHeaders, loadAuthorization } from '@/lib/accounts.server'
import { failureReason } from '@/lib/management.server'

const unchecked = 'Coral could not check the connection. '

/**
 * An authorization's progress for the agent page's connect dialog. Every
 * outcome is data, so a failed check stays in the dialog and never replaces
 * the page under it.
 */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    return data(
      { view: await loadAuthorization(request, context, params) },
      {
        headers: accountHeaders,
      },
    )
  } catch (error) {
    // A succeeded authorization redirects to where it started. The dialog is
    // already there, so it reloads that page's data instead.
    if (error instanceof Response && error.status >= 300 && error.status < 400)
      return data({ succeeded: true as const }, { headers: accountHeaders })
    // Not a 401 here: sign-in would then return to this route, not the agent's page.
    // The dialog reloads that page instead, and its 401 starts sign-in.
    if (error instanceof ConnectError && error.code === Code.Unauthenticated)
      return data({ signedOut: true as const }, { headers: accountHeaders })
    const message =
      error instanceof Response ? unchecked.trim() : failureReason(error, unchecked)[1]
    return data({ error: message }, { headers: accountHeaders })
  }
}
