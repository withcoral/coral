import { redirect } from 'react-router'
import { Code, ConnectError } from '@connectrpc/connect'
import type { Route } from './+types/provider-setup-oauth-callback'
import { management, privateHeaders } from '@/lib/management.server'
import { authorizationPath } from '@/lib/accounts.server'
import {
  clearCallbackReturn,
  readCallbackReturn,
  stepperPathFor,
} from '@/lib/provider-publication-attempt/setup.server'

/** Coral completes the callback. Reef uses the safe workflow reference to return to its page. */
export async function loader({ request, context }: Route.LoaderArgs) {
  const headers = { ...privateHeaders, 'Referrer-Policy': 'no-referrer' }
  if (request.method !== 'GET')
    throw new Response('Invalid authorization callback', { status: 400, headers })
  let completion
  try {
    completion = await management(context).accounts.completeProviderSetupAuthorization({
      query: new URL(request.url).search.slice(1),
    })
  } catch (error) {
    const status =
      error instanceof ConnectError
        ? error.code === Code.NotFound
          ? 404
          : error.code === Code.PermissionDenied
            ? 403
            : error.code === Code.Unauthenticated
              ? 401
              : error.code === Code.Unavailable
                ? 503
                : 400
        : 503
    throw new Response(
      status === 400 ? 'Invalid authorization callback' : 'Sign-in could not be completed',
      {
        status,
        headers,
      },
    )
  }
  const { workflow } = completion
  if (workflow.case !== 'accountAuthorization' && workflow.case !== 'providerSetup')
    throw new Response('Sign-in could not be completed', { status: 503, headers })
  const pending = await readCallbackReturn(request, workflow.value, context)
  // Account callbacks always reopen Coral's exact workflow. The cookie carries
  // only a nested, revalidated navigation choice, never outer path/URL authority.
  const returnTo =
    workflow.case === 'accountAuthorization'
      ? authorizationPath(
          workflow.value,
          new URL(pending?.returnTo ?? '/', 'https://coral.invalid').searchParams.get('returnTo'),
        )
      : pending && pending.setup === workflow.value
        ? pending.returnTo
        : stepperPathFor(workflow.value)
  return redirect(returnTo, {
    headers: { ...headers, 'Set-Cookie': await clearCallbackReturn(workflow.value, context) },
  })
}
