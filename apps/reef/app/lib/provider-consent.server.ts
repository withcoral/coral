import { redirect } from 'react-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { privateHeaders, type ManagementApi } from './management.server'

/**
 * The only Provider consent redirect interface: references in, HTTP response out.
 * Each call reads fresh issuance from Coral's Provider authorization Module.
 * No URL-taking export or persistent navigation cache exists here.
 */
export async function redirectToProviderConsent(
  api: ManagementApi,
  target: { kind: 'setup' | 'account'; name: string },
): Promise<Response> {
  const issued =
    target.kind === 'setup'
      ? (await api.setups.getProviderSetup({ name: target.name })).authorization
      : await api.accounts.getProviderAccountAuthorization({ name: target.name })
  if (issued?.consentRejected)
    throw new ConnectError('CONSENT_DESTINATION_REJECTED', Code.FailedPrecondition)
  const address = issued?.consentNavigation?.url
  if (!address)
    throw new Response('This sign-in is no longer pending.', {
      status: 409,
      headers: privateHeaders,
    })
  return redirect(address, { headers: { ...privateHeaders, 'Referrer-Policy': 'no-referrer' } })
}
