// Real management prerequisites and safe evidence for the removal journeys.
import { expect, inject } from 'vitest'
import {
  CredentialRoute_Kind,
  SetupAuthorizationState,
} from '../generated/coral/management/v1/provider_pb.ts'
import { ProviderAccountScope } from '../generated/coral/management/v1/account_pb.ts'
import { managementClients } from './management.ts'
import { publishWorkspaceProvider } from './providers.ts'

export async function assertUserAccountRemainsRetired(
  own: ReturnType<typeof managementClients>,
  workspace: string,
  previous: { provider: string; pending: string },
) {
  expect(
    (
      await own.accounts.getProviderAccount({
        parent: workspace,
        provider: previous.provider,
        scope: ProviderAccountScope.USER,
      })
    ).account,
  ).toBeUndefined()
  expect(
    (await own.accounts.getProviderAccountAuthorization({ name: previous.pending })).state,
  ).toBe(SetupAuthorizationState.CANCELED)
}

export async function prepareOffboardingAccount(
  ownerToken: string,
  callerToken: string,
  workspace: string,
  agent?: string,
) {
  const publication = await publishWorkspaceProvider(
    ownerToken,
    workspace,
    Buffer.from(
      JSON.stringify({
        openapi: '3.0.3',
        info: { title: 'Offboarding fixture', version: '1' },
        servers: [{ url: 'https://example.com' }],
        security: [{ bearer: [] }],
        components: { securitySchemes: { bearer: { type: 'http', scheme: 'bearer' } } },
        paths: {
          '/value': { get: { operationId: 'value', responses: { '200': { description: 'OK' } } } },
        },
      }),
    ),
    'offboarding',
    CredentialRoute_Kind.CALLER,
  )
  const provider = publication.provider
  const api = managementClients(callerToken)
  const redirectUri = new URL('/provider-setup/oauth/callback', inject('reel').target.baseURL).href
  const authorization = await api.accounts.startProviderAccountAuthorization({
    parent: workspace,
    requestId: crypto.randomUUID(),
    redirectUri,
    target: agent
      ? { case: 'agentAccount', value: { provider, agent } }
      : { case: 'provider', value: provider },
  })
  const connected = await api.accounts.submitProviderAccountSecret({
    name: authorization.name,
    credentialValues: { token: 'disposable-offboarding-secret' },
  })
  expect(connected.state).toBe(SetupAuthorizationState.SUCCEEDED)
  const account = (
    await api.accounts.getProviderAccount({
      parent: workspace,
      provider,
      scope: agent ? ProviderAccountScope.AGENT : ProviderAccountScope.USER,
      agent,
    })
  ).account!
  const pending = await api.accounts.startProviderAccountAuthorization({
    parent: workspace,
    requestId: crypto.randomUUID(),
    redirectUri,
    target: { case: 'reconnect', value: { provider, account: account.id } },
  })
  return { provider, pending: pending.name, account: account.id }
}
