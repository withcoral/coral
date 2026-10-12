import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'

import { fakeManagement } from './management.test-support'
import { redirectToProviderConsent } from './provider-consent.server'

const accountTarget = { kind: 'account', name: 'workspaces/w/accountAuthorizations/a' } as const

it.each(['setup', 'account'] as const)(
  'fetches fresh checked %s consent for every navigation, never a caller URL',
  async (kind) => {
    const consentNavigation = { url: 'http://sidecar:5000/authorize?state=first' }
    const getProviderSetup = vi.fn(() => ({ authorization: { consentNavigation } }))
    const getProviderAccountAuthorization = vi.fn(() => ({ consentNavigation }))
    const api = fakeManagement({
      setups: { getProviderSetup },
      accounts: { getProviderAccountAuthorization },
    })
    const target = { kind, name: 'workspaces/w/authorization-reference' }
    const response = await redirectToProviderConsent(api, target)
    expect(response.headers.get('Location')).toBe('http://sidecar:5000/authorize?state=first')
    expect(response.headers.get('Referrer-Policy')).toBe('no-referrer')
    expect(response.headers.get('Cache-Control')).toContain('no-store')
    const read = kind === 'setup' ? getProviderSetup : getProviderAccountAuthorization
    read.mockImplementationOnce(() => {
      throw new ConnectError('Consent issuance refused', Code.FailedPrecondition)
    })
    await expect(redirectToProviderConsent(api, target)).rejects.toMatchObject({
      code: Code.FailedPrecondition,
      rawMessage: 'Consent issuance refused',
    })
    expect(read).toHaveBeenCalledTimes(2)
    expect(read).toHaveBeenLastCalledWith(
      expect.objectContaining({ name: target.name }),
      expect.anything(),
    )
  },
)

it('does not navigate terminal or secret-entry authorizations', async () => {
  const api = fakeManagement({ accounts: { getProviderAccountAuthorization: () => ({}) } })
  await expect(redirectToProviderConsent(api, accountTarget)).rejects.toMatchObject({
    status: 409,
  })
})

it('refuses navigation when Coral reports screening failure without hiding progress', async () => {
  const api = fakeManagement({
    accounts: { getProviderAccountAuthorization: () => ({ consentRejected: true }) },
  })
  await expect(redirectToProviderConsent(api, accountTarget)).rejects.toMatchObject({
    rawMessage: 'CONSENT_DESTINATION_REJECTED',
  })
})
