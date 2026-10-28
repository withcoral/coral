import { expect, test } from 'vitest'
import type { Page } from 'playwright'
import { authenticatedTarget, loginActor } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { oauthApiServer } from '../../../support/provider-fixtures.ts'
import { managementClients } from '../../../support/management.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import {
  acceptTools,
  addProviderFromFile,
  chooseAccount,
  chooseFile,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  openProviderSettings,
  openYourAccount,
  waitForNoAccount,
  waitForPublication,
  waitForYourAccount,
} from '../../../support/providers.ts'

/** The Permissions table's rows: what Coral requests. */
async function permissions(page: Page) {
  const rows = page
    .getByRole('table', { name: 'Permissions', exact: true })
    .getByRole('rowgroup')
    .last()
    .getByRole('row')
  return (await rows.allInnerTexts()).map((row) => row.split(/\s+/).join(' ').trim())
}

test.for(['complete', 'partial', 'rejected'] as const)(
  'Published provider OAuth account authorization: %s grant',
  async (grant, context) => {
    const actor = await loginActor(context, authenticatedTarget().authentication.owner)
    const token = `published-access-${crypto.randomUUID()}`
    const upstream = await oauthApiServer(context, token, {
      ...(grant === 'partial' ? { reportedScope: 'read' } : {}),
      ...(grant === 'rejected' ? { tokenError: 'invalid_grant' } : {}),
    })
    await withBrowserSession(
      context,
      async ({ page, step }) => {
        const responses = await collectDataResponses(page)
        let accountPage = ''
        await step(
          'Add a caller provider without authorizing an account during publication',
          async () => {
            const workspace = await createWorkspace(
              actor.accessToken,
              `Account OAuth ${crypto.randomUUID()}`,
            )
            await addProviderFromFile(page, workspace, upstream.document('Records'), 'records', {
              account: 'own-later',
            })
            await openProviderSettings(page)
            await openYourAccount(page)
            accountPage = new URL(page.url()).pathname
            await page.getByText('Sign in to Records to connect it.', { exact: true }).waitFor()
            // Before a grant the table lists only what Coral asks for.
            await page.getByText('Coral asks for 2 permissions.', { exact: true }).waitFor()
            expect(await permissions(page)).toEqual(['read', 'write'])
          },
        )
        await step('One click goes to host-owned OAuth consent and returns here', async () => {
          await page.getByRole('button', { name: 'Connect', exact: true }).click()
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
          const url = new URL(page.url())
          expect(url.searchParams.get('code_challenge_method')).toBe('S256')
          expect(url.searchParams.get('scope')).toBe('read write')
          expect(new URL(url.searchParams.get('redirect_uri')!).pathname).toBe(
            '/provider-setup/oauth/callback',
          )
          expect(upstream.counts.tokens).toBe(0)
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
          if (grant === 'rejected')
            await page
              .getByText('The service rejected the sign-in code.', { exact: false })
              .waitFor()
          else {
            await waitForYourAccount(page)
            expect(new URL(page.url()).pathname).toBe(accountPage)
          }
          expect(upstream.counts.tokens).toBe(1)
        })
        await step('The reported grant connects the account and shows as written', async () => {
          await page.goto(accountPage)
          if (grant === 'rejected') {
            await waitForNoAccount(page)
            expect(await permissions(page)).toEqual(['read', 'write'])
          } else {
            await waitForYourAccount(page)
            // The grant shows as the service reported it, apart from the request.
            await page
              .getByRole('complementary', { name: 'Details', exact: true })
              .getByRole('definition')
              .filter({ hasText: grant === 'partial' ? /^read$/ : /^read write$/ })
              .waitFor()
            expect(await permissions(page)).toEqual(['read', 'write'])
          }
          for (const body of await responses()) {
            expect(body).not.toContain(token)
            expect(body).not.toContain('disposable-code')
            expect(body).not.toContain('code_challenge')
            expect(body).not.toContain('code_verifier')
          }
        })
      },
      { actor: actor.label, storageState: actor.storageState },
    )
  },
)

test.for(['complete', 'deny'] as const)(
  'Optional personal OAuth during Add without discovery credentials: %s',
  async (choice, context) => {
    const actor = await loginActor(context, authenticatedTarget().authentication.owner)
    const token = `initial-personal-${crypto.randomUUID()}`
    const upstream = await oauthApiServer(context, token)
    const api = managementClients(actor.accessToken)
    await withBrowserSession(
      context,
      async ({ page, step }) => {
        const responses = await collectDataResponses(page)
        let workspace = ''
        let setupName = ''
        let published: Promise<URLSearchParams> | undefined
        let grant: string | null = null
        await step(
          'Choose to connect your personal account before adding the provider',
          async () => {
            workspace = await createWorkspace(
              actor.accessToken,
              `Initial personal OAuth ${crypto.randomUUID()}`,
            )
            await page.goto(`/workspaces/${workspace}/providers/new`)
            await chooseFile(page, 'records.json', upstream.document('Personal records'))
            await page.getByRole('button', { name: 'Read', exact: true }).click()
            await page.getByLabel('Namespace', { exact: true }).fill('personalRecords')
            await confirmConfiguration(page)
            await acceptTools(page)
            setupName = `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
            expect(upstream.counts.consents).toBe(0)
            await chooseAccount(page, 'own-connect')
            await page.getByRole('button', { name: /^Sign in/ }).click()
            if (choice === 'complete') published = nextPublication(page)
            await page
              .getByRole('link', {
                name: choice === 'complete' ? 'Approve fixture access' : 'Deny fixture access',
                exact: true,
              })
              .click()
          },
        )
        await step(
          'Add the provider with the approved account, or without one after denial',
          async () => {
            if (published) {
              // The completed sign-in answered the question, so Coral adds the
              // provider with it rather than ask again.
              const form = await published
              expect(form.get('account')).toBe('caller')
              grant = form.get('accountGrant')
              expect(grant).toBeTruthy()
            } else {
              await page
                .getByText('You denied access. Sign in again to approve access.', { exact: true })
                .waitFor()
              // Nothing was kept, so the outcomes that name a sign-in are gone
              // and the plain question waits for an answer.
              for (const name of ['Connect now with this account', 'Use this account'])
                expect(await page.getByRole('radio', { name, exact: true }).count()).toBe(0)
              await chooseAccount(page, 'own-later')
              expect(
                (await api.providers.listProviders({ parent: `workspaces/${workspace}` }))
                  .providers,
              ).toHaveLength(1)
              await page.getByRole('button', { name: 'Add provider', exact: true }).click()
            }
            await waitForPublication(page)
            const provider = new URL(page.url()).pathname.slice(1)
            const inspect = (scope: ProviderAccountScope) =>
              api.accounts.getProviderAccount({
                parent: `workspaces/${workspace}`,
                provider,
                scope,
              })
            const account = (await inspect(ProviderAccountScope.USER)).account
            if (choice === 'complete') {
              expect(account!.state).toBe(ProviderAccountState.READY)
              expect(account!.id).toBe(grant)
            } else expect(account).toBeUndefined()
            expect((await inspect(ProviderAccountScope.SHARED)).account).toBeUndefined()
            expect((await api.setups.getProviderSetup({ name: setupName })).authorized).toBe(false)
            expect(upstream.counts.tokens).toBe(choice === 'complete' ? 1 : 0)
            expect(upstream.counts.consents).toBe(1)
            for (const body of await responses()) {
              expect(body).not.toContain(token)
              expect(body).not.toContain('disposable-code')
            }
          },
        )
      },
      { actor: actor.label, storageState: actor.storageState },
    )
  },
)
