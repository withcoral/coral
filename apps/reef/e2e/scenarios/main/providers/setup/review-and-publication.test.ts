import { expect, test } from 'vitest'
import { ProviderAccountScope } from '../../../../generated/coral/management/v1/account_pb.ts'
import { CredentialRoute_Kind } from '../../../../generated/coral/management/v1/provider_pb.ts'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptOperations,
  accountQuestion,
  backToConfiguration,
  chooseAccount,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  setNamespace,
  waitForPublication,
} from '../../../../support/providers.ts'

test('Setup reviews operations before accounts and preserves the draft until one publication', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Setup flow ${crypto.randomUUID()}`)
    const api = managementClients(actor.accessToken)
    const noProvider = async () => {
      expect(
        (await api.providers.listProviders({ parent: `workspaces/${workspace}` })).providers,
      ).toHaveLength(0)
    }
    await step('Upload a document and name the provider before reading it', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await noProvider()
      await openCustomSource(page)
      await page.getByLabel('OpenAPI file', { exact: true }).setInputFiles({
        name: 'records.json',
        mimeType: 'application/json',
        buffer: Buffer.from(
          openApiDocument(
            'Uploaded records',
            { listRecords: {} },
            { security: 'bearer' },
          ).toString(),
        ),
      })
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      expect(await page.getByRole('textbox', { name: 'Source', exact: true }).inputValue()).toBe(
        'Uploaded document',
      )
      await noProvider()
    })
    await step('Name it, confirm configuration and choose caller accounts', async () => {
      await page.getByLabel('Display name', { exact: true }).waitFor()
      // The document's title is suggested; the owner's name replaces it.
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'Uploaded records',
      )
      await page.getByLabel('Display name', { exact: true }).fill('Records (uploaded)')
      await page.getByLabel('Namespace', { exact: true }).fill('records')
      await confirmConfiguration(page, 'Bearer token')
      await page.getByRole('heading', { name: 'Operations', exact: true }).waitFor()
      expect(await accountQuestion(page).count()).toBe(0)
      expect(new URL(page.url()).searchParams.has('operationsAccepted')).toBe(false)
      await noProvider()
      await acceptOperations(page)
      await page.reload()
      await accountQuestion(page).waitFor()
      expect(new URL(page.url()).searchParams.get('operationsAccepted')).toBe('1')
      await page.getByRole('link', { name: 'Back', exact: true }).click()
      await page.getByRole('heading', { name: 'Operations', exact: true }).waitFor()
      expect(new URL(page.url()).searchParams.has('operationsAccepted')).toBe(false)
      await acceptOperations(page)
      expect(new URL(page.url()).searchParams.get('namespace')).toBe('records')
      await chooseAccount(page, 'own-later')
      await noProvider()
      await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
      expect(await page.getByLabel('Namespace', { exact: true }).count()).toBe(0)
      await noProvider()
    })
    await step('Revise the namespace, relist operations, and reload the review', async () => {
      await setNamespace(page, 'records_reviewed')
      await backToConfiguration(page)
      await page.waitForURL((url) => url.searchParams.get('configure') === '1')
      expect(new URL(page.url()).searchParams.has('operationsAccepted')).toBe(false)
      await confirmConfiguration(page, 'Bearer token')
      await acceptOperations(page)
      await page.reload()
      await chooseAccount(page, 'own-later')
      await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
      await noProvider()
      await page.setViewportSize({ width: 390, height: 844 })
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
      ).toBe(true)
    })
    await step('Add the provider once with the reviewed name and account choice', async () => {
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      const providers = (await api.providers.listProviders({ parent: `workspaces/${workspace}` }))
        .providers
      expect(providers).toHaveLength(1)
      expect(providers[0].namespace).toBe('records_reviewed')
      // The confirmed name is the provider's, not the document's title.
      expect((await api.providers.getProvider({ name: providers[0].name })).displayName).toBe(
        'Records (uploaded)',
      )
      // The heading uses the display name; the tree keeps the namespace.
      await page.getByRole('rowgroup', { name: 'Records (uploaded)', exact: true }).waitFor()
      expect(providers[0].credentialRoute?.kind).toBe(CredentialRoute_Kind.CALLER)
      for (const scope of [ProviderAccountScope.USER, ProviderAccountScope.SHARED]) {
        expect(
          (
            await api.accounts.getProviderAccount({
              parent: `workspaces/${workspace}`,
              provider: providers[0].name,
              scope,
            })
          ).account,
        ).toBeUndefined()
      }
    })
  })
})

// The card's reading state and a refused read are add-provider.dom.test.tsx
// cases; a bundled recipe cannot be made to fail here.
test('A catalog recipe reads in the dialog and opens its configuration', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Recipe ${crypto.randomUUID()}`)
    await step('Choose a recipe with a bundled document from the catalog', async () => {
      await page.goto(`/workspaces/${workspace}/providers`)
      // The split layout repeats the page header for the list and detail panes.
      await page.getByRole('button', { name: 'Add provider', exact: true }).first().click()
      // Gmail's document ships with Reef, so the read needs no outbound network.
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Add Gmail API', exact: true })
        .click()
    })
    await step('Configuration opens for the Setup the read made', async () => {
      await page.waitForURL(
        (url) => url.searchParams.has('setup') && url.searchParams.get('recipe') === 'gmail',
      )
      await page.getByRole('dialog').waitFor({ state: 'detached' })
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe('Gmail')
      expect(await page.getByLabel('Namespace', { exact: true }).inputValue()).toBe('gmail')
    })
  })
})
