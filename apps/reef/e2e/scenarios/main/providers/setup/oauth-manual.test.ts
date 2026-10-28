import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { oauthMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  accountQuestion,
  chooseAccount,
  collectDataResponses,
  configurationNext,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  openTool,
  signInToReadTools,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

// The server supports neither Client ID Metadata Document nor Dynamic Client
// Registration, so the person uses their own OAuth application.
// oauth-openapi owns the consent URL, resume and forged-callback checks.
test('MCP discovery signs in through an explicit OAuth application and adds the provider', async (context) => {
  const access = `fixture-access-${crypto.randomUUID()}`
  const upstream = await oauthMcpServer(context, access, {
    dcr: false,
    cimd: false,
  })
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const responses = await collectDataResponses(page)
    let workspace = ''
    await step('The server asks for a sign-in through my OAuth application', async () => {
      workspace = await createWorkspace(actor.accessToken, `Manual OAuth ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      // The Configuration skeleton appears before the source has a durable setup URL.
      await page.waitForURL((url) => url.searchParams.has('setup'))
      await configurationNext(page).waitFor()
      // Configuration is confirmed before provisional discovery authorization.
      await page.getByLabel('Namespace', { exact: true }).fill('manual')
      await confirmConfiguration(page)
      expect(await accountQuestion(page).count()).toBe(0)
      await signInToReadTools(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      // No registration: the consent names the person's own application.
      expect(new URL(page.url()).searchParams.get('client_id')).toBe('manual-client')
      expect(upstream.counts.registrations).toBe(0)
    })
    await step('Approving exchanges the code once', async () => {
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
      expect(upstream.counts.tokens).toBe(1)
    })
    await step('A complete exchange adds it', async () => {
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      await waitForProviderStatus(page, { path: new URL(page.url()).pathname }, 'Ready', {
        available: 1,
        total: 1,
      })
      // The tool's full name carries the chosen namespace.
      const drawer = await openTool(page, 'lookup')
      await drawer.getByText('coral.providers.manual.lookup', { exact: true }).waitFor()
    })
    for (const body of await responses()) {
      expect(body).not.toContain(access)
      expect(body).not.toContain('disposable-code')
      expect(body).not.toContain('code_verifier')
    }
  })
})
