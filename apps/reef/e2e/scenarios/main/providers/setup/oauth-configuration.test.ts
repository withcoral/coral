import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { bearerMcpServer, oauthMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  accountQuestion,
  chooseAccount,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  readTools,
  signInMethods,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

test.for(['anonymous', 'bearer', 'oauth'])(
  'MCP %s reviews metadata before sign-in and resets the destination safely',
  async (method, context) => {
    const secret = `configuration-canary-${crypto.randomUUID()}`
    const upstream =
      method === 'oauth'
        ? await oauthMcpServer(context, secret)
        : await bearerMcpServer(context, secret, method === 'anonymous')
    const replacement = await bearerMcpServer(context, 'other-token')
    await withOwnerSession(context, async ({ page, step, actor }) => {
      let workspace = ''
      await step('Review only supported configuration before entering credentials', async () => {
        workspace = await createWorkspace(actor.accessToken, `MCP review ${crypto.randomUUID()}`)
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await openCustomSource(page)
        await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
        expect(await page.getByLabel('Access token', { exact: true }).count()).toBe(0)
        await page.getByRole('button', { name: 'Connect', exact: true }).click()
        await page.getByLabel('Display name', { exact: true }).waitFor()
        const server = page.getByRole('group', { name: 'MCP server URL', exact: true })
        expect(await server.getByRole('textbox').inputValue()).toBe(`${upstream.origin}/mcp`)
        expect(await server.getByRole('textbox').isDisabled()).toBe(true)
        // An MCP server takes no header keys, so the menu leaves them out.
        const methods = await signInMethods(page)
        expect(methods).not.toContain('API keys in headers')
        expect(
          await page.getByRole('button', { name: 'Clear the selection', exact: true }).count(),
        ).toBe(0)
        if (method === 'anonymous') {
          expect(upstream.counts.discovery).toBeGreaterThan(0)
          expect(await page.getByText('No tools found', { exact: true }).count()).toBe(0)
        }
        if (method === 'oauth') {
          for (const [label, value] of [
            ['Authorization URL', `${upstream.origin}/authorize`],
            ['Token URL', `${upstream.origin}/token`],
          ]) {
            expect(await page.getByRole('textbox', { name: label, exact: true }).inputValue()).toBe(
              value,
            )
          }
          // Discovered endpoints stay locked; discovered scopes can be overridden.
          for (const label of ['Authorization URL', 'Token URL']) {
            const field = page.getByRole('textbox', { name: label, exact: true })
            await field.waitFor()
            expect(await field.isDisabled()).toBe(true)
          }
          expect(await page.getByLabel('Client ID', { exact: true }).inputValue()).toBe('')
          expect('consents' in upstream.counts && upstream.counts.consents).toBe(0)
        } else if (method === 'bearer') {
          expect(methods).toContain('Bearer token')
          expect(methods).not.toContain('No authentication')
          expect(methods).not.toContain('OAuth authorization code')
        }
        await confirmConfiguration(page, method === 'bearer' ? 'Bearer token' : undefined)
        if (method === 'anonymous') await page.getByText('1 tool', { exact: true }).waitFor()
      })
      if (method === 'bearer')
        await step('A refused token returns review without exposing the token', async () => {
          await readTools(page, 'wrong-token')
          await confirmConfiguration(page, 'Bearer token')
          await readTools(page, secret)
          await acceptTools(page)
          await accountQuestion(page).waitFor()
          expect(await page.content()).not.toContain(secret)
        })
      await step(
        'Change the URL and acquire fresh suggestions without forwarding credentials',
        async () => {
          // A new attempt starts from the add page; the old one simply expires.
          await page.goto(`/workspaces/${workspace}/providers/new`)
          await openCustomSource(page)
          await page.getByLabel('MCP server URL', { exact: true }).fill(`${replacement.origin}/mcp`)
          await page.getByRole('button', { name: 'Connect', exact: true }).click()
          await page.getByLabel('Display name', { exact: true }).waitFor()
          expect(
            await page.getByRole('textbox', { name: 'MCP server URL', exact: true }).inputValue(),
          ).toBe(`${replacement.origin}/mcp`)
          expect(replacement.counts.credentialRequests).toBe(0)
          expect(replacement.counts.discovery).toBe(0)
        },
      )
    })
  },
)

test('Update completes protected MCP discovery without replacing the shared account', async (context) => {
  const secret = `update-mcp-${crypto.randomUUID()}`
  const upstream = await bearerMcpServer(context, secret)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let providerPath = ''
    await step('Add the protected catalog with one shared account', async () => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `MCP Update ${crypto.randomUUID()}`,
      )
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('protected')
      await confirmConfiguration(page, 'Bearer token')
      await readTools(page, secret)
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      providerPath = new URL(page.url()).pathname
    })
    await step('Review and authorize only discovery for this linked update', async () => {
      await page.goto(`${providerPath}/update`)
      await page.getByRole('button', { name: 'Read tools', exact: true }).click()
      await page.getByLabel('Token', { exact: true }).fill(secret)
      await page.getByRole('button', { name: 'List tools', exact: true }).click()
      await page.getByRole('radio', { name: 'Keep the current account', exact: true }).check()
      // Keeping the shared account needs no consent, so Save publishes at once.
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
      await page.goto(providerPath)
      await waitForProviderStatus(page, { path: providerPath }, 'Ready', {
        available: 1,
        total: 1,
      })
      expect(await page.content()).not.toContain(secret)
    })
  })
})
