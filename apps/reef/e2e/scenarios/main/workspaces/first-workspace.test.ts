import { expect, test } from 'vitest'
import { loginActor, mcpToken, newUser } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { observeRun, programRunner } from '../../../support/providers.ts'

test('Someone with no workspace lands on a page that creates their first one', async (context) => {
  const carol = await loginActor(context, await newUser(context, 'Carol'))
  const name = `First workspace ${crypto.randomUUID()}`
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const content = page.getByRole('main')
      let id = ''
      await step('Land on the create-workspace page', async () => {
        await page.goto('/')
        await content.getByText('No workspaces yet', { exact: true }).waitFor()
      })
      await step('Create the first workspace from the page and open it', async () => {
        // Hydration first: a click that lands before it opens nothing.
        await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
        await content.getByRole('button', { name: 'Create workspace', exact: true }).click()
        await page.getByLabel('Workspace name', { exact: true }).fill(name)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create workspace', exact: true })
          .click()
        await page.waitForURL((url) => /^\/workspaces\/[0-9a-f-]{36}$/.test(url.pathname))
        await page
          .getByRole('navigation', { name: 'breadcrumb', exact: true })
          .getByText(name, { exact: true })
          .waitFor()
        id = new URL(page.url()).pathname.split('/')[2]
      })
      const token = await mcpToken(page, id)
      await step('Home opens the workspace Activity once a workspace exists', async () => {
        await page.goto('/')
        await page.waitForURL((url) => /^\/workspaces\/[0-9a-f-]{36}\/activity$/.test(url.pathname))
        await content.getByRole('heading', { name: 'Your recent activity', exact: true }).waitFor()
        await content.getByText('No activity yet', { exact: true }).waitFor()
      })
      await step(
        'Copy the MCP URL from the empty Activity, where sign-in picks the workspace',
        async () => {
          await page.evaluate(() => {
            Object.defineProperty(navigator, 'clipboard', {
              configurable: true,
              value: {
                writeText: async (text: string) => {
                  ;(window as unknown as { copied: string }).copied = text
                },
              },
            })
          })
          await content.getByRole('button', { name: 'Copy MCP URL', exact: true }).click()
          await content.getByRole('button', { name: 'Copy MCP URL: copied', exact: true }).waitFor()
          expect(await page.evaluate(() => (window as unknown as { copied: string }).copied)).toBe(
            `${target.mcpURL}/mcp`,
          )
        },
      )
      await step('The first MCP call replaces the empty state without a reload', async () => {
        const runner = programRunner(page, target, id, {
          accessToken: token,
          taskIntent: 'First MCP goal',
        })
        await observeRun(runner, await runner.exec('return 1;'))
        await content
          .getByRole('link', { name: 'First MCP goal', exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await content.getByText('No activity yet', { exact: true }).count()).toBe(0)
        expect(
          await content.getByRole('button', { name: 'Copy MCP URL', exact: true }).count(),
        ).toBe(0)
      })
    },
    { actor: carol.label, storageState: carol.storageState },
  )
})
