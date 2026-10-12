import { expect, test } from 'vitest'
import { mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'
import { copiedBy } from '../../../support/clipboard.ts'
import { createWorkspace, programRunner } from '../../../support/providers.ts'

test('Workspace Overview starts each part, offers MCP setup, hands off a new Agent key, and follows your live activity', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const workspace = await createWorkspace(actor.accessToken, `Overview ${crypto.randomUUID()}`)
    const overview = `/workspaces/${workspace}`
    const content = page.getByRole('main')
    const table = page.getByRole('table', {
      name: 'Activity records',
      includeHidden: true,
    })
    const setup = page.getByRole('region', { name: 'Connect your MCP client', exact: true })
    const reload = async () => {
      await page.reload()
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await content.getByRole('heading', { name: 'Providers', exact: true, level: 2 }).waitFor()
    }
    let key = ''
    await step('An empty Overview offers the action that fills each part', async () => {
      await page.goto(overview)
      // Hydration first: a click that lands before it opens nothing.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await content.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
      await table.getByText('No activity yet', { exact: true }).waitFor()
    })
    await step('The Overview sets up Claude Code with sign-in', async () => {
      const add = setup.getByRole('button', { name: 'Copy Claude Code command', exact: true })
      expect(await copiedBy(page, add)).toBe(
        `claude mcp add --transport http coral ${target.mcpURL}/workspaces/${workspace}/mcp`,
      )
      const login = setup.getByRole('button', {
        name: 'Copy Claude Code sign-in command',
        exact: true,
      })
      expect(await copiedBy(page, login)).toBe('claude mcp login coral')
    })
    await step('Create an Agent from the Overview and keep its copied key', async () => {
      await content.getByRole('button', { name: 'Create agent', exact: true }).click()
      await page.getByLabel('Agent name', { exact: true }).fill('Overview worker')
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create agent', exact: true })
        .click()
      key = await copyNewAgentKey(page)
      expect(new URL(page.url()).pathname).toBe(overview)
      await content.getByRole('link', { name: 'Overview worker', exact: true }).waitFor()
      expect(await content.getByRole('button', { name: 'Create agent', exact: true }).count()).toBe(
        0,
      )
    })
    await step('The copied key runs a program as the Agent', async () => {
      const agent = programRunner(page, target, workspace, {
        accessToken: key,
        taskIntent: 'Agent goal',
      })
      expect((await agent.exec('return 42;')).isError).not.toBe(true)
    })
    await step('Your own program arrives in the Overview without a reload', async () => {
      const own = programRunner(page, target, workspace, {
        accessToken: await mcpToken(page, workspace),
        taskIntent: 'Overview goal',
      })
      expect((await own.exec('return 42;')).isError).not.toBe(true)
      await table
        .getByRole('link', { name: 'Overview goal', exact: true })
        .waitFor({ timeout: 10_000 })
      // The Overview lists your own work; an Agent's work stays on its page.
      expect(await table.getByRole('link', { name: 'Agent goal', exact: true }).count()).toBe(0)
      expect(new URL(page.url()).pathname).toBe(overview)
    })
    await step('After your first call, the Overview keeps its setup', async () => {
      await reload()
      await table.getByRole('link', { name: 'Overview goal', exact: true }).waitFor()
      await setup.waitFor()
    })
    await step('Its details open on the Activity page, as Reveal record does', async () => {
      const activity = `${overview}/activity`
      await table.getByRole('link', { name: 'Overview goal', exact: true }).click()
      await page.waitForURL((url) => url.searchParams.has('record'))
      expect(new URL(page.url()).pathname).toBe(activity)
      const details = page.getByRole('dialog', { name: 'Activity details', exact: true })
      await details.getByText('Overview goal', { exact: true }).waitFor()
      await details.getByRole('button', { name: 'Close details', exact: true }).click()
      await details.waitFor({ state: 'hidden' })
      expect(new URL(page.url()).pathname).toBe(activity)
    })
  })
})
