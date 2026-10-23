import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { breadcrumb, createWorkspace, programRunner } from '../../../support/providers.ts'

test("An agent's page lists its latest work live and opens the complete Activity from there", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const workspace = await createWorkspace(actor.accessToken, `Agent page ${crypto.randomUUID()}`)
    const issued = await managementClients(actor.accessToken).agents.createAgent({
      parent: `workspaces/${workspace}`,
      displayName: 'Dashboard worker',
      requestId: crypto.randomUUID(),
    })
    const agentPath = `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}`
    const work = async (intent: string) => {
      const runner = programRunner(page, target, workspace, {
        accessToken: issued.credential,
        taskIntent: intent,
      })
      expect((await runner.exec('return 1;')).structuredContent.state).toBe('succeeded')
      return `workspaces/${workspace}/tasks/${runner.taskId()}`
    }
    const recent = page.getByRole('region', { name: 'Recent activity', exact: true })
    const backToAgent = page.getByRole('link', { name: 'Back to agent', exact: true })
    for (const n of [1, 2, 3, 4]) await work(`Agent goal ${n}`)

    await step('Recent activity lists every goal of the first page', async () => {
      await page.goto(agentPath)
      // Hydration first: the live stream starts after it.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      for (const n of [4, 3, 2, 1])
        await recent.getByRole('link', { name: `Agent goal ${n}`, exact: true }).waitFor()
      // The whole first page shows, so no link leads to more.
      expect(
        await recent.getByRole('link', { name: 'View all activity', exact: true }).count(),
      ).toBe(0)
    })

    let latest = ''
    await step('New agent work reaches Recent activity without a reload', async () => {
      latest = await work('Agent goal 5')
      await recent.getByRole('link', { name: 'Agent goal 5', exact: true }).waitFor()
      // The first page has room for it, so the oldest goal stays.
      await recent.getByRole('link', { name: 'Agent goal 1', exact: true }).waitFor()
    })

    await step('A goal opened from Recent activity opens the complete Activity', async () => {
      await recent.getByRole('link', { name: 'Agent goal 5', exact: true }).click()
      await page.waitForURL((url) => url.searchParams.get('record') === latest)
      await breadcrumb(page, 'Activity').waitFor()
      await page.getByRole('button', { name: 'Close details', exact: true }).click()
      await page.waitForURL((url) => !url.searchParams.has('record'))
      await backToAgent.waitFor()
      expect(await recent.count()).toBe(0)
    })

    await step('Back to agent returns to Recent activity', async () => {
      await backToAgent.click()
      await page.waitForURL((url) => url.pathname === agentPath && !url.search)
      await recent.getByRole('link', { name: 'Agent goal 5', exact: true }).waitFor()
    })
  })
})
