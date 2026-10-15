import { expect, test } from 'vitest'
import { mcpToken, withOwnerSession } from '../../../support/actors.ts'
import {
  breadcrumb,
  createWorkspace,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'

const wrap = `export default async function wrap(
  ctx: CoralMiddleware.Invocation,
  next: CoralMiddleware.Next,
) {
  return { wrapped: await next() }
}
`

test('An owner reviews saved middleware, enables it, and sees it wrap the next execution', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    let workspace = ''
    let execute: ReturnType<typeof programRunner> | undefined
    await step('Save middleware through MCP; Coral leaves it disabled', async () => {
      workspace = await createWorkspace(actor.accessToken, `Middleware ${crypto.randomUUID()}`)
      execute = programRunner(page, target, workspace, {
        accessToken: await mcpToken(page, workspace),
        taskIntent: 'Add execution middleware',
      })
      await execute.listTools()
      const saved = await execute.callTool('save_middleware', {
        task_id: execute.taskId(),
        intent: 'Wrap each execution result',
        name: 'wrap',
        description: 'Wrap each execution result in an object.',
        source: wrap,
      })
      expect(saved.result?.isError).not.toBe(true)
      expect(saved.result?.structuredContent).toMatchObject({ name: 'wrap', enabled: false })
    })
    await step('Open the middleware page and read the source', async () => {
      await page.goto(`/workspaces/${workspace}/middleware`)
      await breadcrumb(page, 'Middleware').waitFor()
      const table = page.getByRole('table', { name: 'Middleware', exact: true })
      await table.getByRole('link', { name: 'wrap', exact: true }).click()
      const dialog = page.getByRole('dialog', { name: 'wrap', exact: true })
      await dialog.getByText('To change the code, use save_middleware', { exact: false }).waitFor()
      await expect.poll(() => dialog.locator('pre').first().textContent()).toContain('wrapped')
      await page.keyboard.press('Escape')
      await dialog.waitFor({ state: 'detached' })
    })
    await step('Choose executions, then enable the middleware', async () => {
      // The checkbox shows saved scopes, so it changes once the save returns.
      const executions = page.getByRole('checkbox', { name: 'Executions for wrap', exact: true })
      await executions.click()
      await page.getByText('Scopes saved.', { exact: false }).waitFor()
      await expect.poll(() => executions.isChecked()).toBe(true)
      const enable = page.getByRole('switch', { name: 'Enable wrap', exact: true })
      expect(await enable.getAttribute('aria-checked')).toBe('false')
      await enable.click()
      await page.getByText('Middleware enabled.', { exact: false }).waitFor()
      await expect.poll(() => enable.getAttribute('aria-checked')).toBe('true')
    })
    await step('Run a program; the enabled middleware wraps its final value', async () => {
      const observed = await observeRun(execute!, await execute!.exec('return 1;'))
      expect(observed.update.final_value).toEqual({ kind: 'value', value: { wrapped: 1 } })
      const tree = new URLSearchParams()
      tree.append('expand', `workspaces/${workspace}/tasks/${execute!.taskId()}`)
      tree.append('expand', `workspaces/${workspace}/runs/${observed.update.run_id}`)
      await page.goto(`/workspaces/${workspace}/activity?${tree}`)
      const row = page
        .getByRole('table', { name: 'Activity records', exact: true })
        .getByRole('row')
        .filter({ has: page.getByRole('link', { name: 'wrap around execution', exact: true }) })
      await row.getByRole('img', { name: 'exit phase', exact: true }).waitFor()
      await row.getByRole('img', { name: 'succeeded', exact: true }).waitFor()
    })
  })
})
