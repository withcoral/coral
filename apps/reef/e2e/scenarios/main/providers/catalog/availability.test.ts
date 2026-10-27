import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  openTool,
  programRunner,
  waitForProviderStatus,
} from '../../../../support/providers.ts'

test('A Tool Coral cannot call stays in the table as Unavailable', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    let workspace = ''
    await step('Add a Provider with one Tool Coral cannot call', async () => {
      workspace = await createWorkspace(actor.accessToken, `Unavailable ${crypto.randomUUID()}`)
      await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Mixed', {
          works: { summary: 'Works' },
          broken: { summary: 'Has no responses', broken: true },
        }),
        'mixed',
      )
    })
    await step('The table keeps it and its drawer says why', async () => {
      const drawer = await openTool(page, 'broken')
      await drawer.getByText('Unavailable', { exact: true }).waitFor()
      await drawer.getByText('responses', { exact: false }).first().waitFor()
      await drawer.getByText('coral.providers.mixed.broken', { exact: true }).waitFor()
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      // The provider stays Ready: its heading counts both tools and marks
      // only the broken one Unavailable.
      await waitForProviderStatus(page, 'Mixed', 'Ready', { available: 1, total: 2 })
    })
    await step('A Program run against it fails safely', async () => {
      const run = programRunner(page, target, workspace)
      const unavailable = await run('return await coral.providers.mixed.broken({});')
      expect(unavailable).not.toContain('succeeded')
      expect(unavailable).not.toContain('panic')
    })
  })
})
