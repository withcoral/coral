import { expect, test } from 'vitest'

import {
  copyMcpEndpoint,
  desktopProfile,
  desktopProcesses,
  processExists,
  withDesktopApp,
} from '../../support/desktop-app.ts'
import { openApiDocument } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspaceViaUi,
  programRunner,
} from '../../support/providers.ts'

test('Desktop shows a runtime crash, then restarts Coral at the same addresses', async (context) => {
  const profile = await desktopProfile(context)
  const name = 'Desktop crash'
  await withDesktopApp(context, profile, async ({ page, app, reef, target, step }) => {
    let endpoint = ''
    const search = (taskIntent: string) => {
      const workspace = new URL(endpoint).pathname.split('/')[2]
      const runner = programRunner(
        page,
        { ...target, mcpURL: new URL(endpoint).origin },
        workspace,
        { taskIntent },
      )
      return runner.search('Look up a record')
    }
    await step('Create a workspace with a provider', async () => {
      const workspace = await createWorkspaceViaUi(page, name)
      await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Desktop crash records', { lookup: { summary: 'Look up a record' } }),
        'records',
      )
      endpoint = await copyMcpEndpoint(page, app, name)
    })
    await step('Run a Program, which starts a local Run Executor', async () => {
      expect((await search('Desktop crash first goal')).isError).not.toBe(true)
    })
    const before = await desktopProcesses(app)
    expect(before.runtime).toHaveLength(1)
    expect(before.runExecutors).toHaveLength(1)
    await step('Kill the runtime, and see the window report it', async () => {
      process.kill(before.runtime[0], 'SIGKILL')
      await page.getByRole('heading', { name: 'Coral stopped', exact: true }).waitFor()
    })
    await step('The local Run Executor exits with the runtime', async () => {
      await expect
        .poll(() => processExists(before.runExecutors[0]), { timeout: 30_000 })
        .toBe(false)
    })
    await step('Coral restarts by itself and Reef returns at the same address', async () => {
      await page.waitForURL((url) => url.origin === reef, { timeout: 60_000 })
      await page
        .getByRole('button', { name: 'Open account menu for Local user', exact: true })
        .waitFor()
      const after = await desktopProcesses(app)
      expect(after.runtime).toHaveLength(1)
      expect(after.runtime[0]).not.toBe(before.runtime[0])
      expect(after.reef).toHaveLength(1)
      expect(after.reef[0]).not.toBe(before.reef[0])
    })
    await step('The same MCP endpoint answers', async () => {
      const found = await search('Desktop crash goal')
      expect(found.isError).not.toBe(true)
      expect(found.structuredContent.hits.map((hit) => hit.call_path)).toContain(
        'coral.providers.records.lookup',
      )
    })
  })
})

// Runs in flight at a crash need the runtime's startup recovery first.
test.todo('A Run in flight when the runtime crashes reaches its outcome after the restart (#347)')
