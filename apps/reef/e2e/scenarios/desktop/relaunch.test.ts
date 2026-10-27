import { expect, test } from 'vitest'

import {
  copyMcpEndpoint,
  desktopProfile,
  desktopProcesses,
  processExists,
  withDesktopApp,
} from '../../support/desktop-app.ts'
import { tcpListeners } from '../../support/os-processes.ts'
import { openApiDocument } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspaceViaUi,
  programRunner,
} from '../../support/providers.ts'

test('Quit stops Desktop’s processes, and a relaunch keeps the workspace at the same addresses', async (context) => {
  const profile = await desktopProfile(context)
  const name = 'Desktop relaunch'
  let reefOrigin = ''
  let endpoint = ''
  let children: number[] = []
  const quit = await withDesktopApp(
    context,
    profile,
    async ({ page, app, reef, step }) => {
      reefOrigin = reef
      await step('Create a workspace with a provider', async () => {
        const workspace = await createWorkspaceViaUi(page, name)
        await addProviderFromFile(
          page,
          workspace,
          openApiDocument('Desktop records', { lookup: { summary: 'Look up a record' } }),
          'records',
        )
        endpoint = await copyMcpEndpoint(page, app, name)
      })
      const running = await desktopProcesses(app)
      expect(running.runtime).toHaveLength(1)
      expect(running.reef).toHaveLength(1)
      children = [...running.runtime, ...running.reef]
      await step('The runtime and Reef listen only on loopback', async () => {
        // A listener on another address would also raise a firewall prompt
        // on Windows.
        const listeners = await tcpListeners(children)
        expect(listeners.map(({ port }) => port)).toEqual(
          expect.arrayContaining([
            Number(new URL(reef).port),
            Number(new URL(endpoint).port),
            // The runtime's fixed Provider OAuth callback.
            8779,
          ]),
        )
        for (const listener of listeners)
          expect(listener.address, `port ${listener.port}`).toBe('127.0.0.1')
      })
      // The session ends with Quit.
    },
    { label: 'First launch' },
  )
  // Quit finished by itself, not through the harness's fallback kill.
  expect(quit).toEqual({ code: 0, signal: null })
  for (const pid of children) expect(processExists(pid), `process ${pid}`).toBe(false)

  const relaunched = await withDesktopApp(
    context,
    profile,
    async ({ page, app, reef, target, step }) => {
      await step('Reopen the workspace at the same Reef and MCP addresses', async () => {
        expect(reef).toBe(reefOrigin)
        await page.goto('/')
        expect(await copyMcpEndpoint(page, app, name)).toBe(endpoint)
      })
      await step('Find the provider’s tool over the same MCP endpoint', async () => {
        const workspace = new URL(endpoint).pathname.split('/')[2]
        const runner = programRunner(
          page,
          { ...target, mcpURL: new URL(endpoint).origin },
          workspace,
          { taskIntent: 'Desktop relaunch goal' },
        )
        const found = await runner.search('Look up a record')
        expect(found.isError).not.toBe(true)
        expect(found.structuredContent.hits.map((hit) => hit.call_path)).toContain(
          'coral.providers.records.lookup',
        )
      })
    },
    { label: 'Relaunch' },
  )
  expect(relaunched).toEqual({ code: 0, signal: null })
})
