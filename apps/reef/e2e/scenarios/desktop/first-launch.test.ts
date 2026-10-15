import { existsSync } from 'node:fs'
import { createServer } from 'node:http'
import { join } from 'node:path'
import { expect, test } from 'vitest'

import { DESKTOP_DATA, loopbackFixtures } from '../../support/config.ts'
import { copyMcpEndpoint, desktopProfile, withDesktopApp } from '../../support/desktop-app.ts'
import { listen, openApiDocument } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspaceViaUi,
  observeRun,
  programRunner,
  type RunUpdate,
} from '../../support/providers.ts'

// Desktop's own wiring, end to end: its supervisor starts the runtime and Reef,
// and the journey runs in its window. Reef's local-mode rules stay in local-mode.
test('A fresh Desktop runs its first Program over the workspace MCP endpoint', async (context) => {
  const secret = `desktop-secret-${crypto.randomUUID()}`
  let calls = 0
  // Desktop's local destination policy reaches loopback services.
  const upstream = await listen(
    context,
    createServer((request, response) => {
      if (request.headers.authorization !== `Bearer ${secret}`) {
        response.writeHead(401).end()
        return
      }
      calls++
      response.setHeader('content-type', 'application/json')
      response.end(JSON.stringify({ entries: ['first'] }))
    }),
    loopbackFixtures,
  )
  const profile = await desktopProfile(context)
  const name = 'Desktop first launch'
  await withDesktopApp(context, profile, async ({ page, app, reef, target, step }) => {
    let workspace = ''
    let endpoint = ''
    await step('Open Reef in the window as the local user', async () => {
      await page
        .getByRole('button', { name: 'Open account menu for Local user', exact: true })
        .waitFor()
      // The data folder follows the app id, never the 0.x app's name.
      expect(existsSync(join(profile.appData, DESKTOP_DATA, 'runtime', 'config.toml'))).toBe(true)
      expect(existsSync(join(profile.appData, 'Coral'))).toBe(false)
    })
    await step('Create a workspace and copy its MCP endpoint', async () => {
      workspace = await createWorkspaceViaUi(page, name)
      endpoint = await copyMcpEndpoint(page, app, name)
      expect(new URL(endpoint).pathname).toBe(`/workspaces/${workspace}/mcp`)
    })
    await step('Add a provider with its shared account', async () => {
      await addProviderFromFile(
        page,
        workspace,
        openApiDocument(
          'Desktop ledger',
          { listEntries: { summary: 'List entries', method: 'post' } },
          {
            server: upstream.origin,
          },
        ),
        'ledger',
        { secret, authentication: 'Bearer token', account: 'shared' },
      )
    })
    const runner = programRunner(page, { ...target, mcpURL: new URL(endpoint).origin }, workspace, {
      taskIntent: 'Desktop first goal',
      waitMs: 1000,
    })
    let waiting: RunUpdate | undefined
    await step('Run a Program from an MCP client with no credential', async () => {
      const started = await runner.exec(
        'return (await coral.providers.ledger.listEntries({})).data;',
      )
      expect(started.isError).not.toBe(true)
      waiting = started.structuredContent
      for (let pages = 0; pages < 10 && !waiting.required_actions.length; pages++)
        waiting = (await runner.wait(waiting)).structuredContent
      expect(waiting.required_actions).toHaveLength(1)
      expect(calls).toBe(0)
    })
    await step('Approve the call on its run status page in the window', async () => {
      const [action] = waiting!.required_actions
      expect(new URL(action.run_status_url).origin).toBe(reef)
      await page.goto(action.run_status_url)
      await page
        .getByRole('link', { name: 'Review coral.providers.ledger.listEntries', exact: true })
        .click()
      await page.getByRole('button', { name: 'Approve this call', exact: true }).click()
      await page.getByText('Approved', { exact: true }).waitFor()
      const done = await observeRun(runner, await runner.wait(waiting!))
      expect(done.update.final_value).toEqual({ kind: 'value', value: { entries: ['first'] } })
      expect(calls).toBe(1)
    })
    await step('See the run in the workspace activity', async () => {
      await page.goto(`/workspaces/${workspace}/activity`)
      await page.getByRole('link', { name: 'Desktop first goal', exact: true }).waitFor()
    })
  })
})
