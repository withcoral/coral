import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { ToolAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { inspectRunEvents, managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  addedProviders,
  addProviderFromFile,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  setToolPolicy,
  openProviderSettings,
  programRunner,
  type RunUpdate,
  setProviderEnabled,
  waitForPublication,
} from '../../../support/providers.ts'

test('Workspace Provider Add, MCP execution, policy, Update and lifecycle use one identity', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  let calls = 0
  const upstream = await listen(
    context,
    createServer((request, response) => {
      if (request.url !== '/getForecast') {
        response.writeHead(404).end()
        return
      }
      calls++
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"forecast":"sunny"}')
    }),
  )
  const document = (title: string) =>
    openApiDocument(
      title,
      { getForecast: { summary: 'Read the forecast', method: 'post' } },
      { server: upstream.origin },
    )
  const api = managementClients(actor.accessToken)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      let workspace = ''
      let providerPath = ''
      let providerName = ''
      let secondProvider = ''
      let firstRun: RunUpdate
      let run: ReturnType<typeof programRunner>
      const policy = async (path: string, state: 'Always allow' | 'Disabled') => {
        await page.goto(path)
        await setToolPolicy(page, 'getForecast', state)
      }
      const execute = async (namespace: string) => {
        const result = await run.exec(
          `return (await coral.providers.${namespace}.getForecast()).data;`,
        )
        expect(result.isError).not.toBe(true)
        let update = result.structuredContent
        const id = update.run_id
        const events = [...update.events]
        for (
          let pages = 0;
          pages < 10 && (update.state === 'running' || update.has_more);
          pages++
        ) {
          update = (await run.wait(update)).structuredContent
          expect(update.run_id).toBe(id)
          events.push(...update.events)
        }
        expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
        expect(update.has_more).toBe(false)
        expect(update.final_value).toEqual({ kind: 'value', value: { forecast: 'sunny' } })
        const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${update.run_id}`)
        expect(history.filter((event) => event.payload.case === 'invocationStarted')).toHaveLength(
          1,
        )
        expect(events).toEqual([])
        return update
      }
      await step(
        'Add a credential-free provider through Reef and enable its exact tool',
        async () => {
          workspace = await createWorkspace(
            actor.accessToken,
            `Provider journey ${crypto.randomUUID()}`,
          )
          providerPath = await addProviderFromFile(page, workspace, document('Weather'), 'weather')
          const providers = (
            await api.providers.listProviders({ parent: `workspaces/${workspace}` })
          ).providers
          expect(providers).toHaveLength(2)
          const [added] = addedProviders(providers)
          providerName = added.name
          expect(providerPath).toBe(`/${providerName}`)
          expect(added.namespace).toBe('weather')
          await policy(providerPath, 'Always allow')
          run = programRunner(page, target, workspace, {
            accessToken: await mcpToken(page, workspace),
          })
        },
      )
      await step('Discover and execute through a real Workspace MCP session', async () => {
        const description = await run.describe('coral.providers.weather.getForecast')
        expect(description.isError).not.toBe(true)
        expect(JSON.stringify(description)).toContain('coral.providers.weather.getForecast')
        firstRun = await execute('weather')
        expect(calls).toBe(1)
      })
      await step(
        'A second provider from the same source takes its own name and owns independent policy and identity',
        async () => {
          const taken = page.getByText(
            'Another provider uses this name. Enter a different display name.',
            { exact: true },
          )
          await page.goto(`/workspaces/${workspace}/providers/new`)
          await chooseFile(page, 'otherWeather.json', document('Weather'))
          await page.getByRole('button', { name: 'Read', exact: true }).click()
          await taken.waitFor()
          await page.getByLabel('Display name', { exact: true }).fill('Other weather')
          await taken.waitFor({ state: 'detached' })
          await page.getByLabel('Namespace', { exact: true }).fill('otherWeather')
          await confirmConfiguration(page)
          await acceptTools(page)
          await page.getByRole('button', { name: 'Add provider', exact: true }).click()
          await waitForPublication(page)
          secondProvider = new URL(page.url()).pathname
          expect(secondProvider).not.toBe(providerPath)
          await policy(secondProvider, 'Always allow')
          await policy(providerPath, 'Disabled')
          expect((await run.describe('coral.providers.weather.getForecast')).isError).toBe(true)
          expect((await run.describe('coral.providers.otherWeather.getForecast')).isError).not.toBe(
            true,
          )
          await execute('otherWeather')
          expect(calls).toBe(2)
          await policy(providerPath, 'Always allow')
        },
      )
      await step(
        'Rename the provider and keep that name when updating its pinned catalog',
        async () => {
          await page.goto(`${providerPath}/settings`)
          await page.getByRole('button', { name: 'Edit provider name', exact: true }).click()
          await page.getByLabel('Provider name', { exact: true }).fill('Forecasts')
          await page.getByRole('button', { name: 'Save', exact: true }).click()
          await page.getByRole('heading', { level: 2, name: 'Forecasts', exact: true }).waitFor()
          await page.goto(`${providerPath}/update`)
          await chooseFile(page, 'weather-updated.json', document('Updated weather'))
          await page.getByRole('button', { name: 'Read', exact: true }).click()
          await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
          expect(
            await page.getByRole('textbox', { name: 'Display name', exact: true }).count(),
          ).toBe(0)
          await page.getByRole('button', { name: 'Save', exact: true }).click()
          await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
          await page.getByRole('heading', { level: 2, name: 'Forecasts', exact: true }).waitFor()
          const updated = await api.providers.getProvider({ name: providerName })
          expect(updated.displayName).toBe('Forecasts')
          expect(updated.namespace).toBe('weather')
          expect(updated.name).toBe(providerName)
          await execute('weather')
          expect(calls).toBe(3)
        },
      )
      await step(
        'Disable blocks discovery and dispatch; enable restores the same provider',
        async () => {
          // Reef offers no control for this; an operator changes it through Coral.
          await setProviderEnabled(actor.accessToken, providerPath, false)
          expect((await run.describe('coral.providers.weather.getForecast')).isError).toBe(true)
          const blocked = await run.exec('return await coral.providers.weather.getForecast();')
          expect(blocked.structuredContent.state).toBe('failed')
          expect(calls).toBe(3)
          await setProviderEnabled(actor.accessToken, providerPath, true)
          await execute('weather')
          expect(calls).toBe(4)
        },
      )
      await step(
        'Remove and recreate releases the namespace without inheriting identity or policy',
        async () => {
          await openProviderSettings(page)
          await page.getByRole('button', { name: 'Remove provider', exact: true }).click()
          await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Remove provider', exact: true })
            .click()
          await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/providers`)
          expect((await page.request.get(providerPath)).status()).toBe(404)
          const retained = (await run.wait(firstRun)).structuredContent
          expect(retained.run_id).toBe(firstRun.run_id)
          expect(retained.state).toBe('succeeded')
          const recreated = await addProviderFromFile(
            page,
            workspace,
            document('Weather again'),
            'weather',
          )
          expect(recreated).not.toBe(providerPath)
          const { tools } = await api.policies.listToolAccess({
            parent: recreated.slice(1),
            pageSize: 100,
          })
          expect(tools).toHaveLength(1)
          expect(tools[0].accessState).toBe(ToolAccessState.APPROVAL_REQUIRED)
          expect(tools[0].explicit).toBe(false)
          expect(calls).toBe(4)
          expect(
            (await api.providers.listProviders({ parent: `workspaces/${workspace}` })).providers,
          ).toHaveLength(3)
          expect((await page.request.get(secondProvider)).status()).toBe(200)
        },
      )
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
