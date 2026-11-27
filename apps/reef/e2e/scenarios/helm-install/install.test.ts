import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { authenticatedTarget, signIn } from '../../support/actors.ts'
import { withBrowserSession } from '../../support/browser-session.ts'
import { listen, openApiDocument } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspaceViaUi,
  programRunner,
  type RunUpdate,
} from '../../support/providers.ts'

// Everything here goes through the Install's public hosts, as a user and an MCP
// client would: the chart's HTTPRoutes, its Gateway, and its TLS. Running the
// Program proves that the Run Executor pods work.
test('An Install from the chart signs a user in, adds a provider and runs a program', async (context) => {
  const { owner } = authenticatedTarget().authentication
  let calls = 0
  const upstream = await listen(
    context,
    createServer((request, response) => {
      if (request.method !== 'GET' || request.url !== '/getForecast') {
        response.writeHead(404).end()
        return
      }
      calls++
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"forecast":"sunny"}')
    }),
  )
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      let workspace = ''
      await step('Sign in through the identity provider', async () => {
        await signIn(page, owner)
      })
      await step('Create a workspace and add a provider', async () => {
        workspace = await createWorkspaceViaUi(page, `Install ${crypto.randomUUID()}`)
        await addProviderFromFile(
          page,
          workspace,
          // A GET tool runs without approval by default.
          openApiDocument(
            'Weather',
            { getForecast: { summary: 'Read the forecast' } },
            { server: upstream.origin },
          ),
          'weather',
        )
      })
      await step('Run a program that calls the provider, over MCP', async () => {
        const run = programRunner(page, target, workspace)
        const started = await run.exec('return (await coral.providers.weather.getForecast()).data;')
        expect(started.isError).not.toBe(true)
        let update: RunUpdate = started.structuredContent
        for (let pages = 0; pages < 10 && (update.state === 'running' || update.has_more); pages++)
          update = (await run.wait(update)).structuredContent
        expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
        expect(update.has_more).toBe(false)
        expect(update.final_value).toEqual({ kind: 'value', value: { forecast: 'sunny' } })
        expect(calls).toBe(1)
      })
    },
    { actor: owner.label, label: 'Install journey', purpose: 'interaction' },
  )
})
