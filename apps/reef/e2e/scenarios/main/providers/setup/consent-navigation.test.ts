import { createServer } from 'node:http'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { expect, inject, test } from 'vitest'
import { loginActor } from '../../../../support/actors.ts'
import { withBrowserSession } from '../../../../support/browser-session.ts'
import { listen, oauthApiServer } from '../../../../support/provider-fixtures.ts'
import { startStack } from '../../../../support/stack.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  createWorkspaceViaUi,
  waitForPublication,
  signInAndShareAccount,
} from '../../../../support/providers.ts'

test('Consent uses private HTTP independently of HTTPS-only server requests and rejects public HTTP issuance', async (context) => {
  const config = inject('reel')
  const bootstrap = inject('reelBootstrap')
  if (!config.tlsDirectory || config.fixtures?.advertisedHost !== 'sidecar')
    throw new Error('Run consent acceptance inside the Reel container')
  const runDir = join(config.runDir, config.target.name, 'consent-transport')
  await mkdir(runDir, { recursive: true })
  // An independent transport-policy control owns its own SQLite state.
  const stack = await startStack({
    ...bootstrap,
    postgresURL: undefined,
    runDir,
    authenticated: true,
    privateNetworkAccess: 'https',
  })
  context.onTestFinished(() => stack.stop())
  const target = { ...stack.target, authentication: stack.target.authentication! }
  const actor = await loginActor(context, target.authentication.owner, target)
  const upstream = await oauthApiServer(context, 'consent-transport-token')
  const secure = await listen(
    context,
    createServer((request, response) => upstream.server.emit('request', request, response)),
    {
      ...config.fixtures,
      tls: {
        certFile: join(config.tlsDirectory, 'trusted.pem'),
        keyFile: join(config.tlsDirectory, 'trusted.key'),
        caFile: join(config.tlsDirectory, 'trusted-ca.pem'),
      },
    },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      // A regression must fail without contacting an external or metadata service.
      let forbiddenNavigations = 0
      for (const pattern of ['http://public-consent.test/**', 'https://169.254.169.254/**']) {
        await page.route(pattern, (route) => {
          forbiddenNavigations++
          return route.abort()
        })
      }
      const workspace = await createWorkspaceViaUi(page, `Consent transport ${crypto.randomUUID()}`)
      let attempts = 0
      async function begin(authorizationUrl: string) {
        const document = JSON.parse(upstream.document('Consent').toString())
        document.servers = [{ url: secure.origin }]
        const flow = Object.values(document.components.securitySchemes)[0] as {
          flows: { authorizationCode: { authorizationUrl: string; tokenUrl: string } }
        }
        flow.flows.authorizationCode.authorizationUrl = authorizationUrl
        flow.flows.authorizationCode.tokenUrl = `${secure.origin}/token`
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await chooseFile(page, 'consent.json', Buffer.from(JSON.stringify(document)))
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        // Each attempt adds to one workspace, and Configuration refuses a name or namespace in use.
        await page.getByLabel('Display name', { exact: true }).fill(`Consent ${++attempts}`)
        await page.getByLabel('Namespace', { exact: true }).fill(`consent_${attempts}`)
        await confirmConfiguration(page)
        await acceptTools(page)
        await chooseAccount(page, 'shared')
        const resume = page.url()
        await signInAndShareAccount(page)
        return resume
      }
      await step(
        'Navigate and resume private HTTP consent while Coral exchanges only over verified HTTPS',
        async () => {
          const resume = await begin(`${upstream.origin}/authorize`)
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
          expect(page.url()).toMatch(/^http:\/\/sidecar:/)
          const initial = page.url()
          await page.goto(resume)
          await page.getByRole('button', { name: 'Continue sign-in', exact: true }).click()
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
          expect(page.url()).toBe(initial)
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
          // The sign-in answers the account question, so it adds the provider.
          await waitForPublication(page)
          expect(upstream.counts.tokens).toBe(1)
          expect(upstream.counts.consents).toBe(2)
        },
      )
      await step(
        'Reject public HTTP hostnames and HTTPS special literals before redirecting the browser',
        async () => {
          for (const url of [
            'http://public-consent.test/authorize',
            'https://169.254.169.254/authorize',
          ]) {
            await begin(url)
            await page
              .getByText('Coral cannot open this sign-in address.', { exact: false })
              .first()
              .waitFor()
            expect(new URL(page.url()).origin).toBe(target.baseURL)
            expect(forbiddenNavigations).toBe(0)
            expect(upstream.counts.consents).toBe(2)
            expect(upstream.counts.tokens).toBe(1)
          }
        },
      )
    },
    { target, actor: actor.label, storageState: actor.storageState },
  )
})
