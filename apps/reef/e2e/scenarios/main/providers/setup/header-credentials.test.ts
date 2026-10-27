import { createServer, type IncomingMessage } from 'node:http'
import type { Page } from 'playwright'
import { expect, inject, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { listen, openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  chooseSignIn,
  confirmConfiguration,
  createWorkspace,
  enableTool,
  observeRun,
  programRunner,
  type SignInMethod,
  waitForPublication,
  openDialog,
} from '../../../../support/providers.ts'

const FIRST = 'first-secret-canary'
const SECOND = 'second-secret-canary'

/** Names each credential in a header or parameter table: one row per [name, label]. */
async function describeBindings(
  page: Page,
  table: 'Headers' | 'Query parameters',
  noun: 'Header' | 'Parameter',
  bindings: [string, string][],
) {
  const rows = page
    .getByRole('table', { name: table, exact: true })
    .getByRole('row')
    .filter({ has: page.getByRole('textbox', { name: noun, exact: true }) })
  const add = page
    .getByRole('table', { name: table, exact: true })
    .getByRole('button', { name: `Add ${noun.toLowerCase()}`, exact: true })
  for (const [index, [name, label]] of bindings.entries()) {
    await add.click()
    const row = rows.nth(index)
    await row.getByRole('textbox', { name: noun, exact: true }).fill(name)
    await row.getByRole('textbox', { name: 'Credential label', exact: true }).fill(label)
  }
  return rows
}

interface Placement {
  name: string
  method: SignInMethod
  /** Describes where the two credentials go, labelled "First" and "Second". */
  describe: (page: Page) => Promise<void>
  /** Whether the service received both credentials where they belong. */
  received: (request: IncomingMessage) => boolean
}

// The shapes of Datadog (two headers and a fixed one), Jira (HTTP Basic) and
// Trello (two query parameters). Reef Vitest owns the form rules and preview.
const placements: Placement[] = [
  {
    name: 'two API keys and a fixed value in headers',
    method: 'API keys in headers',
    describe: async (page) => {
      const rows = await describeBindings(page, 'Headers', 'Header', [
        ['dd-api-key', 'First'],
        ['dd-application-key', 'Second'],
      ])
      await page
        .getByRole('table', { name: 'Headers', exact: true })
        .getByRole('button', { name: 'Add header', exact: true })
        .click()
      const fixed = rows.nth(2)
      await fixed.getByRole('textbox', { name: 'Header', exact: true }).fill('dd-api-version')
      await fixed.getByRole('button', { name: 'Type', exact: true }).click()
      await page.getByRole('menuitemradio', { name: 'Fixed value', exact: true }).click()
      await fixed.getByRole('textbox', { name: 'Value', exact: true }).fill('2024-01')
    },
    received: ({ headers }) =>
      headers['dd-api-key'] === FIRST &&
      headers['dd-application-key'] === SECOND &&
      headers['dd-api-version'] === '2024-01',
  },
  {
    name: 'an email and API token as HTTP Basic',
    method: 'Username and password',
    describe: async (page) => {
      const table = page.getByRole('table', { name: 'Username and password', exact: true })
      for (const [part, label] of [
        ['Username', 'First'],
        ['Password', 'Second'],
      ]) {
        const row = table.getByRole('row').filter({ hasText: part })
        await row.getByRole('button', { name: `${part} type`, exact: true }).click()
        await page.getByRole('menuitemradio', { name: 'Credential', exact: true }).click()
        await page.getByRole('menu').waitFor({ state: 'detached' })
        await row.getByRole('textbox', { name: `${part} credential`, exact: true }).fill(label)
      }
    },
    received: ({ headers }) =>
      headers.authorization === `Basic ${Buffer.from(`${FIRST}:${SECOND}`).toString('base64')}`,
  },
  {
    name: 'a key and token in the query',
    method: 'API keys in the query',
    describe: async (page) => {
      await describeBindings(page, 'Query parameters', 'Parameter', [
        ['key', 'First'],
        ['token', 'Second'],
      ])
    },
    received: ({ url }) => {
      const query = new URL(url ?? '/', 'http://upstream').searchParams
      return query.get('key') === FIRST && query.get('token') === SECOND
    },
  },
]

test.for(placements)('Set up $name by hand, then call a tool', async (placement, context) => {
  const { target } = inject('reel')
  const accepted: boolean[] = []
  const upstream = await listen(
    context,
    createServer((request, response) => {
      accepted.push(placement.received(request))
      if (!accepted.at(-1)) {
        response.writeHead(401).end()
        return
      }
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"id":"me"}')
    }),
  )
  const document = openApiDocument(
    'Keys',
    { me: { summary: 'Who am I' } },
    { server: upstream.origin },
  )

  await withOwnerSession(context, async ({ page, step, actor }) => {
    let workspace = ''
    await step('Describe where each credential goes', async () => {
      workspace = await createWorkspace(actor.accessToken, `Credential keys ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'keys.json', document)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor()
      await chooseSignIn(page, placement.method)
      await placement.describe(page)
      await page.getByLabel('Namespace', { exact: true }).fill('keys')
      await confirmConfiguration(page)
      await acceptTools(page)
    })

    await step('Connect one shared account with a box for each credential', async () => {
      await chooseAccount(page, 'shared')
      const dialog = await openDialog(
        page,
        page.getByRole('button', { name: 'Connect the shared account', exact: true }),
      )
      await dialog.getByLabel('First', { exact: true }).fill(FIRST)
      await dialog.getByLabel('Second', { exact: true }).fill(SECOND)
      await dialog.getByRole('button', { name: 'Save and share the account', exact: true }).click()
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      await enableTool(page, 'me')
    })

    await step(
      'The call carries both credentials, and neither reaches the run or the page',
      async () => {
        const run = programRunner(page, target, workspace, { waitMs: 3000 })
        const observed = await observeRun(
          run,
          await run.exec('return await coral.providers.keys.me({});'),
        )
        expect(accepted).toEqual([true])
        for (const secret of [FIRST, SECOND]) {
          expect(observed.responses.join('')).not.toContain(secret)
          expect(await page.content()).not.toContain(secret)
        }
      },
    )
  })
})
