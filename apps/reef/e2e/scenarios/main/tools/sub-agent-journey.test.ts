import { createServer } from 'node:http'
import type { Page } from 'playwright'
import { expect, test, type TestContext } from 'vitest'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { listen } from '../../../support/provider-fixtures.ts'
import {
  expectArrivalToast,
  observeRun,
  openDialog,
  programRunner,
} from '../../../support/providers.ts'
import { openSettings } from '../../../support/settings.ts'

// The Sub-agent across every process: an Organization Owner sets its model in Reef, a Workspace
// Owner consents to where it sends their data, a Program gets an answer from the model's own
// server, and a new base URL takes the consent back. The form, the dialog and the policy rules
// stay in Reef's tests and in `crates/coral-policy`.

const KEY = 'sk-reel-disposable'
const ANSWER = 'Dana owns account 42.'

/** A model server that speaks Chat Completions, and what reached it. */
async function modelServer(context: TestContext) {
  const requests: { path: string; authorization?: string; model: string; temperature?: number }[] =
    []
  const { origin } = await listen(
    context,
    createServer((request, response) => {
      const chunks: Buffer[] = []
      request.on('data', (chunk: Buffer) => chunks.push(chunk))
      request.on('end', () => {
        const { model, temperature } = JSON.parse(Buffer.concat(chunks).toString())
        requests.push({
          path: request.url!,
          authorization: request.headers.authorization,
          model,
          temperature,
        })
        response.writeHead(200, { 'content-type': 'application/json' }).end(
          JSON.stringify({
            model,
            choices: [{ message: { role: 'assistant', content: ANSWER }, finish_reason: 'stop' }],
          }),
        )
      })
    }),
  )
  return { origin, requests }
}

const save = (page: Page) =>
  expectArrivalToast(page, { flag: 'saved', title: 'Model saved' }, () =>
    page.getByRole('button', { name: 'Save', exact: true }).click(),
  )

const agentRow = (page: Page) =>
  page.getByRole('row').filter({
    has: page.getByRole('checkbox', { name: 'Select agent', exact: true }),
  })
/** Opens the Tools page and waits until `coral.agent` has the policy. */
async function expectAgentPolicy(page: Page, workspace: string, policy: 'Allow' | 'Block') {
  const response = await page.goto(`/${workspace}/tools`)
  expect(response?.status()).toBe(200)
  await agentRow(page).getByRole('radio', { name: policy, exact: true, checked: true }).waitFor()
}

test('A workspace owner turns on the sub-agent that an organization owner set up, a program gets its answer, and a new base URL turns it off', async (context) => {
  const { owner, alice } = await createActors(context)
  const model = await modelServer(context)
  const host = new URL(model.origin).host
  const organization = managementClients(owner.accessToken).subAgentModel
  // The stack has one organization, so no other scenario may start with this one's model.
  const removeModel = async () => {
    const { model: left } = await organization.getSubAgentModel({})
    if (left) await organization.deleteSubAgentModel({ etag: left.etag })
  }
  await removeModel()
  context.onTestFinished(removeModel)
  const { name: workspace } = await managementClients(alice.accessToken).workspaces.createWorkspace(
    { displayName: `Sub-agent ${crypto.randomUUID()}`, requestId: crypto.randomUUID() },
  )
  const id = workspace.split('/')[1]!
  const asOwner = (body: Parameters<typeof withBrowserSession>[1]) =>
    withBrowserSession(context, body, { actor: owner.label, storageState: owner.storageState })
  const asAlice = (body: Parameters<typeof withBrowserSession>[1]) =>
    withBrowserSession(context, body, { actor: alice.label, storageState: alice.storageState })

  await asOwner(async ({ page, step }) => {
    await step('The organization owner sets the model', async () => {
      await page.goto('/')
      await openSettings(page, 'Sub-agent')
      await page.getByLabel('Base URL').fill(`${model.origin}/v1`)
      await page.getByLabel('Model name').fill('reel-1')
      await page.getByLabel('API key').fill(KEY)
      await page.getByText('Advanced', { exact: true }).click()
      await page.getByLabel('Arguments (JSON)').fill('{"temperature": 0.5}')
      await save(page)
      await page.getByText('A key is saved.', { exact: true }).waitFor()
    })
  })

  await asAlice(async ({ page, target, step }) => {
    const lead = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Ask for the account owner',
    })
    const ask = `
      try {
        const { data } = await coral.agent({ message: 'Who owns account 42?' });
        return [data.state, data.answer];
      } catch (error) {
        return error.message;
      }`
    const answer = async () => (await observeRun(lead, await lead.exec(ask))).update.final_value
    await step('The tool is in the workspace, off', async () => {
      await expectAgentPolicy(page, workspace, 'Block')
      expect(await answer()).toEqual({ kind: 'value', value: 'Tool is disabled' })
      expect(model.requests).toEqual([])
    })
    await step('The workspace owner reads where the data goes and turns it on', async () => {
      const dialog = await openDialog(
        page,
        agentRow(page).getByRole('radio', { name: 'Allow', exact: true }),
      )
      await dialog.getByRole('heading', { name: 'Turn on the sub-agent?', exact: true }).waitFor()
      await dialog.getByText(host, { exact: true }).waitFor()
      await dialog.getByText('reel-1', { exact: true }).waitFor()
      await dialog.getByRole('button', { name: 'Turn on', exact: true }).click()
      await agentRow(page)
        .getByRole('radio', { name: 'Allow', exact: true, checked: true })
        .waitFor()
    })
    await step('The lead’s program gets the model’s answer', async () => {
      expect(await answer()).toEqual({ kind: 'value', value: ['completed', ANSWER] })
      expect(model.requests).toEqual([
        {
          path: '/v1/chat/completions',
          authorization: `Bearer ${KEY}`,
          model: 'reel-1',
          temperature: 0.5,
        },
      ])
    })
  })

  await asOwner(async ({ page, step }) => {
    await step('The organization owner changes the base URL', async () => {
      await page.goto('/')
      await openSettings(page, 'Sub-agent')
      await page.getByLabel('Base URL').fill(`${model.origin}/v2`)
      await page
        .getByText('Saving a new base URL blocks the sub-agent in every workspace.')
        .waitFor()
      await page.getByRole('button', { name: 'Use no key', exact: true }).click()
      await save(page)
      await page.getByRole('group', { name: 'API key' }).waitFor({ state: 'hidden' })
    })
  })

  await asAlice(async ({ page, target, step }) => {
    const lead = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Ask again',
    })
    await step('The tool is off again, and the new host gets nothing', async () => {
      await expectAgentPolicy(page, workspace, 'Block')
      const { update } = await observeRun(
        lead,
        await lead.exec(
          `try { await coral.agent({ message: 'Again?' }); } catch (error) { return error.message; }`,
        ),
      )
      expect(update.final_value).toEqual({ kind: 'value', value: 'Tool is disabled' })
      expect(model.requests).toHaveLength(1)
    })
  })
})
