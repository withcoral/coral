import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  breadcrumb,
  chooseFile,
  configurationNext,
  createWorkspace,
  openProviderPolicy,
  openProviderSettings,
} from '../../../support/providers.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'

test('Each page has one title, and breadcrumbs lead back from nested pages', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const crumbs = breadcrumb(page)
    const topBar = page.locator('header', { has: crumbs })
    const trail = () => crumbs.getByRole('listitem').allInnerTexts()
    const titles = () => page.getByRole('heading', { level: 1 }).allInnerTexts()
    const heading = page.getByRole('heading', { name: 'Agents', exact: true, level: 1 })
    let workspace = ''
    const name = `Breadcrumbs ${crypto.randomUUID()}`
    await step('A workspace page starts with its workspace', async () => {
      workspace = await createWorkspace(actor.accessToken, name)
      await page.goto(`/workspaces/${workspace}`)
      await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .getByRole('link', { name: 'Agents', exact: true })
        .click()
      await heading.waitFor()
      await expect.poll(trail).toEqual([name, 'Agents'])
      // The toolbar's title is the page's only h1, not the current crumb.
      await expect.poll(titles).toEqual(['Agents'])
      expect(await crumbs.getByRole('link').count()).toBe(1)
      await crumbs.getByRole('link', { name, exact: true }).waitFor()
    })
    await step('A nested page links back to each parent page', async () => {
      await page.getByRole('button', { name: 'Create agent', exact: true }).click()
      await page.getByLabel('Agent name', { exact: true }).fill('Automation')
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create agent', exact: true })
        .click()
      await copyNewAgentKey(page)
      await page.getByRole('link', { name: 'Automation', exact: true }).click()
      await crumbs.getByText('Automation', { exact: true }).waitFor()
      await expect.poll(trail).toEqual([name, 'Agents', 'Automation'])
      await crumbs.getByRole('link', { name: 'Agents', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/agents`)
    })
    await step('Create agent ends the agents toolbar', async () => {
      await heading.waitFor()
      await page
        .getByRole('main')
        .getByRole('button', { name: 'Create agent', exact: true })
        .waitFor()
      expect(await topBar.getByRole('button', { name: 'Create agent', exact: true }).count()).toBe(
        0,
      )
    })
    await step(
      'Each setup step titles the page with its provider and heads its content with its actions',
      async () => {
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await crumbs.getByRole('heading', { name: 'Add provider', exact: true, level: 1 }).waitFor()
        await expect.poll(trail).toEqual([name, 'Providers', 'Add provider'])
        await chooseFile(
          page,
          'journal.json',
          openApiDocument('Journal', { listNotes: { summary: 'List notes' } }),
        )
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await page.getByLabel('Display name', { exact: true }).waitFor()
        await crumbs.getByRole('heading', { name: 'Add Journal', exact: true, level: 1 }).waitFor()
        await expect.poll(trail).toEqual([name, 'Providers', 'Add Journal'])
        await expect.poll(titles).toEqual(['Add Journal'])
        // The step's title and actions head its content, not the top bar.
        await page.getByRole('heading', { name: 'Setup', exact: true, level: 2 }).waitFor()
        await configurationNext(page).waitFor()
        expect(await topBar.getByRole('button', { name: 'Next', exact: true }).count()).toBe(0)
        await crumbs.getByRole('link', { name: 'Providers', exact: true }).click()
        await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/providers`)
      },
    )
    await step('Provider pages lead back to the provider and the provider list', async () => {
      const providerPath = await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Ledger', { listEntries: { summary: 'List entries' } }),
        'ledger',
      )
      await openProviderSettings(page)
      await expect.poll(trail).toEqual([name, 'Providers', 'Ledger', 'Settings'])
      // The provider crumb returns to the provider's operations.
      await crumbs.getByRole('link', { name: 'Ledger', exact: true }).click()
      await page.waitForURL((url) => url.pathname === providerPath)
      await openProviderPolicy(page, providerPath)
      await expect.poll(trail).toEqual([name, 'Providers', 'Ledger', 'Operation policy'])
      await crumbs.getByRole('link', { name: 'Ledger', exact: true }).click()
      await page.waitForURL((url) => url.pathname === providerPath)
      await openProviderPolicy(page, providerPath)
      await crumbs.getByRole('link', { name: 'Providers', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/providers`)
      // The provider list's toolbar titles the page. Add provider ends that
      // toolbar, so every tab shows it.
      await page.getByRole('heading', { name: 'Providers', exact: true, level: 1 }).waitFor()
      await expect.poll(titles).toEqual(['Providers'])
      await page
        .getByRole('main')
        .getByRole('button', { name: 'Add provider', exact: true })
        .waitFor()
      expect(await topBar.getByRole('button', { name: 'Add provider', exact: true }).count()).toBe(
        0,
      )
    })
    await step('The workspace crumb leads to its overview', async () => {
      await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .getByRole('link', { name: 'Accounts', exact: true })
        .click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/accounts`)
      await expect.poll(trail).toEqual([name, 'Accounts'])
      await crumbs.getByRole('link', { name, exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}`)
      await expect.poll(trail).toEqual([name, 'Overview'])
      // The overview's title is its workspace name, which it can rename.
      await expect.poll(titles).toEqual([name])
    })
    await step('A missing page in the workspace is titled by its crumb', async () => {
      await page.goto(`/workspaces/${workspace}/agents/${crypto.randomUUID()}`)
      await crumbs.getByRole('heading', { name: 'Page not found', exact: true, level: 1 }).waitFor()
      await expect.poll(titles).toEqual(['Page not found'])
      await page.getByText('This page does not exist.', { exact: true }).waitFor()
    })
  })
})
