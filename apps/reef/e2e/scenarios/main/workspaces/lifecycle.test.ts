import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { breadcrumb } from '../../../support/providers.ts'

test('Workspace creation, MCP endpoint copy, settings dialog, rename, archive confirmation and restoration', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const name = `Lifecycle ${crypto.randomUUID()}`
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    const settings = page.getByRole('dialog', { name: 'Workspace settings', exact: true })
    let overview = ''
    await step('Create a Workspace', async () => {
      await page.goto('/')
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
      await page.getByLabel('Workspace name', { exact: true }).fill(name)
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create workspace', exact: true })
        .click()
      await breadcrumb(page, name).waitFor()
      await page.getByText(`${actor.label} (You)`, { exact: true }).waitFor()
      overview = page.url()
    })
    await step('Copy the Workspace MCP endpoint from its sidebar header', async () => {
      await page.evaluate(() => {
        Object.defineProperty(navigator, 'clipboard', {
          configurable: true,
          value: {
            writeText: async (text: string) => {
              ;(window as unknown as { copied: string }).copied = text
            },
          },
        })
      })
      await nav.getByRole('button', { name: `Copy ${name} MCP endpoint`, exact: true }).click()
      await nav
        .getByRole('button', { name: `Copy ${name} MCP endpoint: copied`, exact: true })
        .waitFor()
      const workspace = new URL(overview).pathname.split('/')[2]
      expect(await page.evaluate(() => (window as unknown as { copied: string }).copied)).toBe(
        `${target.mcpURL}/workspaces/${workspace}/mcp`,
      )
    })
    await step('Open settings as a dialog over the Overview', async () => {
      await nav.getByRole('link', { name: `${name} settings`, exact: true }).click()
      await settings.waitFor()
      expect(page.url()).toBe(`${overview}/settings`)
    })
    await step('Rename without changing identity', async () => {
      await settings.getByLabel('Workspace name', { exact: true }).fill(`${name} renamed`)
      await settings.getByRole('button', { name: 'Save name', exact: true }).click()
      await expect
        .poll(() => settings.getByLabel('Workspace name', { exact: true }).inputValue())
        .toBe(`${name} renamed`)
      expect(page.url()).toBe(`${overview}/settings`)
    })
    const archive = page.getByRole('dialog', { name: 'Archive workspace', exact: true })
    await step('Cancel archiving', async () => {
      await settings.getByRole('button', { name: 'Archive workspace', exact: true }).click()
      await archive.getByRole('button', { name: 'Cancel', exact: true }).click()
      await expect.poll(() => archive.count()).toBe(0)
      await settings.getByText('Active', { exact: true }).first().waitFor()
    })
    await step('Confirm archive and reject configuration', async () => {
      await settings.getByRole('button', { name: 'Archive workspace', exact: true }).click()
      await archive.getByRole('button', { name: 'Archive workspace', exact: true }).click()
      await settings.getByText('Archived', { exact: true }).first().waitFor()
      expect(await settings.getByRole('button', { name: 'Save name', exact: true }).count()).toBe(0)
      await page.reload()
      await settings.getByText('Archived', { exact: true }).first().waitFor()
    })
    await step('Restore future work', async () => {
      await settings.getByRole('button', { name: 'Restore workspace', exact: true }).click()
      await settings.getByText('Active', { exact: true }).first().waitFor()
      await settings.getByRole('button', { name: 'Save name', exact: true }).waitFor()
    })
    await step('Closing settings replaces them with the Overview in history', async () => {
      const entries = await page.evaluate(() => history.length)
      await page.keyboard.press('Escape')
      await expect.poll(() => settings.count()).toBe(0)
      await page.waitForURL(overview)
      await breadcrumb(page, `${name} renamed`).waitFor()
      await nav
        .getByRole('button', { name: `${name} renamed`, exact: true, expanded: true })
        .waitFor()
      expect(await page.evaluate(() => history.length)).toBe(entries)
    })
    await step(
      'Sidebar creation, the Workspace tree, and browser history keep URL-derived context',
      async () => {
        const header = (workspace: string) =>
          nav.getByRole('button', { name: workspace, exact: true })
        // Each open Workspace labels its destinations with its name.
        const group = (workspace: string) =>
          nav.getByRole('group', { name: workspace, exact: true })
        await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('textbox', { name: 'Workspace name', exact: true })
          .fill(`${name} second`)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create workspace', exact: true })
          .click()
        await breadcrumb(page, `${name} second`).waitFor()
        const second = page.url()
        expect(second).not.toBe(overview)
        expect(await header(`${name} second`).getAttribute('aria-expanded')).toBe('true')
        // Opening another Workspace keeps the earlier one open.
        expect(await header(`${name} renamed`).getAttribute('aria-expanded')).toBe('true')
        expect(
          await group(`${name} second`)
            .getByRole('link', { name: 'Overview', exact: true })
            .getAttribute('aria-current'),
        ).toBe('page')
        // A header toggles its destinations without navigating.
        await header(`${name} second`).click()
        await nav.getByRole('button', { name: `${name} second`, expanded: false }).waitFor()
        expect(await group(`${name} second`).count()).toBe(0)
        expect(await header(`${name} renamed`).getAttribute('aria-expanded')).toBe('true')
        expect(page.url()).toBe(second)
        await header(`${name} second`).click()
        await group(`${name} second`).getByRole('link', { name: 'Overview', exact: true }).waitFor()
        // Navigating into a Workspace opens it in the tree.
        await page.goBack()
        await breadcrumb(page, `${name} renamed`).waitFor()
        expect(page.url()).toBe(overview)
        await nav
          .getByRole('button', { name: `${name} renamed`, exact: true, expanded: true })
          .waitFor()
        expect(
          await group(`${name} renamed`)
            .getByRole('link', { name: 'Overview', exact: true })
            .getAttribute('aria-current'),
        ).toBe('page')
        await page.goForward()
        await breadcrumb(page, `${name} second`).waitFor()
        await nav
          .getByRole('button', { name: `${name} second`, exact: true, expanded: true })
          .waitFor()
        // A page outside any Workspace opens none. The shared target has other
        // scenarios' Workspaces, so pick whichever one the tree shows first.
        await page.goto('/organization/members')
        await breadcrumb(page, 'Members').waitFor()
        await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
        expect(await page.getByRole('link', { name: 'Overview', exact: true }).count()).toBe(0)
        expect(await page.getByRole('link', { name: 'Providers', exact: true }).count()).toBe(0)
        const choice = nav
          .getByRole('group', { name: 'Workspaces', exact: true })
          .getByRole('button', { expanded: false })
          .first()
        const selectedName = await choice.innerText()
        await choice.click()
        const overviewLink = page.getByRole('link', { name: 'Overview', exact: true })
        const selectedUrl = new URL(
          (await overviewLink.getAttribute('href'))!,
          page.url(),
        ).toString()
        await overviewLink.click()
        await breadcrumb(page, selectedName).waitFor()
        expect(page.url()).toBe(selectedUrl)
      },
    )
  })
})
