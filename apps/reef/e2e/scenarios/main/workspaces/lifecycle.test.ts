import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { breadcrumb, dismissOnboarding } from '../../../support/providers.ts'

test('Workspace creation, MCP endpoint copy, rename, archive from the sidebar and restoration', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const name = `Lifecycle ${crypto.randomUUID()}`
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    let overview = ''
    await step('Create a Workspace and dismiss its onboarding', async () => {
      await page.goto('/')
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
      await page.getByLabel('Workspace name', { exact: true }).fill(name)
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create workspace', exact: true })
        .click()
      await dismissOnboarding(page)
      await breadcrumb(page, name).waitFor()
      await page.getByText(`${actor.label} (You)`, { exact: true }).waitFor()
      overview = page.url()
    })
    await step('Copy the workspace MCP URL from its sidebar header', async () => {
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
      await nav.getByRole('button', { name: `Copy ${name} workspace MCP URL`, exact: true }).click()
      await nav
        .getByRole('button', { name: `Copy ${name} workspace MCP URL: copied`, exact: true })
        .waitFor()
      const workspace = new URL(overview).pathname.split('/')[2]
      expect(await page.evaluate(() => (window as unknown as { copied: string }).copied)).toBe(
        `${target.mcpURL}/workspaces/${workspace}/mcp`,
      )
    })
    const renamed = `${name} renamed`
    await step('Rename from the Overview heading without changing identity', async () => {
      await page.getByRole('button', { name: 'Edit workspace name', exact: true }).click()
      const input = page.getByRole('textbox', { name: 'Workspace name', exact: true })
      await input.fill(renamed)
      await input.press('Enter')
      await page.getByRole('heading', { name: renamed, exact: true, level: 1 }).waitFor()
      await breadcrumb(page, renamed).waitFor()
      await nav.getByRole('button', { name: renamed, exact: true, expanded: true }).waitFor()
      expect(page.url()).toBe(overview)
    })
    const archive = page.getByRole('dialog', { name: `Archive ${renamed}`, exact: true })
    const archivedEntry = nav.getByRole('link', { name: `${renamed}, archived`, exact: true })
    const banner = page.getByText('This workspace is archived', { exact: true })
    await step('Cancel archiving from the sidebar header', async () => {
      await nav.getByRole('button', { name: `Archive ${renamed}`, exact: true }).click()
      await archive.getByRole('button', { name: 'Cancel', exact: true }).click()
      await expect.poll(() => archive.count()).toBe(0)
      expect(page.url()).toBe(overview)
      expect(await archivedEntry.count()).toBe(0)
    })
    await step(
      'Confirm archive: the Workspace moves to Archived and opens its history',
      async () => {
        await nav.getByRole('button', { name: `Archive ${renamed}`, exact: true }).click()
        await archive.getByRole('button', { name: 'Archive workspace', exact: true }).click()
        await page.waitForURL(`${overview}/activity`)
        await banner.waitFor()
        // The archived group opens on the current Workspace; the active tree drops it.
        expect(await archivedEntry.getAttribute('aria-current')).toBe('page')
        expect(
          await nav
            .getByRole('button', { name: 'Archived', exact: true })
            .getAttribute('aria-expanded'),
        ).toBe('true')
        expect(await nav.getByRole('button', { name: renamed, exact: true }).count()).toBe(0)
        // Its Overview opens its history, so nothing there offers new work.
        await page.goto(overview)
        await page.waitForURL(`${overview}/activity`)
        await banner.waitFor()
      },
    )
    await step('Restore from the sidebar returns it to the active tree', async () => {
      await nav.getByRole('button', { name: `Restore ${renamed}`, exact: true }).click()
      await nav.getByRole('button', { name: renamed, exact: true, expanded: true }).waitFor()
      await expect.poll(() => banner.count()).toBe(0)
      expect(await archivedEntry.count()).toBe(0)
      await nav
        .getByRole('group', { name: renamed, exact: true })
        .getByRole('link', { name: 'Overview', exact: true })
        .click()
      await page.waitForURL(overview)
      await page.getByRole('button', { name: 'Edit workspace name', exact: true }).waitFor()
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
        await dismissOnboarding(page)
        await breadcrumb(page, `${name} second`).waitFor()
        const second = page.url()
        expect(second).not.toBe(overview)
        expect(await header(`${name} second`).getAttribute('aria-expanded')).toBe('true')
        // Opening another Workspace keeps the earlier one open.
        expect(await header(renamed).getAttribute('aria-expanded')).toBe('true')
        expect(
          await group(`${name} second`)
            .getByRole('link', { name: 'Overview', exact: true })
            .getAttribute('aria-current'),
        ).toBe('page')
        // A header toggles its destinations without navigating.
        await header(`${name} second`).click()
        await nav.getByRole('button', { name: `${name} second`, expanded: false }).waitFor()
        expect(await group(`${name} second`).count()).toBe(0)
        expect(await header(renamed).getAttribute('aria-expanded')).toBe('true')
        expect(page.url()).toBe(second)
        await header(`${name} second`).click()
        await group(`${name} second`).getByRole('link', { name: 'Overview', exact: true }).waitFor()
        // Navigating into a Workspace opens it in the tree.
        await page.goBack()
        await breadcrumb(page, renamed).waitFor()
        expect(page.url()).toBe(overview)
        await nav.getByRole('button', { name: renamed, exact: true, expanded: true }).waitFor()
        expect(
          await group(renamed)
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
