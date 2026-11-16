import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { breadcrumb } from '../../../support/providers.ts'
import {
  archiveConfirmation,
  backToWorkspace,
  openArchiveConfirmation,
  openSettings,
} from '../../../support/settings.ts'
import {
  copyWorkspaceMcpUrl,
  readSwitcher,
  openCreateWorkspace,
  switcherWorkspace,
  workspaceSwitcher,
} from '../../../support/sidebar.ts'

test('Workspace creation, MCP endpoint copy, rename, archive from the organization and restoration', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const name = `Lifecycle ${crypto.randomUUID()}`
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    /** The switcher, once it names this workspace as the current one. */
    const current = (workspace: string) =>
      nav.getByRole('button', { name: `Workspace: ${workspace}`, exact: true })
    // The current workspace labels its pages with its name.
    const group = (workspace: string) => nav.getByRole('group', { name: workspace, exact: true })
    let overview = ''
    await step('Create a Workspace', async () => {
      await page.goto('/')
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await openCreateWorkspace(page)
      await page.getByLabel('Workspace name', { exact: true }).fill(name)
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create workspace', exact: true })
        .click()
      await breadcrumb(page, name).waitFor()
      await page.getByText(`${actor.label} (You)`, { exact: true }).waitFor()
      overview = page.url()
    })
    await step('Copy the workspace MCP URL from the switcher', async () => {
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
      await copyWorkspaceMcpUrl(page, name)
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
      await current(renamed).waitFor()
      expect(page.url()).toBe(overview)
    })
    const archive = archiveConfirmation(page)
    const archivedCurrent = current(`${renamed}, archived`)
    const banner = page.getByText('This workspace is archived', { exact: true })
    await step("Cancel archiving from the organization's Workspaces page", async () => {
      await openArchiveConfirmation(page, renamed)
      await archive.getByRole('button', { name: 'Cancel', exact: true }).click()
      await expect.poll(() => archive.count()).toBe(0)
      await backToWorkspace(page)
      expect(page.url()).toBe(overview)
      expect(await archivedCurrent.count()).toBe(0)
    })
    await step(
      'Confirm archive: the Workspace stays open as archived, with only its history',
      async () => {
        const row = await openArchiveConfirmation(page, renamed)
        await archive.getByRole('button', { name: 'Archive workspace', exact: true }).click()
        await row.getByText('Archived', { exact: true }).waitFor()
        // Back in the workspace, it opens on its history.
        await backToWorkspace(page)
        await page.waitForURL(`${overview}/activity`)
        await banner.waitFor()
        // The switcher marks it archived; its only page is Activity.
        await archivedCurrent.waitFor()
        expect(
          await group(renamed)
            .getByRole('link', { name: 'Activity', exact: true })
            .getAttribute('aria-current'),
        ).toBe('page')
        expect(await group(renamed).getByRole('link').count()).toBe(1)
        // It leaves the switcher, which lists only the workspaces you can work in.
        expect(await readSwitcher(page, () => switcherWorkspace(page, renamed).count())).toBe(0)
        // Its Overview opens its history, so nothing there offers new work.
        await page.goto(overview)
        await page.waitForURL(`${overview}/activity`)
        await banner.waitFor()
      },
    )
    await step('Restore from the banner returns it to the active workspaces', async () => {
      await page.getByRole('button', { name: 'Restore workspace', exact: true }).click()
      await current(renamed).waitFor()
      await expect.poll(() => banner.count()).toBe(0)
      await group(renamed).getByRole('link', { name: 'Overview', exact: true }).click()
      await page.waitForURL(overview)
      await page.getByRole('button', { name: 'Edit workspace name', exact: true }).waitFor()
    })
    await step(
      'Sidebar creation, the switcher, and browser history keep URL-derived context',
      async () => {
        const overviewLink = (workspace: string) =>
          group(workspace).getByRole('link', { name: 'Overview', exact: true })
        await openCreateWorkspace(page)
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
        // The sidebar shows the new workspace's pages, and only those.
        await current(`${name} second`).waitFor()
        expect(await overviewLink(`${name} second`).getAttribute('aria-current')).toBe('page')
        expect(await group(renamed).count()).toBe(0)
        // The sidebar follows the URL through browser history.
        await page.goBack()
        await breadcrumb(page, renamed).waitFor()
        expect(page.url()).toBe(overview)
        await current(renamed).waitFor()
        expect(await overviewLink(renamed).getAttribute('aria-current')).toBe('page')
        await page.goForward()
        await breadcrumb(page, `${name} second`).waitFor()
        await current(`${name} second`).waitFor()
        // The switcher marks the current workspace and opens another.
        await workspaceSwitcher(nav).click()
        await page
          .getByRole('menuitemradio', { name: `${name} second`, exact: true, checked: true })
          .waitFor()
        await switcherWorkspace(page, renamed).click()
        await page.waitForURL(overview)
        await breadcrumb(page, renamed).waitFor()
        await current(renamed).waitFor()
        // The organization's settings take the sidebar; Back to workspace returns to the page you left.
        await openSettings(page, 'Workspaces')
        await page.getByRole('table', { name: 'Workspaces', exact: true }).waitFor()
        expect(new URL(page.url()).pathname).toBe('/organization/workspaces')
        expect(await workspaceSwitcher(nav).count()).toBe(0)
        await backToWorkspace(page)
        expect(page.url()).toBe(overview)
        await current(renamed).waitFor()
      },
    )
  })
})
