import type { Locator, Page } from 'playwright'

/** The sidebar button that names the current workspace and opens the menu of the others. */
export function workspaceSwitcher(scope: Page | Locator): Locator {
  return scope.getByRole('button', { name: /^(Workspace: |Workspaces)/ })
}

/**
 * A workspace in the open switcher menu. The menu sits outside the sidebar, so
 * this looks on the page. An item's name may also carry its pending actions.
 */
export function switcherWorkspace(page: Page, name: string): Locator {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return page.getByRole('menuitemradio', { name: new RegExp(`^${escaped}(\\. \\d+ pending |$)`) })
}

/** Opens the form that creates a workspace, from the switcher's menu. */
export async function openCreateWorkspace(page: Page) {
  await workspaceSwitcher(page).click()
  await page.getByRole('menuitem', { name: 'Create workspace', exact: true }).click()
}

/**
 * Whether the sidebar shows the pages only an owner has, such as Agents, for
 * the current workspace. It shows the role the navigation acts on.
 */
export async function showsOwnerPages(page: Page) {
  const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
  // Every member has Overview, so its link shows that the workspace's pages are there to read.
  await nav.getByRole('link', { name: 'Overview', exact: true }).waitFor()
  return (await nav.getByRole('link', { name: 'Agents', exact: true }).count()) > 0
}

/**
 * Copies a workspace's MCP URL from its row in the switcher's menu. The row's
 * action is named copied only after the write succeeds, so what the clipboard
 * holds afterwards cannot be a URL copied earlier.
 */
export async function copyWorkspaceMcpUrl(
  page: Page,
  workspace: string,
  scope: Page | Locator = page,
) {
  await workspaceSwitcher(scope).click()
  await page.getByRole('menuitem', { name: `Copy ${workspace} MCP URL`, exact: true }).click()
  await page
    .getByRole('menuitem', { name: `Copy ${workspace} MCP URL: copied`, exact: true })
    .waitFor()
  // The menu stays open after a copy.
  await page.keyboard.press('Escape')
  await page.getByRole('menu').waitFor({ state: 'detached' })
}

/**
 * Opens the switcher, reads its menu, and closes it again, so the open menu
 * does not cover the page for the next step.
 */
export async function readSwitcher<T>(page: Page, read: () => Promise<T>): Promise<T> {
  await workspaceSwitcher(page).click()
  const menu = page.getByRole('menu')
  await menu.waitFor()
  const result = await read()
  await page.keyboard.press('Escape')
  await menu.waitFor({ state: 'detached' })
  return result
}
