import type { Page, Locator } from 'playwright'
import { organizationWorkspaceRow } from './members.ts'
import { workspaceSwitcher } from './sidebar.ts'

/** The settings nav, which takes the sidebar's place in the organization's settings. */
export const settingsNav = (page: Page): Locator =>
  page.getByRole('navigation', { name: 'Coral', exact: true })

/** Opens the organization's settings from the account menu and shows one of their sections. */
export async function openSettings(
  page: Page,
  section: 'Members' | 'Workspaces' | 'Secret stores' | 'Usage data',
) {
  await page.getByRole('button', { name: /^Open account menu for / }).click()
  await page.getByRole('menuitem', { name: 'Settings', exact: true }).click()
  await settingsNav(page).getByRole('link', { name: 'Back to workspace', exact: true }).waitFor()
  await settingsNav(page).getByRole('link', { name: section, exact: true }).click()
  // The nav marks the section once its page is the current one.
  await settingsNav(page).locator('a[aria-current="page"]').filter({ hasText: section }).waitFor()
}

/** Leaves the organization's settings for the page you opened them from. */
export async function backToWorkspace(page: Page) {
  await settingsNav(page).getByRole('link', { name: 'Back to workspace', exact: true }).click()
  await workspaceSwitcher(settingsNav(page)).waitFor()
}

/** The confirmation that archives a workspace, once `openArchiveConfirmation` has opened it. */
export const archiveConfirmation = (page: Page): Locator =>
  page.getByRole('dialog', { name: 'Archive workspace', exact: true })

/**
 * Opens the confirmation that archives a workspace, from its row on the
 * organization's Workspaces page. Only an organization owner can. Returns the row.
 */
export async function openArchiveConfirmation(page: Page, workspace: string) {
  await openSettings(page, 'Workspaces')
  const row = await organizationWorkspaceRow(page, workspace)
  await row.getByRole('button', { name: `Actions for ${workspace}`, exact: true }).click()
  await page.getByRole('menuitem', { name: 'Archive workspace', exact: true }).click()
  await archiveConfirmation(page).waitFor()
  return row
}
