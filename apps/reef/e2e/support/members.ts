import type { Locator, Page } from 'playwright'
import { expect } from 'vitest'

/**
 * Picks one person in the open Add members or Assign owners dialog. An
 * organization member matches by email; Add members takes an email the list
 * does not offer as typed.
 */
export async function pickPerson(page: Page, email: string) {
  const people = page.getByRole('dialog').getByRole('combobox', { name: 'People', exact: true })
  await people.fill(email)
  await page.getByRole('option', { name: email }).first().waitFor()
  await people.press('Enter')
  // Picking someone clears the search for the next person.
  await expect.poll(() => people.inputValue()).toBe('')
}

/**
 * Finds a workspace's row on the organization Workspaces page. The shared
 * target holds other scenarios' workspaces on earlier pages.
 */
export async function organizationWorkspaceRow(page: Page, name: string): Promise<Locator> {
  const row = page.getByRole('row').filter({ hasText: name })
  for (let pages = 0; pages < 50 && (await row.count()) === 0; pages++) {
    const previous = page.url()
    await page.getByRole('link', { name: 'Next workspaces', exact: true }).click()
    await page.waitForURL((url) => url.href !== previous)
    await page.getByRole('table', { name: 'Workspaces', exact: true }).waitFor()
  }
  if ((await row.count()) === 0) throw new Error(`No organization Workspaces page lists ${name}`)
  return row
}
