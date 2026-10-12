import type { Page } from 'playwright'
import { expect } from 'vitest'

/**
 * Picks one person in the open Add members dialog. An organization member
 * matches by email; an email the list does not offer is added as typed.
 */
export async function pickPerson(page: Page, email: string) {
  const people = page.getByRole('dialog').getByRole('combobox', { name: 'People', exact: true })
  await people.fill(email)
  await page.getByRole('option', { name: email }).first().waitFor()
  await people.press('Enter')
  // Picking someone clears the search for the next person.
  await expect.poll(() => people.inputValue()).toBe('')
}
