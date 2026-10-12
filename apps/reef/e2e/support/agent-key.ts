import type { Page } from 'playwright'
import { expect } from 'vitest'
import { copiedBy } from './clipboard.ts'

/**
 * Leaves a new agent's one-time key dialog by copying, and returns the copied
 * key. The key stays masked on screen throughout.
 */
export async function copyNewAgentKey(page: Page): Promise<string> {
  const dialog = page.getByRole('dialog')
  await dialog.getByRole('heading', { name: /^Save the API key/ }).waitFor()
  const key = await copiedBy(
    page,
    dialog.getByRole('button', { name: 'Copy API key and close', exact: true }),
  )
  await dialog.waitFor({ state: 'detached' })
  expect((await page.content()).includes(key)).toBe(false)
  return key
}
