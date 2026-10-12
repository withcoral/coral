import type { Locator, Page } from 'playwright'
import { expect } from 'vitest'

/**
 * Clicks a copy button and returns what it wrote to the clipboard. The test
 * origin need not be a secure context, so the page's clipboard is replaced
 * with a recorder for the click.
 */
export async function copiedBy(page: Page, button: Locator): Promise<string> {
  await page.evaluate(() => {
    delete (window as unknown as { copied?: string }).copied
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: async (text: string) => {
          ;(window as unknown as { copied: string }).copied = text
        },
      },
    })
  })
  await button.click()
  const copied = await page.evaluate(() => {
    // Drop the recorder so later copies reach the page's own clipboard.
    delete (navigator as unknown as { clipboard?: unknown }).clipboard
    return (window as unknown as { copied?: string }).copied ?? ''
  })
  expect(copied).not.toBe('')
  return copied
}
