import { expect, test } from 'vitest'
import type { Locator, Page } from 'playwright'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import { createWorkspace, publishWorkspaceProvider } from '../../../../support/providers.ts'

const row = (page: Page, name: string) =>
  page.getByRole('row').filter({
    has: page.getByRole('checkbox', { name: `Select ${name}`, exact: true }),
  })

/** The operation the keyboard cursor rests on. */
const cursorName = (table: Locator) =>
  table.locator('[role="row"][data-active] a[href*="/ops/"]').textContent()

// Cursor movement, the summary filter, and key routing are
// workspace-providers-page.dom.test.tsx cases; this scenario keeps what needs
// a real layout, real focus, and a saved policy.
test('The keyboard drives the operations table, its drawer, and its policy', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const table = page.getByRole('table', { name: 'Operations', exact: true })
    const filter = page.getByRole('searchbox', { name: 'Filter operations', exact: true })
    const heading = (provider: string) =>
      table.getByRole('rowgroup', { name: provider, exact: true })
    let workspace = ''
    await step('Open a large catalog beside a small one', async () => {
      workspace = await createWorkspace(actor.accessToken, `Keyboard ${crypto.randomUUID()}`)
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument(
          'Keys',
          Object.fromEntries(
            Array.from({ length: 150 }, (_, index) => [`op${String(index).padStart(3, '0')}`, {}]),
          ),
        ),
        'keys',
      )
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Aside', { other: {} }),
        'aside',
      )
      // A short viewport keeps most of the large catalog out of view.
      await page.setViewportSize({ width: 1440, height: 600 })
      await page.goto(`/workspaces/${workspace}/providers`)
      await table.waitFor()
    })
    await step('The find shortcut focuses the operation filter', async () => {
      await page.keyboard.press('ControlOrMeta+f')
      await expect
        .poll(() => filter.evaluate((input) => input === document.activeElement))
        .toBe(true)
    })
    await step('The cursor row scrolls into view below the pinned headings', async () => {
      // With no cursor, ↑ starts at the last operation, at the far end of the table.
      await page.keyboard.press('ArrowUp')
      await expect.poll(() => cursorName(table)).toBe('op149')
      await page.keyboard.press('ArrowUp')
      await page.keyboard.press('ArrowUp')
      await page.keyboard.press('ArrowUp')
      await expect.poll(() => cursorName(table)).toBe('op146')
      // The row's top and bottom edges belong to it, not to a sticky heading or
      // to the space outside the table.
      await expect
        .poll(() =>
          table.locator('[role="row"][data-active]').evaluate((element) => {
            const bounds = element.getBoundingClientRect()
            const x = bounds.left + bounds.width / 2
            return [bounds.top + 1, bounds.bottom - 1].every((y) =>
              element.contains(document.elementFromPoint(x, y)),
            )
          }),
        )
        .toBe(true)
    })
    await step('← saves the cursor operation’s policy', async () => {
      // Focus leaves the filter, whose caret keeps ← and →.
      await filter.blur()
      await page.keyboard.press('ArrowLeft')
      await row(page, 'op146')
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      await expect.poll(() => row(page, 'op146').getAttribute('aria-busy')).toBe('false')
      await page.reload()
      await table.waitFor()
      await filter.fill('op146')
      await row(page, 'op146')
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      await filter.fill('')
    })
    await step('A summary count filters every provider until it is cleared', async () => {
      const count = heading('Keys').getByRole('button', { name: '1 Always allow', exact: true })
      await count.click()
      expect(await count.getAttribute('aria-pressed')).toBe('true')
      await expect
        .poll(() => table.locator('a[href*="/ops/"]').allTextContents())
        .toEqual(['op146'])
      await heading('Keys').getByText('1 of 150 operations', { exact: true }).waitFor()
      await heading('Aside').getByText('No matching operations.', { exact: true }).waitFor()
      await page
        .getByRole('button', { name: 'Filter operations by', exact: true })
        .getByText('Always allow', { exact: true })
        .waitFor()
      await count.click()
      await heading('Aside').getByRole('link', { name: 'other', exact: true }).waitFor()
      // The pinned heading repeats the count.
      await heading('Keys').getByText('150 operations', { exact: true }).first().waitFor()
    })
    await step('A row click opens the drawer, and ↓ moves it to the next operation', async () => {
      await filter.fill('op00')
      // The name cell's far end holds no control, so the row takes the click.
      const cell = row(page, 'op001').getByRole('cell').nth(1)
      const bounds = await cell.boundingBox()
      if (!bounds) throw new Error('The op001 row is not laid out')
      await cell.click({ position: { x: bounds.width - 8, y: bounds.height / 2 } })
      await page.getByRole('dialog', { name: 'op001', exact: true }).waitFor()
      await page.waitForURL((url) => url.pathname.endsWith('/ops/op001'))
      await page.keyboard.press('ArrowDown')
      await page.getByRole('dialog', { name: 'op002', exact: true }).waitFor()
      await page.waitForURL((url) => url.pathname.endsWith('/ops/op002'))
    })
    await step('After Escape, Space checks the operation the drawer showed', async () => {
      await page.keyboard.press('Escape')
      await page.waitForURL((url) => !url.pathname.includes('/ops/'))
      await expect.poll(() => cursorName(table)).toBe('op002')
      // Focus returns to the table, which must not keep Space for itself.
      await page.keyboard.press('Space')
      await expect
        .poll(() => page.getByRole('checkbox', { name: 'Select op002', exact: true }).isChecked())
        .toBe(true)
      expect(
        await page.getByRole('checkbox', { name: 'Select op001', exact: true }).isChecked(),
      ).toBe(false)
    })
  })
})
