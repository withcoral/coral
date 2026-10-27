import type { Locator, Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import { createWorkspace, publishWorkspaceProvider } from '../../../../support/providers.ts'

/**
 * The mean luminance (0 to 1) of the opaque pixels in the one logo image that
 * shows inside `scope`. A dark-theme logo renders beside the light one, and
 * only the theme class decides which shows, so this reads what a person sees.
 */
async function visibleGlyphLuminance(scope: Locator) {
  const image = scope.locator('img').filter({ visible: true })
  await image.waitFor()
  expect(await image.count()).toBe(1)
  return image.evaluate(async (element: HTMLImageElement) => {
    await element.decode()
    const canvas = document.createElement('canvas')
    canvas.width = element.naturalWidth
    canvas.height = element.naturalHeight
    const context = canvas.getContext('2d')!
    context.drawImage(element, 0, 0)
    const { data } = context.getImageData(0, 0, canvas.width, canvas.height)
    let total = 0
    let count = 0
    for (let index = 0; index < data.length; index += 4) {
      if (data[index + 3] < 128) continue
      total += (0.2126 * data[index] + 0.7152 * data[index + 1] + 0.0722 * data[index + 2]) / 255
      count += 1
    }
    return total / count
  })
}

async function useTheme(page: Page, theme: 'dark' | 'light') {
  // The system preference is the default, so Reef follows the emulated change.
  await page.emulateMedia({ colorScheme: theme })
  await expect.poll(() => page.evaluate(() => document.body.style.colorScheme)).toBe(theme)
}

// GitHub's mark is black; its recipe adds a white one for the dark theme.
test('A recipe logo with a dark-theme variant reads in both themes', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    // GitHub has two recipes, so its card holds a button for each.
    const card = page
      .getByRole('listitem')
      .filter({ has: page.getByRole('button', { name: 'Add GitHub API', exact: true }) })
    const heading = page
      .getByRole('table', { name: 'Tools', exact: true })
      .locator('[role="row"][aria-level="1"]')
      .filter({ hasText: 'Logo fixture' })
    let workspace = ''
    await step('Show the white GitHub mark in the dark catalog', async () => {
      workspace = await createWorkspace(actor.accessToken, `Logos ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await useTheme(page, 'dark')
      expect(await visibleGlyphLuminance(card)).toBeGreaterThan(0.8)
    })
    await step('Swap to the black mark when the system turns light', async () => {
      await useTheme(page, 'light')
      expect(await visibleGlyphLuminance(card)).toBeLessThan(0.2)
    })
    await step('Show the recipe logo for a provider added under its id', async () => {
      const { provider } = await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Logo fixture', { hello: {} }),
        'github',
      )
      await page.goto(`/${provider}`)
      expect(await visibleGlyphLuminance(heading)).toBeLessThan(0.2)
      await useTheme(page, 'dark')
      expect(await visibleGlyphLuminance(heading)).toBeGreaterThan(0.8)
    })
  })
})
