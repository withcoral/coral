import { test as base, type Disposable } from '@playwright/test'
import { defineNetworkFixture, type NetworkFixture } from '@msw/playwright'
import type { AnyHandler } from 'msw'

type ReviewFixtures = {
  review: {
    chapter: (title: string, description?: string) => Promise<void>
    pause: (ms?: number) => Promise<void>
  }
}

interface NetworkFixtures extends ReviewFixtures {
  handlers: AnyHandler[]
  network: NetworkFixture
}

const reviewMode = process.env.PW_UI_SCREENCAST === '1'
const reviewPauseMs = Number(process.env.PW_UI_REVIEW_PAUSE_MS ?? 900)

function safeArtifactName(name: string): string {
  return (
    name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .slice(0, 80) || 'test'
  )
}

export const test = base.extend<NetworkFixtures>({
  handlers: [[], { option: true }],
  network: [
    async ({ context, handlers }, use) => {
      const network = defineNetworkFixture({
        context,
        handlers,
        onUnhandledRequest(request, print) {
          if (new URL(request.url).pathname.startsWith('/coral.v1.')) {
            print.error()
          }
        },
      })

      await network.enable()
      await use(network)
      await network.disable()
    },
    { auto: true },
  ],
  review: [
    async ({ page }, use, testInfo) => {
      let actions: Disposable | undefined
      let recording = false

      if (reviewMode) {
        await page.screencast.start({
          path: testInfo.outputPath(`${safeArtifactName(testInfo.title)}.webm`),
          size: page.viewportSize() ?? { width: 1440, height: 900 },
          quality: 100,
        })
        recording = true
        actions = await page.screencast.showActions({
          duration: 1_400,
          fontSize: 22,
          position: 'top-right',
        })
        await page.screencast.showChapter(testInfo.title, {
          description: `Coral UI review recording — ${testInfo.project.name}`,
          duration: 1_600,
        })
      }

      await use({
        chapter: async (title, description) => {
          if (!reviewMode) return
          await page.screencast.showChapter(title, { description, duration: 1_400 })
          await page.waitForTimeout(Math.min(reviewPauseMs, 1_400))
        },
        pause: async (ms = reviewPauseMs) => {
          if (!reviewMode) return
          await page.waitForTimeout(ms)
        },
      })

      await actions?.dispose()
      if (recording) {
        await page.screencast.stop()
      }
    },
    { auto: true },
  ],
})

export { expect } from '@playwright/test'
