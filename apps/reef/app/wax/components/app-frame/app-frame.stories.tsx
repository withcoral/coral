import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'

import { resizeViewport } from '@/wax/storybook/browser'

import { NavigationFrame } from '@/wax/components/navigation-frame/navigation-frame'
import { PageLayout } from '@/wax/components/page-layout'
import { Typography } from '@/wax/components/typography'

import { AppFrame } from './app-frame'

const meta = {
  component: AppFrame,
  title: 'Wax/App frame',
  parameters: { layout: 'fullscreen' },
  args: {
    navigation: (
      <NavigationFrame navigationKey="providers">
        {() => (
          <nav aria-label="Example navigation" style={{ flex: '0 0 220px', padding: 12 }}>
            <Typography.Body>Navigation</Typography.Body>
          </nav>
        )}
      </NavigationFrame>
    ),
    children: (
      <PageLayout breadcrumbs={[{ id: 'providers', type: 'text', text: 'Providers' }]}>
        <Typography.Body as="p">Add a provider to start.</Typography.Body>
      </PageLayout>
    ),
  },
} satisfies Meta<typeof AppFrame>
export default meta
type Story = StoryObj<typeof meta>

/** The page's frame: the box with the line, inside the `main` landmark. */
function frameOf(canvasElement: HTMLElement) {
  const frame = within(canvasElement).getByRole('main').firstElementChild!
  const style = getComputedStyle(frame)
  const lines = [
    style.borderTopWidth,
    style.borderRightWidth,
    style.borderBottomWidth,
    style.borderLeftWidth,
  ]
  return { box: frame.getBoundingClientRect(), lines, radius: style.borderTopRightRadius }
}

/**
 * The page is a frame beside the navigation. A fine line and round corners on
 * every side set it a little inside the window's edge.
 */
export const Desktop: Story = {
  // Catches: a page that runs to the window's edge, or a frame with a line on one side only.
  play: async ({ canvasElement }) => {
    if (!(await resizeViewport(1280, 800))) return
    const { box, lines, radius } = frameOf(canvasElement)
    await expect(lines).toEqual(['1px', '1px', '1px', '1px'])
    await expect(radius).toBe('12px')
    await expect(box.top).toBe(8)
    await expect(window.innerWidth - box.right).toBe(8)
    await expect(window.innerHeight - box.bottom).toBe(8)
    // The navigation's own padding is the gap on its side.
    const navigation = within(canvasElement).getByRole('navigation', { name: 'Example navigation' })
    await expect(box.left).toBe(navigation.getBoundingClientRect().right)
  },
}

/** A narrow window has no navigation beside the page, so the page fills it. */
export const Mobile: Story = {
  // Chromatic reads `viewports`; Storybook and its Vitest run read the `viewport` global.
  globals: { viewport: { value: 'mobile2' } },
  parameters: { chromatic: { viewports: [390] } },
  // Catches: a frame that keeps its inset or its line where the window has no room for them.
  play: async ({ canvasElement }) => {
    if (!(await resizeViewport(390, 800))) return
    const { box, lines, radius } = frameOf(canvasElement)
    await expect(lines).toEqual(['0px', '0px', '0px', '0px'])
    await expect(radius).toBe('0px')
    await expect([box.left, box.top, box.width, box.height]).toEqual([0, 0, 390, 800])
  },
}
