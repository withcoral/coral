import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub, Link } from 'react-router'
import { expect, userEvent, waitFor, within } from 'storybook/test'

import { resizeViewport } from '@/wax/storybook/browser'

import { AppFrame } from '@/wax/components/app-frame'
import { PageLayout } from '@/wax/components/page-layout'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Typography } from '@/wax/components/typography'

import { Sidebar } from './sidebar'

/** The sidebar with a person's choice, which a real app also saves. */
function Example({ brandLabel, minimized }: { brandLabel?: string; minimized: boolean }) {
  const [isMinimized, setIsMinimized] = useState(minimized)
  return (
    <AppFrame
      navigation={
        <Sidebar
          brandLabel={brandLabel}
          home="/"
          isMinimized={isMinimized}
          onToggle={() => setIsMinimized(!isMinimized)}
        >
          {(rail) => (
            <SidebarButton
              as={Link}
              to="/"
              icon="LayoutGrid"
              isMinimized={rail}
              isActive
              aria-current="page"
              aria-label="Overview"
              title="Overview"
            >
              Overview
            </SidebarButton>
          )}
        </Sidebar>
      }
    >
      <PageLayout breadcrumbs={[{ id: 'overview', type: 'text', text: 'Overview' }]}>
        <Typography.Body as="p">Workspace overview</Typography.Body>
      </PageLayout>
    </AppFrame>
  )
}

const meta = {
  component: Example,
  title: 'Wax/Sidebar',
  parameters: { layout: 'fullscreen' },
  args: { minimized: false },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub initialEntries={['/']} />
    },
  ],
} satisfies Meta<typeof Example>
export default meta
type Story = StoryObj<typeof meta>

const centre = (element: Element) => {
  const box = element.getBoundingClientRect()
  return box.top + box.height / 2
}

export const Expanded: Story = {
  // Catches: a brand that is off the centre line of the page's top bar, and a
  // toggle that leaves the sidebar at its width.
  play: async ({ canvasElement }) => {
    if (!(await resizeViewport(1280, 800))) return
    const canvas = within(canvasElement)
    const sidebar = canvas.getByRole('navigation', { name: 'Coral' })
    // The top bar is a `header` inside `main`, so it has no role to find it by.
    await expect(centre(within(sidebar).getByRole('link', { name: 'Coral' }))).toBe(
      centre(canvasElement.querySelector('main header')!),
    )
    await expect(sidebar.getBoundingClientRect().width).toBe(220)

    await userEvent.click(canvas.getByRole('button', { name: 'Collapse sidebar' }))
    await expect(canvas.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
    await waitFor(() => expect(sidebar.getBoundingClientRect().width).toBe(68))
  },
}

export const Collapsed: Story = { args: { minimized: true } }

/** An app that is not Coral itself names itself beside the brand. */
export const WithBrandLabel: Story = {
  args: { brandLabel: 'console' },
  // Catches: a landmark that does not say which app it navigates.
  play: async ({ canvas }) => {
    const sidebar = canvas.getByRole('navigation', { name: 'Coral console' })
    await expect(within(sidebar).getByRole('link', { name: 'Coral console' })).toBeVisible()
  },
}

/** On a phone the sidebar is a drawer. It shows its labels, also for a person who collapsed it. */
export const Drawer: Story = {
  args: { minimized: true },
  // Chromatic reads `viewports`; Storybook and its Vitest run read the `viewport` global.
  globals: { viewport: { value: 'mobile2' } },
  parameters: { chromatic: { viewports: [390] } },
  // Catches: a drawer that keeps the collapsed rail, or that has no way to close.
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)
    await userEvent.click(page.getByRole('button', { name: 'Open navigation' }))
    const drawer = await page.findByRole('dialog', { name: 'Navigation' })
    await expect(within(drawer).getByText('Overview')).toBeVisible()
    await expect(within(drawer).queryByRole('button', { name: /sidebar/ })).not.toBeInTheDocument()
    await userEvent.click(within(drawer).getByRole('button', { name: 'Close navigation' }))
    await waitFor(() => expect(page.queryByRole('dialog')).not.toBeInTheDocument())
  },
}
