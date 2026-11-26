import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, userEvent, waitFor, within } from 'storybook/test'
import { Button, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { NavigationFrame, NavigationRoot } from './navigation-frame'

const meta = {
  title: 'Wax/Navigation frame',
  component: NavigationFrame,
  args: { navigationKey: 'Members', children: () => null },
  parameters: { layout: 'fullscreen' },
  render: () => <Example />,
} satisfies Meta<typeof NavigationFrame>
export default meta
type Story = StoryObj<typeof meta>
function Example() {
  const [destination, setDestination] = useState('Members')
  return (
    <NavigationRoot>
      <div style={{ display: 'flex', height: '100vh' }}>
        <NavigationFrame navigationKey={destination}>
          {() => (
            <nav aria-label="Example navigation">
              <Typography.HeadingSmall>Workspace</Typography.HeadingSmall>
              {['Members', 'Settings'].map((name) => (
                <Button.TextButton key={name} variant="bare" onClick={() => setDestination(name)}>
                  {name}
                </Button.TextButton>
              ))}
            </nav>
          )}
        </NavigationFrame>
        {/* Each destination mounts a new page, as a route change does. */}
        <PageLayout
          key={destination}
          breadcrumbs={[{ id: destination, type: 'text', text: destination }]}
        >
          <Typography.Body>{destination}</Typography.Body>
        </PageLayout>
      </div>
    </NavigationRoot>
  )
}
export const Desktop: Story = {}
export const Mobile: Story = {
  // Chromatic reads `viewports`; Storybook and its Vitest run read the `viewport` global.
  globals: { viewport: { value: 'mobile2' } },
  parameters: { chromatic: { viewports: [390] } },
  // Catches: focus lost with the old page's trigger after the drawer navigates.
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)
    await userEvent.click(page.getByRole('button', { name: 'Open navigation' }))
    const drawer = await page.findByRole('dialog', { name: 'Navigation' })
    await userEvent.click(within(drawer).getByRole('button', { name: 'Settings' }))
    await waitFor(() => expect(page.queryByRole('dialog')).not.toBeInTheDocument())
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible()
    await waitFor(() => expect(page.getByRole('button', { name: 'Open navigation' })).toHaveFocus())
  },
}
