import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { Button, Typography } from '@/wax/components'
import { NavigationFrame } from './navigation-frame'

const meta = {
  title: 'Wax/Navigation frame',
  component: NavigationFrame,
  args: { navigationKey: 'Members', label: 'Members', children: () => null },
  parameters: { layout: 'fullscreen' },
  render: () => <Example />,
} satisfies Meta<typeof NavigationFrame>
export default meta
type Story = StoryObj<typeof meta>
function Example() {
  const [destination, setDestination] = useState('Members')
  return (
    <NavigationFrame navigationKey={destination} label={destination}>
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
  )
}
export const Desktop: Story = {}
export const Mobile: Story = { parameters: { chromatic: { viewports: [390] } } }
