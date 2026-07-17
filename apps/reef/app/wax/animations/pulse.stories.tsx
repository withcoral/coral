import type { Meta, StoryObj } from '@storybook/react-vite'

import { animations } from '@/wax/animations'

import { theme } from '../theme/theme.css'

const PulseDemo = () => (
  <div
    className={animations.pulseAnimation}
    style={{
      backgroundColor: theme.surface.floating,
      borderRadius: '8px',
      padding: 16,
    }}
  >
    Pulse Animation
  </div>
)

const meta = {
  component: PulseDemo,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Animations/Pulse',
} satisfies Meta<typeof PulseDemo>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
