import type { Meta, StoryObj } from '@storybook/react-vite'

import { theme } from '../theme/theme.css'
import { pulse } from './pulse.css'

const PulseDemo = () => (
  <div
    style={{
      backgroundColor: theme.surface.floating,
      borderRadius: '8px',
      padding: 16,
      ...pulse,
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
