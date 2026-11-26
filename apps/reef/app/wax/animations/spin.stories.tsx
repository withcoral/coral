import type { Meta, StoryObj } from '@storybook/react-vite'

import { animations } from '@/wax/animations'
import { Icon } from '@/wax/components/icon'

const SpinDemo = () => <Icon className={animations.spinAnimation} color="tertiary" name="Loader" />

const meta = {
  component: SpinDemo,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Animations/Spin',
} satisfies Meta<typeof SpinDemo>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
